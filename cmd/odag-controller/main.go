package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// CRD reference for dsf.io/v1/ODAG
var odagGVR = schema.GroupVersionResource{
	Group:    "dsf.io",
	Version:  "v1",
	Resource: "odags",
}

const (
	labelODAGName  = "dsf-odag"
	labelTaskName  = "dsf-task"
	dataOutputPath = "/data/dsf-outputs"
	dataAgentPort  = 8081
)

// nodeInfo holds both the node name and its internal IP (needed for cross-node
// data fetches via the data-agent DaemonSet).
type nodeInfo struct {
	name      string
	ip        string
	cpuMillis int64 // allocatable CPU in millicores (e.g. 4000 = 4 cores)
	memBytes  int64 // allocatable memory in bytes
}

// assignmentCache stores task→nodeInfo assignments keyed by "namespace/odagName".
// Populated in deployODAG, read in processReadyTasks.
var assignmentCache sync.Map // "ns/name" -> map[string]nodeInfo

// podCache stores the latest pod state for every ODAG pod, keyed by pod name.
// Updated by watchPods on every event, read by processReadyTasks.
var podCache sync.Map // "ns/podName" -> *corev1.Pod

// profilerDB is the SQLite database for task/link profiling.
// Initialized in main(); nil if profiling is not configured.
var profilerDB *sql.DB

// processedODAGs prevents double-deploying the same ODAG on reconnect.
var processedODAGs sync.Map // "ns/name" -> bool

// runningODAGs tracks ODAGs currently in Running/Scheduling/Pending phase
// so the status poller knows which ones to refresh.
var runningODAGs sync.Map // "ns/name" -> bool

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var kubeconfig string
	var dbPath string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig (leave empty for in-cluster)")
	flag.StringVar(&dbPath, "db", envOrDefault("DSF_PROFILER_DB", "/data/dsf-profiler.db"), "profiler SQLite database path")
	flag.Parse()

	cfg, err := buildConfig(kubeconfig)
	if err != nil {
		log.Fatalf("[odag-ctrl] failed to build config: %v", err)
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[odag-ctrl] failed to create kubernetes client: %v", err)
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[odag-ctrl] failed to create dynamic client: %v", err)
	}

	// Initialize profiler database.
	profilerDB, err = initProfilerDB(dbPath)
	if err != nil {
		log.Printf("[odag-ctrl] WARNING: profiler DB init failed: %v (profiling disabled)", err)
	}

	log.Println("[odag-ctrl] starting odag-controller (layer-by-layer, file transport)")

	go watchBandwidthConfigMap(client)
	go watchODAGTemplates(dynClient)
	go watchODAGs(dynClient, client)
	go pollRunningODAGs(dynClient, client)
	watchPods(client, dynClient)
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		}
	}
	if err != nil {
		return nil, err
	}
	// Raise client-side rate limits (defaults: QPS=5, Burst=10) so that
	// multi-ODAG workloads don't stall on client-side throttling.
	cfg.QPS = 50
	cfg.Burst = 100
	return cfg, nil
}

// --------------------------------------------------------------------------
// ODAG watcher
// --------------------------------------------------------------------------

// pollRunningODAGs periodically refreshes task statuses for all Running ODAGs
// so that fast-changing fields like sending are captured between pod events.
func pollRunningODAGs(dynClient dynamic.Interface, client *kubernetes.Clientset) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		runningODAGs.Range(func(k, _ any) bool {
			parts := strings.SplitN(k.(string), "/", 2)
			if len(parts) != 2 {
				return true
			}
			ns, name := parts[0], parts[1]
			odagObj, err := dynClient.Resource(odagGVR).Namespace(ns).Get(
				context.Background(), name, metav1.GetOptions{},
			)
			if err != nil {
				return true
			}
			go processReadyTasks(dynClient, client, ns, name, odagObj)
			return true
		})
	}
}

func watchODAGs(dynClient dynamic.Interface, client *kubernetes.Clientset) {
	for {
		watcher, err := dynClient.Resource(odagGVR).Namespace("").Watch(
			context.Background(), metav1.ListOptions{},
		)
		if err != nil {
			log.Printf("[odag-ctrl] error watching ODAGs: %v; retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("[odag-ctrl] watching ODAG resources")
		for event := range watcher.ResultChan() {
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			switch string(event.Type) {
			case "ADDED":
				go deployODAG(dynClient, client, obj)
			case "DELETED":
				key := obj.GetNamespace() + "/" + obj.GetName()
				processedODAGs.Delete(key)
				assignmentCache.Delete(key)
			}
		}
		log.Println("[odag-ctrl] ODAG watcher closed; reconnecting in 2s")
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// Deploy: called once when a new ODAG CR is created.
// Assigns tasks to nodes, caches the assignment, then launches layer 0.
// --------------------------------------------------------------------------

func deployODAG(dynClient dynamic.Interface, client *kubernetes.Clientset, obj *unstructured.Unstructured) {
	odagName := obj.GetName()
	namespace := obj.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}
	key := namespace + "/" + odagName

	if _, loaded := processedODAGs.LoadOrStore(key, true); loaded {
		return
	}

	log.Printf("[odag-ctrl] deploying ODAG %s", key)

	tasks := extractTasks(obj)
	if len(tasks) == 0 {
		log.Printf("[odag-ctrl] ODAG %s has no tasks; skipping", key)
		return
	}

	updateODAGPhase(dynClient, namespace, odagName, "Scheduling", "assigning tasks to nodes")

	nodeMap, err := getNodeInfoMap(client)
	if err != nil || len(nodeMap) == 0 {
		updateODAGPhase(dynClient, namespace, odagName, "Failed", "failed to list schedulable nodes")
		return
	}

	// Build runtime/dataSize/bandwidth resolvers from profiler + template config.
	var rtRes runtimeResolver
	var dsRes dataSizeResolver
	var bwRes bandwidthResolver
	templateObj := getTemplateForODAG(obj)
	if templateObj != nil && profilerDB != nil {
		tplName := obj.GetLabels()["dsf.io/template"]
		cfg := extractProfilingConfig(templateObj)
		defaultRT := extractDefaultRuntime(templateObj)
		defaultDS := extractDefaultDataSize(templateObj)
		rtRes = buildRuntimeResolver(profilerDB, tplName, tasks, cfg.MinSamples, defaultRT, cfg.RuntimeSource)
		dsRes = buildDataSizeResolver(profilerDB, tplName, tasks, cfg.MinSamples, defaultDS, cfg.RuntimeSource)
		bwRes = buildBandwidthResolver(profilerDB, cfg.MinSamples, cfg.BandwidthSource)
		log.Printf("[odag-ctrl] resolvers for %s: runtime=%s, bandwidth=%s (template: %s)",
			key, cfg.RuntimeSource, cfg.BandwidthSource, tplName)
	} else {
		// Non-template ODAG: use ConfigMap bandwidth only.
		bwRes = buildBandwidthResolver(nil, 3, "external")
	}

	schedulerName, _, _ := unstructured.NestedString(obj.Object, "spec", "scheduler")
	var assignMap map[string]nodeInfo
	switch schedulerName {
	case "heft":
		log.Printf("[odag-ctrl] using HEFT scheduler for %s", key)
		assignMap = heftAssignTasks(tasks, nodeMap, rtRes, dsRes, bwRes)
	default:
		log.Printf("[odag-ctrl] using random scheduler for %s", key)
		assignMap = assignTasks(tasks, nodeMap)
	}
	assignmentCache.Store(key, assignMap)

	predicted := computePredictedSchedule(tasks, assignMap, rtRes, dsRes, bwRes)
	writePredictedSchedule(dynClient, namespace, odagName, predicted)

	log.Printf("[odag-ctrl] task placement for %s:", key)
	for task, ni := range assignMap {
		log.Printf("[odag-ctrl]   %-20s -> %s (%s)", task, ni.name, ni.ip)
	}

	// Clear any stale DataReady states on child nodes left over from a
	// previous run of the same ODAG (same name → same hostPath files).
	// For every (dep → child) edge, reset dep's state on the child's node
	// so the controller doesn't see stale DataReady and launch tasks early.
	for _, task := range tasks {
		childNi := assignMap[task.Name]
		if childNi.ip == "" {
			continue
		}
		for _, dep := range task.Dependencies {
			resetTaskState(childNi.ip, odagName, dep)
		}
	}

	updateODAGPhase(dynClient, namespace, odagName, "Running", "")
	processReadyTasks(dynClient, client, namespace, odagName, obj)
}

// --------------------------------------------------------------------------
// Pod watcher: triggers layer-by-layer progression on pod completions.
// --------------------------------------------------------------------------

func watchPods(client *kubernetes.Clientset, dynClient dynamic.Interface) {
	for {
		watcher, err := client.CoreV1().Pods("").Watch(
			context.Background(),
			metav1.ListOptions{LabelSelector: labelODAGName},
		)
		if err != nil {
			log.Printf("[odag-ctrl] error watching pods: %v; retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		for event := range watcher.ResultChan() {
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}

			// Update pod cache on every event (ADDED, MODIFIED, DELETED).
			podKey := pod.Namespace + "/" + pod.Name
			if string(event.Type) == "DELETED" {
				podCache.Delete(podKey)
				continue
			}
			podCache.Store(podKey, pod.DeepCopy())

			if string(event.Type) != "MODIFIED" {
				continue
			}
			odagName := pod.Labels[labelODAGName]
			if odagName == "" {
				continue
			}
			ns := pod.Namespace

			// Fetch the ODAG CR to get ownerUID — we need it to create new pods.
			odagObj, err := dynClient.Resource(odagGVR).Namespace(ns).Get(
				context.Background(), odagName, metav1.GetOptions{},
			)
			if err != nil {
				continue
			}
			go processReadyTasks(dynClient, client, ns, odagName, odagObj)
		}
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// processReadyTasks: the core layer-by-layer scheduling loop.
//
// Called after every pod state change. For each task in the spec:
//   - skip if a pod already exists for it
//   - create a pod if ALL its dependencies have Succeeded
//
// Also updates per-task statuses and checks for overall completion.
// --------------------------------------------------------------------------

func processReadyTasks(dynClient dynamic.Interface, client *kubernetes.Clientset,
	namespace, odagName string, odagObj *unstructured.Unstructured) {

	key := namespace + "/" + odagName
	ownerUID := odagObj.GetUID()

	// Retrieve cached assignment.
	raw, ok := assignmentCache.Load(key)
	if !ok {
		return // ODAG not yet fully initialized
	}
	assignMap := raw.(map[string]nodeInfo)

	// Extract tasks from the passed ODAG object (no extra API call needed).
	tasks := extractTasks(odagObj)

	// Collect pods for this ODAG from the in-memory cache (no API call).
	var podItems []corev1.Pod
	podCache.Range(func(_, val interface{}) bool {
		p := val.(*corev1.Pod)
		if p.Namespace == namespace && p.Labels[labelODAGName] == odagName {
			podItems = append(podItems, *p)
		}
		return true
	})

	// Build a map of which tasks already have pods, and their current pod phase.
	existingPods := make(map[string]bool)
	podPhases := make(map[string]corev1.PodPhase)
	for _, pod := range podItems {
		taskName := pod.Labels[labelTaskName]
		existingPods[taskName] = true
		podPhases[taskName] = pod.Status.Phase
	}

	// For each task: if it has no pod yet AND all dependencies have DataReady on
	// THIS task's node (Proposal-1 per-child-node DataReady), create its pod now.
	for _, task := range tasks {
		if existingPods[task.Name] {
			continue
		}
		childNi := assignMap[task.Name]
		allDepsDone := true
		for _, dep := range task.Dependencies {
			if !existingPods[dep] {
				// Dep pod hasn't been created yet — not ready.
				allDepsDone = false
				break
			}
			if childNi.ip != "" {
				// Check DataReady on this child's node: has dep's data arrived here?
				if !isDataReady(childNi.ip, odagName, dep) {
					allDepsDone = false
					break
				}
			} else {
				// No data-agent reachable for child: fall back to dep PodSucceeded.
				if podPhases[dep] != corev1.PodSucceeded {
					allDepsDone = false
					break
				}
			}
		}
		if !allDepsDone {
			continue
		}

		ni := assignMap[task.Name]
		if ni.ip != "" {
			resetTaskState(ni.ip, odagName, task.Name)
		}
		envVars := buildEnvVars(odagName, task, assignMap, tasks)
		envVars = addTemplateEnvVars(envVars, odagObj.GetLabels())
		if err := ensurePod(client, namespace, odagName, task, ni.name, envVars, ownerUID); err != nil {
			log.Printf("[odag-ctrl] error creating pod for %s/%s: %v", key, task.Name, err)
		} else {
			log.Printf("[odag-ctrl] launched task %s on node %s", task.Name, ni.name)
		}
	}

	// Update per-task statuses and check overall completion.
	updateTaskStatuses(dynClient, namespace, odagName, podItems, assignMap, tasks)
	checkODAGCompletion(dynClient, client, podItems, namespace, odagName, len(tasks))
}

// --------------------------------------------------------------------------
// Helpers: extract task specs from unstructured ODAG CR
// --------------------------------------------------------------------------

type taskSpec struct {
	Name           string
	Image          string
	Command        []string
	Args           []string
	Dependencies   []string
	DataSize       string
	Runtime        float64
	RuntimeProfile map[string]float64 // node name -> runtime (seconds)
	CPU            string
	Memory         string
	Constraints    []string
	UserEnv        []corev1.EnvVar
}

func extractTasks(obj *unstructured.Unstructured) []taskSpec {
	rawTasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	tasks := make([]taskSpec, 0, len(rawTasks))
	for _, raw := range rawTasks {
		t, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := t["name"].(string)
		image, _ := t["image"].(string)
		if name == "" || image == "" {
			continue
		}
		cmd, _, _ := unstructured.NestedStringSlice(t, "command")
		args, _, _ := unstructured.NestedStringSlice(t, "args")
		deps, _, _ := unstructured.NestedStringSlice(t, "dependencies")
		dataSize, _ := t["dataSize"].(string)
		runtime, _ := t["runtime"].(int64)
		// Also try float64 (CRD stores number as float when it has decimals).
		var runtimeF float64
		if runtime > 0 {
			runtimeF = float64(runtime)
		} else if rf, ok := t["runtime"].(float64); ok {
			runtimeF = rf
		}

		// Parse per-node runtime profile: {"anrg-3": 6, "anrg-8": 12}
		var rtProfile map[string]float64
		if rp, ok := t["runtimeProfile"].(map[string]interface{}); ok {
			rtProfile = make(map[string]float64, len(rp))
			for node, val := range rp {
				switch v := val.(type) {
				case float64:
					rtProfile[node] = v
				case int64:
					rtProfile[node] = float64(v)
				}
			}
		}

		constraints, _, _ := unstructured.NestedStringSlice(t, "constraints", "nodeNames")
		cpu, _, _ := unstructured.NestedString(t, "resources", "cpu")
		mem, _, _ := unstructured.NestedString(t, "resources", "memory")

		var userEnv []corev1.EnvVar
		if envList, ok := t["env"].([]interface{}); ok {
			for _, e := range envList {
				if em, ok := e.(map[string]interface{}); ok {
					n, _ := em["name"].(string)
					v, _ := em["value"].(string)
					if n != "" {
						userEnv = append(userEnv, corev1.EnvVar{Name: n, Value: v})
					}
				}
			}
		}

		tasks = append(tasks, taskSpec{
			Name:           name,
			Image:          image,
			Command:        cmd,
			Args:           args,
			Dependencies:   deps,
			DataSize:       dataSize,
			Runtime:        runtimeF,
			RuntimeProfile: rtProfile,
			Constraints:  constraints,
			CPU:          cpu,
			Memory:       mem,
			UserEnv:      userEnv,
		})
	}
	return tasks
}

// getNodeInfoMap returns a map of node name -> nodeInfo for all schedulable nodes.
func getNodeInfoMap(client *kubernetes.Clientset) (map[string]nodeInfo, error) {
	nodeList, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{
		FieldSelector: "spec.unschedulable!=true",
	})
	if err != nil {
		return nil, err
	}
	result := make(map[string]nodeInfo)
	for _, n := range nodeList.Items {
		noSchedule := false
		for _, taint := range n.Spec.Taints {
			if taint.Effect == corev1.TaintEffectNoSchedule {
				noSchedule = true
				break
			}
		}
		if noSchedule {
			continue
		}
		ip := ""
		for _, addr := range n.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				ip = addr.Address
				break
			}
		}
		// Read allocatable resources for resource-aware scheduling.
		cpuMillis := int64(0)
		memBytes := int64(0)
		if cpu, ok := n.Status.Allocatable[corev1.ResourceCPU]; ok {
			cpuMillis = cpu.MilliValue()
		}
		if mem, ok := n.Status.Allocatable[corev1.ResourceMemory]; ok {
			memBytes = mem.Value()
		}
		result[n.Name] = nodeInfo{name: n.Name, ip: ip, cpuMillis: cpuMillis, memBytes: memBytes}
	}
	return result, nil
}


// buildEnvVars builds file-transport environment variables for a task pod.
//
// Every task receives:
//
//	DSF_TRANSPORT_PATTERN=file
//	DSF_ODAG_NAME
//	DSF_TASK_NAME
//	DSF_OUTPUT_DIR       path where this task should write its output
//	DSF_DEPS             comma-separated upstream dependency names
//	NODE_NAME            downward API: the node this pod is running on
//
// For each upstream dependency <dep>:
//
//	DSF_DEP_<DEP>_NODE   node name where that dep ran (informational)
//
// For each downstream successor <succ>:
//
//	DSF_SUCCESSORS            comma-separated successor task names
//	DSF_SUCC_<SUCC>_NODE      node name where that successor will run
//	DSF_SUCC_<SUCC>_HOST      internal IP of that node (for data-agent PUT)
func buildEnvVars(odagName string, task taskSpec, assignMap map[string]nodeInfo, allTasks []taskSpec) []corev1.EnvVar {
	outputDir := fmt.Sprintf("%s/%s/%s", dataOutputPath, odagName, task.Name)

	// Compute which tasks depend on this task (its successors).
	var successorNames []string
	for _, t := range allTasks {
		for _, dep := range t.Dependencies {
			if dep == task.Name {
				successorNames = append(successorNames, t.Name)
				break
			}
		}
	}

	env := []corev1.EnvVar{
		{Name: "DSF_TRANSPORT_PATTERN", Value: "file"},
		{Name: "DSF_ODAG_NAME", Value: odagName},
		{Name: "DSF_TASK_NAME", Value: task.Name},
		{Name: "DSF_OUTPUT_DIR", Value: outputDir},
		{Name: "DSF_DEPS", Value: strings.Join(task.Dependencies, ",")},
		{Name: "DSF_SUCCESSORS", Value: strings.Join(successorNames, ",")},
		{Name: "PYTHONUNBUFFERED", Value: "1"},
		// Downward API: node name and host IP for state protocol + routing.
		{
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
			},
		},
		{
			Name: "DSF_NODE_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"},
			},
		},
		// Spec hints: runtime (seconds) and output data size (bytes) for task awareness.
		{Name: "DSF_RUNTIME", Value: fmt.Sprintf("%d", int(task.Runtime))},
		{Name: "DSF_DATA_SIZE", Value: fmt.Sprintf("%d", parseDataSizeBytes(task.DataSize))},
	}

	// Per-dependency: node name (informational; recv() reads locally so host/path not needed).
	for _, dep := range task.Dependencies {
		ni := assignMap[dep]
		depKey := strings.ToUpper(strings.ReplaceAll(dep, "-", "_"))
		env = append(env,
			corev1.EnvVar{Name: fmt.Sprintf("DSF_DEP_%s_NODE", depKey), Value: ni.name},
		)
	}

	// Per-successor: node name and host IP (needed for data-agent PUT in send()).
	for _, succ := range successorNames {
		ni := assignMap[succ]
		succKey := strings.ToUpper(strings.ReplaceAll(succ, "-", "_"))
		env = append(env,
			corev1.EnvVar{Name: fmt.Sprintf("DSF_SUCC_%s_NODE", succKey), Value: ni.name},
			corev1.EnvVar{Name: fmt.Sprintf("DSF_SUCC_%s_HOST", succKey), Value: ni.ip},
		)
	}

	env = append(env, task.UserEnv...)

	// Inject template/run metadata if this ODAG was created from a template.
	// These are set when buildEnvVars is called from processReadyTasks which
	// has access to the ODAG object via the odagObj parameter.
	// The actual injection happens in processReadyTasks after buildEnvVars returns.

	return env
}

// addTemplateEnvVars appends DSF_TEMPLATE_NAME and DSF_RUN_ID env vars if the
// ODAG was created from a template. Called after buildEnvVars.
func addTemplateEnvVars(env []corev1.EnvVar, odagLabels map[string]string) []corev1.EnvVar {
	if tpl := odagLabels["dsf.io/template"]; tpl != "" {
		env = append(env, corev1.EnvVar{Name: "DSF_TEMPLATE_NAME", Value: tpl})
	}
	if run := odagLabels["dsf.io/run"]; run != "" {
		env = append(env, corev1.EnvVar{Name: "DSF_RUN_ID", Value: run})
	}
	return env
}

// ensurePod creates a task pod with a hostPath volume for file-based data transfer.
func ensurePod(client *kubernetes.Clientset, namespace, odagName string, task taskSpec,
	nodeName string, envVars []corev1.EnvVar, ownerUID types.UID) error {

	podName := fmt.Sprintf("%s-%s", odagName, task.Name)
	_, err := client.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err == nil {
		return nil // already exists
	}

	resources := parseResources(task.CPU, task.Memory)
	hostPathType := corev1.HostPathDirectoryOrCreate

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				labelODAGName: odagName,
				labelTaskName: task.Name,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "dsf.io/v1",
				Kind:       "ODAG",
				Name:       odagName,
				UID:        ownerUID,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Volumes: []corev1.Volume{
				{
					Name: "dsf-outputs",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: dataOutputPath,
							Type: &hostPathType,
						},
					},
				},
				{
					Name: "dsf-shared",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/shared/dsf-outputs",
							Type: &hostPathType,
						},
					},
				},
			},
			Containers: []corev1.Container{{
				Name:            task.Name,
				Image:           task.Image,
				ImagePullPolicy: corev1.PullAlways,
				Command:         task.Command,
				Args:            task.Args,
				Env:             envVars,
				Resources:       resources,
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "dsf-outputs",
						MountPath: dataOutputPath,
					},
					{
						Name:      "dsf-shared",
						MountPath: "/shared/dsf-outputs",
					},
				},
			}},
		},
	}

	// Pin to assigned node via node affinity.
	if nodeName != "" {
		pod.Spec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{nodeName},
						}},
					}},
				},
			},
		}
	}

	_, err = client.CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil && !isAlreadyExists(err) {
		return err
	}
	log.Printf("[odag-ctrl] created pod %s/%s (node: %s)", namespace, podName, nodeName)
	return nil
}

// parseDataSizeBytes converts a human-readable size string (e.g. "30MB", "1GiB")
// to bytes as an integer string for injection into DSF_DATA_SIZE.
func parseDataSizeBytes(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" {
		return 0
	}
	type entry struct {
		suffix string
		mult   int64
	}
	for _, e := range []entry{
		{"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
		{"GB", 1_000_000_000}, {"MB", 1_000_000}, {"KB", 1_000}, {"B", 1},
	} {
		if strings.HasSuffix(s, e.suffix) {
			numStr := strings.TrimSpace(s[:len(s)-len(e.suffix)])
			if v, err := strconv.ParseFloat(numStr, 64); err == nil {
				return int64(v * float64(e.mult))
			}
		}
	}
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v
	}
	return 0
}

// --------------------------------------------------------------------------
// State protocol helpers
// --------------------------------------------------------------------------

// httpClient is reused across state queries to avoid creating a new connection
// for every call.
var httpClient = &http.Client{Timeout: 2 * time.Second}

// isDataReady queries the data-agent on nodeIP and returns true when the task
// has signalled DataReady (or Succeeded, which implies data was already ready).
func isDataReady(nodeIP, odagName, taskName string) bool {
	url := fmt.Sprintf("http://%s:%d/state/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	resp, err := httpClient.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}
	state := strings.TrimSpace(string(body))
	return state == "DataReady"
}

// resetTaskState writes "Scheduled" to the data-agent before pod creation,
// clearing any stale state left by a previous run of the same ODAG.
func resetTaskState(nodeIP, odagName, taskName string) {
	url := fmt.Sprintf("http://%s:%d/state/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader("Scheduled"))
	if err != nil {
		return
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("[odag-ctrl] resetTaskState %s/%s: %v", odagName, taskName, err)
		return
	}
	resp.Body.Close()
}

// querySending returns true if the data-agent reports sending=true for the task.
func querySending(nodeIP, odagName, taskName string) bool {
	url := fmt.Sprintf("http://%s:%d/sending/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	resp, err := httpClient.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(body)) == "true"
}

// queryTaskBytes returns the actual output bytes recorded by the data-agent.
func queryTaskBytes(nodeIP, odagName, taskName string) int64 {
	url := fmt.Sprintf("http://%s:%d/bytes/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	resp, err := httpClient.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		return 0
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	return n
}

// queryTaskState returns the raw state string from the data-agent, or "" on error.
func queryTaskState(nodeIP, odagName, taskName string) string {
	url := fmt.Sprintf("http://%s:%d/state/%s/%s", nodeIP, dataAgentPort, odagName, taskName)
	resp, err := httpClient.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		return ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(body))
}

// --------------------------------------------------------------------------
// Status updates
// --------------------------------------------------------------------------

func updateTaskStatuses(dynClient dynamic.Interface,
	namespace, odagName string, pods []corev1.Pod, assignMap map[string]nodeInfo, tasks []taskSpec) {

	dataSizeMap := make(map[string]string, len(tasks))
	for _, t := range tasks {
		if n := parseDataSizeBytes(t.DataSize); n > 0 {
			dataSizeMap[t.Name] = strconv.FormatInt(n, 10)
		}
	}

	taskStatuses := make([]map[string]interface{}, 0, len(pods))
	for _, pod := range pods {
		taskName := pod.Labels[labelTaskName]
		ts := map[string]interface{}{
			"name":    taskName,
			"podName": pod.Name,
			"node":    pod.Spec.NodeName,
		}
		if pod.Status.StartTime != nil {
			ts["startTime"] = pod.Status.StartTime.UTC().Format(time.RFC3339)
		}
		podPhase := "Pending"
		taskState := "Scheduled" // default when pod exists but not yet Running
		switch pod.Status.Phase {
		case corev1.PodRunning:
			podPhase = "Running"
			ni := assignMap[taskName]
			if ni.ip != "" {
				if s := queryTaskState(ni.ip, odagName, taskName); s != "" {
					taskState = s
				} else {
					taskState = "Executing"
				}
				if querySending(ni.ip, odagName, taskName) {
					ts["sending"] = true
				}
			} else {
				taskState = "Executing"
			}
		case corev1.PodSucceeded:
			podPhase = "Succeeded"
			taskState = "Done" // pod exited cleanly; data-agent state irrelevant
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Terminated != nil {
					ts["completionTime"] = cs.State.Terminated.FinishedAt.UTC().Format(time.RFC3339)
				}
			}
		case corev1.PodFailed:
			podPhase = "Failed"
			taskState = "Failed"
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Terminated != nil {
					ts["completionTime"] = cs.State.Terminated.FinishedAt.UTC().Format(time.RFC3339)
				}
			}
		}
		ts["phase"] = podPhase
		ts["state"] = taskState
		if ds := dataSizeMap[taskName]; ds != "" {
			ts["dataSize"] = ds
		}
		taskStatuses = append(taskStatuses, ts)
	}

	patch := map[string]interface{}{"status": map[string]interface{}{"tasks": taskStatuses}}
	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), odagName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

func checkODAGCompletion(dynClient dynamic.Interface, client *kubernetes.Clientset, pods []corev1.Pod, namespace, odagName string, totalTasks int) {
	if len(pods) < totalTasks {
		return // not all layers launched yet
	}
	allSucceeded := true
	anyFailed := false
	for _, pod := range pods {
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
		case corev1.PodFailed:
			anyFailed = true
			allSucceeded = false
		default:
			allSucceeded = false
		}
	}
	if anyFailed {
		updateODAGPhase(dynClient, namespace, odagName, "Failed", "one or more task pods failed")
	} else if allSucceeded {
		makespan := computeMakespan(pods)
		updateODAGCompletion(dynClient, namespace, odagName, makespan)
		log.Printf("[odag-ctrl] ODAG %s/%s Succeeded (makespan: %.2fs)", namespace, odagName, makespan)

		// Trigger profiling and data cleanup if this ODAG was created from a template.
		go profileODAGIfTemplated(dynClient, client, namespace, odagName, pods, makespan)
	}
}

// profileODAGIfTemplated checks if a completed ODAG was created from a template
// and records profiling data if so.
func profileODAGIfTemplated(dynClient dynamic.Interface, client *kubernetes.Clientset, namespace, odagName string, pods []corev1.Pod, makespan float64) {
	if profilerDB == nil {
		return
	}

	// Fetch the ODAG to check labels.
	obj, err := dynClient.Resource(odagGVR).Namespace(namespace).Get(
		context.Background(), odagName, metav1.GetOptions{},
	)
	if err != nil {
		return
	}

	labels := obj.GetLabels()
	templateName := labels["dsf.io/template"]
	if templateName == "" {
		return
	}

	runNum := getRunNumber(obj)
	tasks := extractTasks(obj)

	// Retrieve assignment map.
	key := namespace + "/" + odagName
	raw, ok := assignmentCache.Load(key)
	if !ok {
		return
	}
	assignMap := raw.(map[string]nodeInfo)

	// Extract actual start/completion times from pods.
	taskStartTimes := make(map[string]time.Time)
	taskCompletionTimes := make(map[string]time.Time)
	for _, pod := range pods {
		taskName := pod.Labels[labelTaskName]
		if pod.Status.StartTime != nil {
			taskStartTimes[taskName] = pod.Status.StartTime.Time
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Terminated != nil {
				taskCompletionTimes[taskName] = cs.State.Terminated.FinishedAt.Time
			}
		}
	}

	profileCompletedRun(dynClient, client, profilerDB, namespace, odagName, templateName, runNum,
		tasks, assignMap, taskStartTimes, taskCompletionTimes, makespan)
}

func updateODAGPhase(dynClient dynamic.Interface, namespace, name, phase, message string) {
	key := namespace + "/" + name
	if phase == "Running" || phase == "Scheduling" || phase == "Pending" {
		runningODAGs.Store(key, true)
	} else {
		runningODAGs.Delete(key)
	}
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"phase":   phase,
			"message": message,
		},
	}
	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), name, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

func updateODAGCompletion(dynClient dynamic.Interface, namespace, name string, makespan float64) {
	runningODAGs.Delete(namespace + "/" + name)
	now := time.Now().UTC().Format(time.RFC3339)
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"phase":          "Succeeded",
			"completionTime": now,
			"makespan":       makespan,
			"message":        "",
		},
	}
	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), name, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

// --------------------------------------------------------------------------
// Utilities
// --------------------------------------------------------------------------

func parseResources(cpu, memory string) corev1.ResourceRequirements {
	r := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}
	if cpu != "" {
		q := resource.MustParse(cpu)
		r.Requests[corev1.ResourceCPU] = q
		r.Limits[corev1.ResourceCPU] = q
	}
	if memory != "" {
		q := resource.MustParse(memory)
		r.Requests[corev1.ResourceMemory] = q
		r.Limits[corev1.ResourceMemory] = q
	}
	return r
}

// parseResourceQuantity parses a k8s resource quantity string like "500m" or "256Mi".
func parseResourceQuantity(s string) (resource.Quantity, error) {
	return resource.ParseQuantity(s)
}

func computeMakespan(pods []corev1.Pod) float64 {
	var earliest, latest time.Time
	for _, pod := range pods {
		if pod.Status.StartTime != nil {
			st := pod.Status.StartTime.Time
			if earliest.IsZero() || st.Before(earliest) {
				earliest = st
			}
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Terminated != nil {
				ft := cs.State.Terminated.FinishedAt.Time
				if latest.IsZero() || ft.After(latest) {
					latest = ft
				}
			}
		}
	}
	if earliest.IsZero() || latest.IsZero() {
		return 0
	}
	return latest.Sub(earliest).Seconds()
}

func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already exists")
}
