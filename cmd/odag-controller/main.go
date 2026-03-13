package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	labelODAGName = "dsf-odag"
	labelTaskName = "dsf-task"
	zmqPort       = int32(5555)
)

// processedODAGs prevents double-deploying the same ODAG on reconnect.
var processedODAGs sync.Map // namespace/name -> bool

func main() {
	var kubeconfig string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig (leave empty for in-cluster)")
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

	log.Println("[odag-ctrl] starting odag-controller")

	// Watch ODAG CRs in a background goroutine.
	go watchODAGs(dynClient, client)

	// Watch pod status changes in the main goroutine (blocks forever).
	watchPods(client, dynClient)
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	}
	return cfg, nil
}

// --------------------------------------------------------------------------
// ODAG watcher
// --------------------------------------------------------------------------

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
			case "MODIFIED":
				go reconcileODAG(dynClient, client, obj)
			case "DELETED":
				key := obj.GetNamespace() + "/" + obj.GetName()
				processedODAGs.Delete(key)
			}
		}
		log.Println("[odag-ctrl] ODAG watcher closed; reconnecting in 2s")
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// Deploy: called once when a new ODAG CR is created
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

	// Get schedulable cluster nodes.
	nodes, err := getNodes(client)
	if err != nil {
		log.Printf("[odag-ctrl] error getting nodes for ODAG %s: %v", key, err)
		updateODAGPhase(dynClient, namespace, odagName, "Failed", fmt.Sprintf("failed to list nodes: %v", err))
		return
	}
	if len(nodes) == 0 {
		log.Printf("[odag-ctrl] no schedulable nodes found for ODAG %s", key)
		updateODAGPhase(dynClient, namespace, odagName, "Failed", "no schedulable nodes available")
		return
	}

	// Assign tasks: pick a random node from the constraint list (or any node).
	assignMap := assignTasks(tasks, nodes)
	log.Printf("[odag-ctrl] task placement for %s:", key)
	for task, node := range assignMap {
		log.Printf("[odag-ctrl]   %-20s -> %s", task, node)
	}

	// Build service name map.
	svcNames := make(map[string]string, len(tasks))
	for _, t := range tasks {
		svcNames[t.Name] = fmt.Sprintf("%s-%s", odagName, t.Name)
	}

	// Create a ClusterIP Service for each task (ZMQ discovery via DNS).
	for _, t := range tasks {
		if err := ensureService(client, namespace, svcNames[t.Name], odagName, t.Name, obj.GetUID()); err != nil {
			log.Printf("[odag-ctrl] error creating service for task %s/%s: %v", key, t.Name, err)
		}
	}

	// Create all task pods simultaneously.
	// All pods start at the same time; downstream tasks block on their PULL
	// socket waiting for data. Upstream tasks do their work then PUSH.
	for _, t := range tasks {
		envVars := buildEnvVars(odagName, namespace, t, tasks, svcNames)
		nodeName := assignMap[t.Name]
		if err := ensurePod(client, namespace, odagName, t, nodeName, envVars, obj.GetUID()); err != nil {
			log.Printf("[odag-ctrl] error creating pod for task %s/%s: %v", key, t.Name, err)
		}
	}

	updateODAGPhase(dynClient, namespace, odagName, "Running", "")
	log.Printf("[odag-ctrl] ODAG %s is Running", key)
}

// reconcileODAG is called on MODIFIED events (e.g. user edits the CR).
func reconcileODAG(dynClient dynamic.Interface, client *kubernetes.Clientset, obj *unstructured.Unstructured) {
	namespace := obj.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}
	checkODAGCompletion(dynClient, client, namespace, obj.GetName())
}

// --------------------------------------------------------------------------
// Pod watcher
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
			if string(event.Type) == "MODIFIED" {
				odagName := pod.Labels[labelODAGName]
				if odagName != "" {
					go checkODAGCompletion(dynClient, client, pod.Namespace, odagName)
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
}

// checkODAGCompletion checks whether all pods for an ODAG have finished and
// updates the ODAG CR status accordingly, including per-task statuses.
func checkODAGCompletion(dynClient dynamic.Interface, client *kubernetes.Clientset, namespace, odagName string) {
	pods, err := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", labelODAGName, odagName),
	})
	if err != nil || len(pods.Items) == 0 {
		return
	}

	allSucceeded := true
	anyFailed := false

	// Build per-task status from pod state.
	taskStatuses := make([]map[string]interface{}, 0, len(pods.Items))
	for _, pod := range pods.Items {
		taskName := pod.Labels[labelTaskName]
		ts := map[string]interface{}{
			"name":    taskName,
			"podName": pod.Name,
			"node":    pod.Spec.NodeName,
		}

		// start time from pod
		if pod.Status.StartTime != nil {
			ts["startTime"] = pod.Status.StartTime.UTC().Format(time.RFC3339)
		}

		// completion time and phase from container status
		podPhase := "Pending"
		switch pod.Status.Phase {
		case corev1.PodRunning:
			podPhase = "Running"
		case corev1.PodSucceeded:
			podPhase = "Succeeded"
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Terminated != nil {
					ts["completionTime"] = cs.State.Terminated.FinishedAt.UTC().Format(time.RFC3339)
				}
			}
		case corev1.PodFailed:
			podPhase = "Failed"
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Terminated != nil {
					ts["completionTime"] = cs.State.Terminated.FinishedAt.UTC().Format(time.RFC3339)
				}
			}
		}
		ts["phase"] = podPhase

		taskStatuses = append(taskStatuses, ts)

		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			// good
		case corev1.PodFailed:
			anyFailed = true
			allSucceeded = false
		default:
			allSucceeded = false
		}
	}

	// Always patch per-task statuses so the UI stays current.
	updateODAGTaskStatuses(dynClient, namespace, odagName, taskStatuses)

	if anyFailed {
		updateODAGPhase(dynClient, namespace, odagName, "Failed", "one or more task pods failed")
	} else if allSucceeded {
		makespan := computeMakespan(pods.Items)
		updateODAGCompletion(dynClient, namespace, odagName, makespan)
		log.Printf("[odag-ctrl] ODAG %s/%s Succeeded (makespan: %.2fs)", namespace, odagName, makespan)
	}
}

// --------------------------------------------------------------------------
// Helpers: extract task specs from unstructured ODAG CR
// --------------------------------------------------------------------------

type taskSpec struct {
	Name         string
	Image        string
	Command      []string
	Args         []string
	Dependencies []string
	DataSize     string
	Runtime      float64
	CPU          string
	Memory       string
	Constraints  []string
	UserEnv      []corev1.EnvVar
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
			Name:         name,
			Image:        image,
			Command:      cmd,
			Args:         args,
			Dependencies: deps,
			DataSize:     dataSize,
			Runtime:      float64(runtime),
			Constraints:  constraints,
			CPU:          cpu,
			Memory:       mem,
			UserEnv:      userEnv,
		})
	}
	return tasks
}

// assignTasks assigns each task to a node using simple constraint-aware random placement.
// If a task has constraints.nodeNames, a random node from that list (intersected with the
// cluster's schedulable nodes) is chosen. Otherwise any schedulable node is picked.
func assignTasks(tasks []taskSpec, clusterNodes []string) map[string]string {
	nodeSet := make(map[string]bool, len(clusterNodes))
	for _, n := range clusterNodes {
		nodeSet[n] = true
	}
	result := make(map[string]string, len(tasks))
	for _, t := range tasks {
		candidates := clusterNodes
		if len(t.Constraints) > 0 {
			var allowed []string
			for _, c := range t.Constraints {
				if nodeSet[c] {
					allowed = append(allowed, c)
				}
			}
			if len(allowed) > 0 {
				candidates = allowed
			} else {
				log.Printf("[odag-ctrl] task %s: no constraint nodes available in cluster; using any node", t.Name)
			}
		}
		result[t.Name] = candidates[rand.Intn(len(candidates))]
	}
	return result
}

func getNodes(client *kubernetes.Clientset) ([]string, error) {
	nodeList, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{
		FieldSelector: "spec.unschedulable!=true",
	})
	if err != nil {
		return nil, err
	}
	var nodes []string
	for _, n := range nodeList.Items {
		noSchedule := false
		for _, taint := range n.Spec.Taints {
			if taint.Effect == corev1.TaintEffectNoSchedule {
				noSchedule = true
				break
			}
		}
		if !noSchedule {
			nodes = append(nodes, n.Name)
		}
	}
	return nodes, nil
}

// buildEnvVars builds the DSF env vars injected into each task pod.
// Every task gets the service endpoints of ALL other tasks in the graph,
// so it can communicate with any peer regardless of direction.
func buildEnvVars(odagName, namespace string, task taskSpec, allTasks []taskSpec, svcNames map[string]string) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "DSF_TASK_NAME", Value: task.Name},
		{Name: "DSF_TRANSPORT_PATTERN", Value: "pushpull"},
		{Name: "DSF_RECV_PORT", Value: fmt.Sprintf("%d", zmqPort)},
		{Name: "PYTHONUNBUFFERED", Value: "1"},
	}

	// Inject all peer service endpoints.
	for _, other := range allTasks {
		if other.Name == task.Name {
			continue
		}
		fqdn := fmt.Sprintf("%s.%s.svc.cluster.local", svcNames[other.Name], namespace)
		// DSF_PEER_BRANCH_A for task named "branch-a"
		envKey := "DSF_PEER_" + strings.ToUpper(strings.ReplaceAll(other.Name, "-", "_"))
		env = append(env, corev1.EnvVar{
			Name:  envKey,
			Value: fmt.Sprintf("zmq://%s:%d", fqdn, zmqPort),
		})
	}

	env = append(env, task.UserEnv...)
	return env
}

// ensureService creates a ClusterIP Service for a task if it doesn't exist.
func ensureService(client *kubernetes.Clientset, namespace, svcName, odagName, taskName string, ownerUID types.UID) error {
	_, err := client.CoreV1().Services(namespace).Get(context.Background(), svcName, metav1.GetOptions{})
	if err == nil {
		return nil // already exists
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: namespace,
			Labels:    map[string]string{labelODAGName: odagName, labelTaskName: taskName},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "dsf.io/v1",
				Kind:       "ODAG",
				Name:       odagName,
				UID:        ownerUID,
			}},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{labelODAGName: odagName, labelTaskName: taskName},
			Ports: []corev1.ServicePort{{
				Name:       "zmq",
				Protocol:   corev1.ProtocolTCP,
				Port:       zmqPort,
				TargetPort: intstr.FromInt32(zmqPort),
			}},
		},
	}
	_, err = client.CoreV1().Services(namespace).Create(context.Background(), svc, metav1.CreateOptions{})
	if err != nil && !isAlreadyExists(err) {
		return err
	}
	log.Printf("[odag-ctrl] created service %s/%s", namespace, svcName)
	return nil
}

// ensurePod creates a task pod if it doesn't already exist.
func ensurePod(client *kubernetes.Clientset, namespace, odagName string, task taskSpec, nodeName string, envVars []corev1.EnvVar, ownerUID types.UID) error {
	podName := fmt.Sprintf("%s-%s", odagName, task.Name)
	_, err := client.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err == nil {
		return nil // already exists
	}

	resources := parseResources(task.CPU, task.Memory)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				labelODAGName: odagName,
				labelTaskName: task.Name,
				"app":         odagName,
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
			Containers: []corev1.Container{{
				Name:            task.Name,
				Image:           task.Image,
				ImagePullPolicy: corev1.PullAlways,
				Command:         task.Command,
				Args:            task.Args,
				Env:             envVars,
				Resources:       resources,
				Ports: []corev1.ContainerPort{{
					Name:          "zmq",
					ContainerPort: zmqPort,
					Protocol:      corev1.ProtocolTCP,
				}},
			}},
		},
	}

	// Pin to HEFT-assigned node via node affinity.
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

// --------------------------------------------------------------------------
// Status updates (JSON merge-patch on status subresource)
// --------------------------------------------------------------------------

func updateODAGPhase(dynClient dynamic.Interface, namespace, name, phase, message string) {
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"phase":   phase,
			"message": message,
		},
	}
	data, _ := json.Marshal(patch)
	if _, err := dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), name, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	); err != nil {
		log.Printf("[odag-ctrl] failed to patch status for ODAG %s/%s: %v", namespace, name, err)
	}
}

func updateODAGTaskStatuses(dynClient dynamic.Interface, namespace, name string, tasks []map[string]interface{}) {
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"tasks": tasks,
		},
	}
	data, _ := json.Marshal(patch)
	if _, err := dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), name, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	); err != nil {
		log.Printf("[odag-ctrl] failed to patch task statuses for ODAG %s/%s: %v", namespace, name, err)
	}
}

func updateODAGCompletion(dynClient dynamic.Interface, namespace, name string, makespan float64) {
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
