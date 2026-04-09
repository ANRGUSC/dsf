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

var cdagGVR = schema.GroupVersionResource{
	Group:    "dsf.io",
	Version:  "v1",
	Resource: "cdags",
}

const (
	labelCDAGName = "dsf-cdag"
	labelTaskName = "dsf-task"
	zmqPort       = int32(5555)

	reconcileInterval = 30 * time.Second
)

var deployedCDAGs sync.Map // namespace/name -> bool

func main() {
	var kubeconfig string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig (leave empty for in-cluster)")
	flag.Parse()

	cfg, err := buildConfig(kubeconfig)
	if err != nil {
		log.Fatalf("[cdag-ctrl] failed to build config: %v", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[cdag-ctrl] failed to create kubernetes client: %v", err)
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[cdag-ctrl] failed to create dynamic client: %v", err)
	}

	log.Println("[cdag-ctrl] starting cdag-controller")

	go watchCDAGTemplates(dynClient)
	go watchCDAGs(dynClient, client)
	go runReconcileLoop(dynClient, client)

	// Block forever.
	select {}
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
// CDAG watcher
// --------------------------------------------------------------------------

func watchCDAGs(dynClient dynamic.Interface, client *kubernetes.Clientset) {
	for {
		watcher, err := dynClient.Resource(cdagGVR).Namespace("").Watch(
			context.Background(), metav1.ListOptions{},
		)
		if err != nil {
			log.Printf("[cdag-ctrl] error watching CDAGs: %v; retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("[cdag-ctrl] watching CDAG resources")
		for event := range watcher.ResultChan() {
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			switch string(event.Type) {
			case "ADDED":
				go deployCDAG(dynClient, client, obj)
			case "MODIFIED":
				go reconcileCDAG(dynClient, client, obj)
			case "DELETED":
				key := obj.GetNamespace() + "/" + obj.GetName()
				deployedCDAGs.Delete(key)
				log.Printf("[cdag-ctrl] CDAG %s deleted (pods/services cleaned up by owner refs)", key)
			}
		}
		log.Println("[cdag-ctrl] CDAG watcher closed; reconnecting in 2s")
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// Deploy: initial setup for a new CDAG
// --------------------------------------------------------------------------

func deployCDAG(dynClient dynamic.Interface, client *kubernetes.Clientset, obj *unstructured.Unstructured) {
	cdagName := obj.GetName()
	namespace := obj.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}
	key := namespace + "/" + cdagName

	if _, loaded := deployedCDAGs.LoadOrStore(key, true); loaded {
		return
	}

	log.Printf("[cdag-ctrl] deploying CDAG %s", key)

	tasks := extractTasks(obj)
	if len(tasks) == 0 {
		log.Printf("[cdag-ctrl] CDAG %s has no tasks; skipping", key)
		return
	}

	updateCDAGPhase(dynClient, namespace, cdagName, "Pending", "assigning tasks to nodes")

	// Get schedulable nodes for initial placement.
	nodes, err := getNodes(client)
	if err != nil || len(nodes) == 0 {
		log.Printf("[cdag-ctrl] no schedulable nodes for CDAG %s: %v", key, err)
		updateCDAGPhase(dynClient, namespace, cdagName, "Failed", "no schedulable nodes")
		return
	}

	// Assign tasks: pick a random node from the constraint list (or any node).
	assignMap := assignTasks(tasks, nodes)
	log.Printf("[cdag-ctrl] task placement for %s:", key)
	for task, node := range assignMap {
		log.Printf("[cdag-ctrl]   %-20s -> %s", task, node)
	}

	// Build service name map.
	svcNames := make(map[string]string, len(tasks))
	for _, t := range tasks {
		svcNames[t.Name] = fmt.Sprintf("%s-%s", cdagName, t.Name)
	}

	// Create a ClusterIP Service per task.
	for _, t := range tasks {
		if err := ensureService(client, namespace, svcNames[t.Name], cdagName, t.Name, obj.GetUID()); err != nil {
			log.Printf("[cdag-ctrl] error creating service for task %s in CDAG %s: %v", t.Name, key, err)
		}
	}

	// Determine restart policy (Always by default for CDAGs).
	restartPolicy := corev1.RestartPolicyAlways
	if rp, _, _ := unstructured.NestedString(obj.Object, "spec", "restartPolicy"); rp == "Never" {
		restartPolicy = corev1.RestartPolicyNever
	} else if rp == "OnFailure" {
		restartPolicy = corev1.RestartPolicyOnFailure
	}

	// Create replicas for each task.
	for _, t := range tasks {
		envVars := buildEnvVars(cdagName, namespace, t, tasks, svcNames)
		nodeName := assignMap[t.Name]
		for i := 0; i < t.Replicas; i++ {
			podName := fmt.Sprintf("%s-%s-%d", cdagName, t.Name, i)
			if err := ensurePod(client, namespace, podName, cdagName, t, nodeName, envVars, restartPolicy, obj.GetUID()); err != nil {
				log.Printf("[cdag-ctrl] error creating pod %s: %v", podName, err)
			}
		}
	}

	// Write initial task statuses with node assignments (pods may not be ready yet).
	var initStatuses []cdagTaskStatusEntry
	for _, t := range tasks {
		var podNames []string
		for i := 0; i < t.Replicas; i++ {
			podNames = append(podNames, fmt.Sprintf("%s-%s-%d", cdagName, t.Name, i))
		}
		initStatuses = append(initStatuses, cdagTaskStatusEntry{
			Name:            t.Name,
			DesiredReplicas: t.Replicas,
			ReadyReplicas:   0,
			PodNames:        podNames,
			Node:            assignMap[t.Name],
		})
	}
	updateCDAGTaskStatuses(dynClient, namespace, cdagName, initStatuses)

	updateCDAGPhase(dynClient, namespace, cdagName, "Running", "")
	log.Printf("[cdag-ctrl] CDAG %s is Running", key)

	// If this CDAG was created from a template, update the template status.
	if tplName := cdagTemplateNameFromLabels(obj.GetLabels()); tplName != "" {
		updateCDAGTemplateStatus(dynClient, namespace, tplName, cdagName, "Running")
		maxInst := extractRetentionMaxInstances(getTemplateForCDAG(obj))
		gcOldInstances(dynClient, namespace, tplName, maxInst)
	}
}

// --------------------------------------------------------------------------
// Reconcile: ensure desired replicas are running
// --------------------------------------------------------------------------

func runReconcileLoop(dynClient dynamic.Interface, client *kubernetes.Clientset) {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for range ticker.C {
		cdagList, err := dynClient.Resource(cdagGVR).Namespace("").List(
			context.Background(), metav1.ListOptions{},
		)
		if err != nil {
			log.Printf("[cdag-ctrl] reconcile: error listing CDAGs: %v", err)
			continue
		}
		for _, item := range cdagList.Items {
			go reconcileCDAG(dynClient, client, &item)
		}
	}
}

func reconcileCDAG(dynClient dynamic.Interface, client *kubernetes.Clientset, obj *unstructured.Unstructured) {
	cdagName := obj.GetName()
	namespace := obj.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}
	key := namespace + "/" + cdagName

	tasks := extractTasks(obj)
	if len(tasks) == 0 {
		return
	}

	restartPolicy := corev1.RestartPolicyAlways
	if rp, _, _ := unstructured.NestedString(obj.Object, "spec", "restartPolicy"); rp == "Never" {
		return // never restart; nothing to reconcile
	} else if rp == "OnFailure" {
		restartPolicy = corev1.RestartPolicyOnFailure
	}

	svcNames := make(map[string]string, len(tasks))
	for _, t := range tasks {
		svcNames[t.Name] = fmt.Sprintf("%s-%s", cdagName, t.Name)
	}

	allReady := true
	var taskStatuses []cdagTaskStatusEntry

	for _, t := range tasks {
		pods, err := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s,%s=%s", labelCDAGName, cdagName, labelTaskName, t.Name),
		})
		if err != nil {
			continue
		}

		// Count running/pending pods and collect their info.
		alive := 0
		ready := 0
		var podNames []string
		var nodeName string
		for _, pod := range pods.Items {
			if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
				alive++
				podNames = append(podNames, pod.Name)
				if pod.Status.Phase == corev1.PodRunning {
					ready++
					if nodeName == "" && pod.Spec.NodeName != "" {
						nodeName = pod.Spec.NodeName
					}
				}
			}
		}

		// If not yet running, try to get node from pending pod's spec.
		if nodeName == "" {
			for _, pod := range pods.Items {
				if pod.Spec.NodeName != "" {
					nodeName = pod.Spec.NodeName
					break
				}
			}
		}

		taskStatuses = append(taskStatuses, cdagTaskStatusEntry{
			Name:            t.Name,
			DesiredReplicas: t.Replicas,
			ReadyReplicas:   ready,
			PodNames:        podNames,
			Node:            nodeName,
		})

		missing := t.Replicas - alive
		if missing > 0 {
			allReady = false
			log.Printf("[cdag-ctrl] CDAG %s task %s: %d/%d replicas alive; recreating %d",
				key, t.Name, alive, t.Replicas, missing)

			// Pick a replacement node respecting constraints.
			nodes, _ := getNodes(client)
			nodeName := pickNode(t.Constraints, nodes)
			envVars := buildEnvVars(cdagName, namespace, t, tasks, svcNames)

			for i := 0; i < t.Replicas; i++ {
				podName := fmt.Sprintf("%s-%s-%d", cdagName, t.Name, i)
				// Only create if this specific pod is missing/failed.
				existing, err := client.CoreV1().Pods(namespace).Get(
					context.Background(), podName, metav1.GetOptions{},
				)
				if err == nil {
					if existing.Status.Phase != corev1.PodFailed {
						continue
					}
					// Delete failed pod before recreating.
					_ = client.CoreV1().Pods(namespace).Delete(
						context.Background(), podName, metav1.DeleteOptions{},
					)
				}
				_ = ensurePod(client, namespace, podName, cdagName, t, nodeName, envVars, restartPolicy, obj.GetUID())
			}
		}
	}

	if len(taskStatuses) > 0 {
		updateCDAGTaskStatuses(dynClient, namespace, cdagName, taskStatuses)
	}

	phase := "Running"
	if !allReady {
		phase = "Degraded"
	}
	updateCDAGPhase(dynClient, namespace, cdagName, phase, "")

	// Keep the template's lastInstancePhase in sync.
	if tplName := cdagTemplateNameFromLabels(obj.GetLabels()); tplName != "" {
		updateCDAGTemplatePhase(dynClient, namespace, tplName, cdagName, phase)
	}
}

// --------------------------------------------------------------------------
// Helpers: extract CDAG task specs
// --------------------------------------------------------------------------

type cdagTaskSpec struct {
	Name         string
	Image        string
	Command      []string
	Args         []string
	Dependencies []string
	Replicas     int
	CPU          string
	Memory       string
	Constraints  []string
	UserEnv      []corev1.EnvVar
}

func extractTasks(obj *unstructured.Unstructured) []cdagTaskSpec {
	rawTasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	tasks := make([]cdagTaskSpec, 0, len(rawTasks))
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
		replicas := 1
		if r, ok := t["replicas"].(int64); ok && r > 0 {
			replicas = int(r)
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
		tasks = append(tasks, cdagTaskSpec{
			Name: name, Image: image, Command: cmd, Args: args,
			Dependencies: deps, Replicas: replicas,
			Constraints: constraints, CPU: cpu, Memory: mem, UserEnv: userEnv,
		})
	}
	return tasks
}

// assignTasks assigns each CDAG task to a node using constraint-aware random placement.
// This is the CDAG-specific placement logic: tasks run continuously, so placement
// focuses on where the task is allowed to run rather than makespan optimisation.
func assignTasks(tasks []cdagTaskSpec, clusterNodes []string) map[string]string {
	nodeSet := make(map[string]bool, len(clusterNodes))
	for _, n := range clusterNodes {
		nodeSet[n] = true
	}
	result := make(map[string]string, len(tasks))
	for _, t := range tasks {
		result[t.Name] = pickNode(t.Constraints, clusterNodes)
		_ = nodeSet // used by pickNode indirectly via clusterNodes
	}
	return result
}

// pickNode selects a random node from the constraint list intersected with clusterNodes.
// Falls back to any cluster node if no constraint nodes are available.
func pickNode(constraints []string, clusterNodes []string) string {
	if len(clusterNodes) == 0 {
		return ""
	}
	if len(constraints) > 0 {
		nodeSet := make(map[string]bool, len(clusterNodes))
		for _, n := range clusterNodes {
			nodeSet[n] = true
		}
		var allowed []string
		for _, c := range constraints {
			if nodeSet[c] {
				allowed = append(allowed, c)
			}
		}
		if len(allowed) > 0 {
			return allowed[rand.Intn(len(allowed))]
		}
		log.Printf("[cdag-ctrl] no constraint nodes available in cluster; using any node")
	}
	return clusterNodes[rand.Intn(len(clusterNodes))]
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
		noSched := false
		for _, taint := range n.Spec.Taints {
			if taint.Effect == corev1.TaintEffectNoSchedule {
				noSched = true
				break
			}
		}
		if !noSched {
			nodes = append(nodes, n.Name)
		}
	}
	return nodes, nil
}

// buildEnvVars injects DSF_PEER_* and transport config into each CDAG task pod.
func buildEnvVars(cdagName, namespace string, task cdagTaskSpec, allTasks []cdagTaskSpec, svcNames map[string]string) []corev1.EnvVar {
	// Compute successors: tasks that list this task as a dependency.
	var successors []string
	for _, other := range allTasks {
		for _, dep := range other.Dependencies {
			if dep == task.Name {
				successors = append(successors, other.Name)
				break
			}
		}
	}

	env := []corev1.EnvVar{
		{Name: "DSF_TASK_NAME", Value: task.Name},
		{Name: "DSF_TRANSPORT_PATTERN", Value: "pubsub"},
		{Name: "DSF_PUB_PORT", Value: fmt.Sprintf("%d", zmqPort)},
		{Name: "DSF_DEPS", Value: strings.Join(task.Dependencies, ",")},
		{Name: "DSF_SUCCESSORS", Value: strings.Join(successors, ",")},
		{Name: "PYTHONUNBUFFERED", Value: "1"},
	}
	for _, other := range allTasks {
		if other.Name == task.Name {
			continue
		}
		fqdn := fmt.Sprintf("%s.%s.svc.cluster.local", svcNames[other.Name], namespace)
		envKey := "DSF_PEER_" + strings.ToUpper(strings.ReplaceAll(other.Name, "-", "_"))
		env = append(env, corev1.EnvVar{
			Name:  envKey,
			Value: fmt.Sprintf("zmq://%s:%d", fqdn, zmqPort),
		})
	}
	env = append(env, task.UserEnv...)
	return env
}

func ensureService(client *kubernetes.Clientset, namespace, svcName, cdagName, taskName string, ownerUID types.UID) error {
	_, err := client.CoreV1().Services(namespace).Get(context.Background(), svcName, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: namespace,
			Labels:    map[string]string{labelCDAGName: cdagName, labelTaskName: taskName},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "dsf.io/v1",
				Kind:       "CDAG",
				Name:       cdagName,
				UID:        ownerUID,
			}},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{labelCDAGName: cdagName, labelTaskName: taskName},
			Ports: []corev1.ServicePort{{
				Name:       "zmq",
				Protocol:   corev1.ProtocolTCP,
				Port:       zmqPort,
				TargetPort: intstr.FromInt32(zmqPort),
			}},
		},
	}
	_, err = client.CoreV1().Services(namespace).Create(context.Background(), svc, metav1.CreateOptions{})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	log.Printf("[cdag-ctrl] created service %s/%s", namespace, svcName)
	return nil
}

func ensurePod(client *kubernetes.Clientset, namespace, podName, cdagName string, task cdagTaskSpec, nodeName string, envVars []corev1.EnvVar, restartPolicy corev1.RestartPolicy, ownerUID types.UID) error {
	_, err := client.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	resources := parseResources(task.CPU, task.Memory)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				labelCDAGName: cdagName,
				labelTaskName: task.Name,
				"app":         cdagName,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "dsf.io/v1",
				Kind:       "CDAG",
				Name:       cdagName,
				UID:        ownerUID,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: restartPolicy,
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
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	log.Printf("[cdag-ctrl] created pod %s/%s (node: %s)", namespace, podName, nodeName)
	return nil
}

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

func updateCDAGPhase(dynClient dynamic.Interface, namespace, name, phase, message string) {
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"phase":   phase,
			"message": message,
		},
	}
	data, _ := json.Marshal(patch)
	if _, err := dynClient.Resource(cdagGVR).Namespace(namespace).Patch(
		context.Background(), name, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	); err != nil {
		log.Printf("[cdag-ctrl] failed to patch status for CDAG %s/%s: %v", namespace, name, err)
	}
}

type cdagTaskStatusEntry struct {
	Name            string
	DesiredReplicas int
	ReadyReplicas   int
	PodNames        []string
	Node            string
}

func updateCDAGTaskStatuses(dynClient dynamic.Interface, namespace, cdagName string, statuses []cdagTaskStatusEntry) {
	items := make([]interface{}, 0, len(statuses))
	for _, s := range statuses {
		podNames := s.PodNames
		if podNames == nil {
			podNames = []string{}
		}
		item := map[string]interface{}{
			"name":            s.Name,
			"desiredReplicas": s.DesiredReplicas,
			"readyReplicas":   s.ReadyReplicas,
			"podNames":        podNames,
		}
		if s.Node != "" {
			item["node"] = s.Node
		}
		items = append(items, item)
	}
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"tasks": items,
		},
	}
	data, _ := json.Marshal(patch)
	if _, err := dynClient.Resource(cdagGVR).Namespace(namespace).Patch(
		context.Background(), cdagName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	); err != nil {
		log.Printf("[cdag-ctrl] failed to patch task statuses for CDAG %s/%s: %v", namespace, cdagName, err)
	}
}
