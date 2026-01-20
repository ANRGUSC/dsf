package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	metricsv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned"
)

// Constants
const (
	LinkBandwidthGbps = 0.1 // 100 Mbps link speed (matches tc limit)
	DataAgentPort     = 8080
	DataOutputPath    = "/data/dag-outputs"
)

// HEFT-specific structures
type HEFTSchedule struct {
	mu              sync.RWMutex
	taskAssignments map[string]map[string]string  // dagName -> taskName -> nodeName
	taskRanks       map[string]map[string]float64 // dagName -> taskName -> rank
	taskEFTs        map[string]map[string]float64 // dagName -> taskName -> EFT
}

type NodeInfo struct {
	Name              string
	AllocatableCPU    int64
	AllocatableMemory int64
	UsedCPU           int64
	UsedMemory        int64
}

var heftSchedule = &HEFTSchedule{
	taskAssignments: make(map[string]map[string]string),
	taskRanks:       make(map[string]map[string]float64),
	taskEFTs:        make(map[string]map[string]float64),
}

func main() {
	var kubeconfig string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "optional path to kubeconfig")
	flag.Parse()

	var cfg *rest.Config
	var err error
	if kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		panic(err)
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}

	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}

	// Watch for DAG resources
	go watchDAGs(dynamicClient, client, cfg)

	// Watch for completed DAG pods to trigger next steps
	dagPodWatcher := cache.NewListWatchFromClient(
		client.CoreV1().RESTClient(),
		"pods",
		corev1.NamespaceAll,
		fields.Everything(),
	)
	_, dagController := cache.NewInformer(
		dagPodWatcher,
		&corev1.Pod{},
		0,
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldPod := oldObj.(*corev1.Pod)
				newPod := newObj.(*corev1.Pod)

				if newPod.Labels["dag-name"] != "" {
					oldCompleted := isMainContainerCompleted(oldPod)
					newCompleted := isMainContainerCompleted(newPod)

					if !oldCompleted && newCompleted {
						log.Printf("DAG pod %s completed, checking for next steps", newPod.Name)
						processDAGCompletion(client, newPod, cfg)
					}
				}
			},
		},
	)

	stop := make(chan struct{})
	defer close(stop)
	dagController.Run(stop)
}

// Watch for DAG resources and compute HEFT schedule
func watchDAGs(dynamicClient dynamic.Interface, client *kubernetes.Clientset, cfg *rest.Config) {
	dagGVR := schema.GroupVersionResource{
		Group:    "workflow.example.com",
		Version:  "v1",
		Resource: "dags",
	}

	watcher, err := dynamicClient.Resource(dagGVR).Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("Error watching DAGs: %v", err)
		return
	}

	for event := range watcher.ResultChan() {
		switch event.Type {
		case "ADDED":
			obj := event.Object.(*unstructured.Unstructured)
			log.Printf("New DAG detected: %s - Computing HEFT schedule", obj.GetName())
			computeHEFTSchedule(client, obj, cfg)
			processDAG(client, obj, cfg)
		case "MODIFIED":
			obj := event.Object.(*unstructured.Unstructured)
			log.Printf("DAG modified: %s", obj.GetName())
			processDAG(client, obj, cfg)
		}
	}
}

// Compute HEFT schedule for entire DAG upfront
func computeHEFTSchedule(client *kubernetes.Clientset, obj *unstructured.Unstructured, cfg *rest.Config) {
	dagName := obj.GetName()
	steps, _, _ := unstructured.NestedSlice(obj.Object, "spec", "steps")

	// Get all available nodes
	allNodes, err := getNodeInfo(client, cfg)
	if err != nil {
		log.Printf("Error getting node info: %v", err)
		return
	}

	log.Printf("[HEFT] Computing schedule for DAG %s with %d tasks", dagName, len(steps))

	// Build task map for easy lookup (includes constraints)
	taskMap := make(map[string]map[string]interface{})
	taskConstraints := make(map[string][]string) // taskName -> allowed nodes

	for _, stepObj := range steps {
		step := stepObj.(map[string]interface{})
		stepName, _ := step["name"].(string)
		taskMap[stepName] = step

		// Get constraints for this step
		nodeNames, _, _ := unstructured.NestedStringSlice(step, "constraints", "nodeNames")
		if len(nodeNames) > 0 {
			taskConstraints[stepName] = nodeNames
			log.Printf("[HEFT] Task %s constrained to nodes: %v", stepName, nodeNames)
		}
	}

	// 1. Calculate average computation costs (only considering allowed nodes)
	avgCompCosts := make(map[string]float64)
	for taskName, task := range taskMap {
		allowedNodes := getNodesForTask(taskName, taskConstraints, allNodes)
		if len(allowedNodes) == 0 {
			log.Printf("[HEFT] Warning: No valid nodes for task %s, using all nodes", taskName)
			allowedNodes = allNodes
		}

		costs := make([]float64, 0)
		for _, node := range allowedNodes {
			cost := calculateComputationCost(task, node)
			costs = append(costs, cost)
		}
		avgCompCosts[taskName] = average(costs)
	}

	// 2. Calculate upward ranks
	ranks := make(map[string]float64)
	calculateUpwardRank(taskMap, avgCompCosts, ranks)

	// 3. Sort tasks by rank (descending)
	sortedTasks := make([]string, 0, len(ranks))
	for task := range ranks {
		sortedTasks = append(sortedTasks, task)
	}
	sort.Slice(sortedTasks, func(i, j int) bool {
		return ranks[sortedTasks[i]] > ranks[sortedTasks[j]]
	})

	log.Printf("[HEFT] Task ranks:")
	for _, task := range sortedTasks {
		log.Printf("  %s: %.2f", task, ranks[task])
	}

	// 4. Schedule tasks in order of rank (respecting constraints)
	assignments := make(map[string]string)
	taskEFTs := make(map[string]float64)
	taskStartTimes := make(map[string]float64)
	nodeAvailTime := make(map[string]float64)

	for _, taskName := range sortedTasks {
		task := taskMap[taskName]

		// Get allowed nodes for this task
		allowedNodes := getNodesForTask(taskName, taskConstraints, allNodes)
		if len(allowedNodes) == 0 {
			log.Printf("[HEFT] Warning: No valid nodes for task %s, using all nodes", taskName)
			allowedNodes = allNodes
		}

		bestNode := ""
		bestEFT := math.MaxFloat64

		// Try each ALLOWED node (respecting constraints)
		for _, node := range allowedNodes {
			eft := calculateEFT(task, node, taskMap, assignments, taskStartTimes, taskEFTs, nodeAvailTime)
			if eft < bestEFT {
				bestEFT = eft
				bestNode = node.Name
			}
		}

		assignments[taskName] = bestNode
		taskEFTs[taskName] = bestEFT

		// Calculate actual start time
		dependencies, _, _ := unstructured.NestedStringSlice(task, "dependencies")
		dataReadyTime := 0.0
		for _, dep := range dependencies {
			commCost := calculateCommunicationCost(taskMap[dep], assignments[dep], bestNode)
			depFinish := taskEFTs[dep] + commCost
			if depFinish > dataReadyTime {
				dataReadyTime = depFinish
			}
		}
		startTime := math.Max(dataReadyTime, nodeAvailTime[bestNode])
		taskStartTimes[taskName] = startTime

		// Update node available time
		var bestNodeObj NodeInfo
		for _, n := range allowedNodes {
			if n.Name == bestNode {
				bestNodeObj = n
				break
			}
		}
		compCost := calculateComputationCost(task, bestNodeObj)
		nodeAvailTime[bestNode] = startTime + compCost

		log.Printf("[HEFT] Task %s -> Node %s (EFT: %.2f, Start: %.2f)", taskName, bestNode, bestEFT, startTime)
	}

	// Store the schedule
	heftSchedule.mu.Lock()
	heftSchedule.taskAssignments[dagName] = assignments
	heftSchedule.taskRanks[dagName] = ranks
	heftSchedule.taskEFTs[dagName] = taskEFTs
	heftSchedule.mu.Unlock()

	log.Printf("[HEFT] Schedule computed for DAG %s - Estimated Makespan: %.2f seconds", dagName, getMaxEFT(taskEFTs))
}

// Get allowed nodes for a task based on constraints
func getNodesForTask(taskName string, taskConstraints map[string][]string, allNodes []NodeInfo) []NodeInfo {
	allowedNames, hasConstraints := taskConstraints[taskName]

	if !hasConstraints {
		return allNodes
	}

	// Filter nodes to only those in the constraint list
	allowedSet := make(map[string]bool)
	for _, name := range allowedNames {
		allowedSet[name] = true
	}

	var result []NodeInfo
	for _, node := range allNodes {
		if allowedSet[node.Name] {
			result = append(result, node)
		}
	}

	return result
}

// Calculate upward rank recursively
func calculateUpwardRank(taskMap map[string]map[string]interface{}, avgCompCosts map[string]float64, ranks map[string]float64) {
	var calcRank func(taskName string) float64
	calcRank = func(taskName string) float64 {
		if rank, exists := ranks[taskName]; exists {
			return rank
		}

		task := taskMap[taskName]

		maxSuccRank := 0.0
		for succName, succTask := range taskMap {
			succDeps, _, _ := unstructured.NestedStringSlice(succTask, "dependencies")
			for _, dep := range succDeps {
				if dep == taskName {
					commCost := getAvgCommunicationCost(task)
					succRank := calcRank(succName)
					if commCost+succRank > maxSuccRank {
						maxSuccRank = commCost + succRank
					}
					break
				}
			}
		}

		rank := avgCompCosts[taskName] + maxSuccRank
		ranks[taskName] = rank
		return rank
	}

	for taskName := range taskMap {
		calcRank(taskName)
	}
}

// Calculate EFT for a task on a node
func calculateEFT(task map[string]interface{}, node NodeInfo, taskMap map[string]map[string]interface{},
	assignments map[string]string, taskStartTimes, taskEFTs map[string]float64, nodeAvailTime map[string]float64) float64 {

	dependencies, _, _ := unstructured.NestedStringSlice(task, "dependencies")
	dataReadyTime := 0.0
	for _, dep := range dependencies {
		if assignedNode, exists := assignments[dep]; exists {
			commCost := calculateCommunicationCost(taskMap[dep], assignedNode, node.Name)
			depFinish := taskEFTs[dep] + commCost
			if depFinish > dataReadyTime {
				dataReadyTime = depFinish
			}
		}
	}

	est := math.Max(dataReadyTime, nodeAvailTime[node.Name])
	compCost := calculateComputationCost(task, node)

	return est + compCost
}

// Calculate computation cost for a task on a node
func calculateComputationCost(task map[string]interface{}, node NodeInfo) float64 {
	runtime, ok := task["runtime"].(int64)
	if !ok {
		runtimeInt, ok := task["runtime"].(int)
		if ok {
			runtime = int64(runtimeInt)
		}
	}
	if runtime <= 0 {
		return 10.0
	}
	return float64(runtime)
}

// Calculate communication cost between nodes
func calculateCommunicationCost(task map[string]interface{}, sourceNode, destNode string) float64 {
	if sourceNode == destNode {
		return 0.0
	}

	dataSize, _ := task["dataSize"].(string)
	if dataSize == "" {
		return 0.0
	}

	sizeInBytes := parseDataSize(dataSize)
	bandwidthBytesPerSec := LinkBandwidthGbps * 125000000

	return float64(sizeInBytes) / bandwidthBytesPerSec
}

// Get average communication cost for rank calculation
func getAvgCommunicationCost(task map[string]interface{}) float64 {
	return calculateCommunicationCost(task, "node1", "node2") * 0.5
}

// Parse data size string (e.g., "100MB" -> bytes)
func parseDataSize(s string) int64 {
	s = strings.ToUpper(s)
	s = strings.ReplaceAll(s, " ", "")

	var num int64
	fmt.Sscanf(s, "%d", &num)

	if strings.Contains(s, "GB") {
		return num * 1000000000
	} else if strings.Contains(s, "MB") {
		return num * 1000000
	} else if strings.Contains(s, "KB") {
		return num * 1000
	}
	return num
}

// Get node information with metrics
func getNodeInfo(client *kubernetes.Clientset, cfg *rest.Config) ([]NodeInfo, error) {
	nodes, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{
		FieldSelector: "spec.unschedulable!=true",
	})
	if err != nil {
		return nil, err
	}

	metricsClient, err := metricsv1beta1.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	nodeMetricsList, err := metricsClient.MetricsV1beta1().NodeMetricses().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	metricsMap := make(map[string]struct {
		cpu    int64
		memory int64
	})
	for _, metric := range nodeMetricsList.Items {
		metricsMap[metric.Name] = struct {
			cpu    int64
			memory int64
		}{
			cpu:    metric.Usage.Cpu().MilliValue(),
			memory: metric.Usage.Memory().Value(),
		}
	}

	nodeInfoList := make([]NodeInfo, 0, len(nodes.Items))
	for _, node := range nodes.Items {
		info := NodeInfo{
			Name:              node.Name,
			AllocatableCPU:    node.Status.Allocatable.Cpu().MilliValue(),
			AllocatableMemory: node.Status.Allocatable.Memory().Value(),
		}
		if metrics, exists := metricsMap[node.Name]; exists {
			info.UsedCPU = metrics.cpu
			info.UsedMemory = metrics.memory
		}
		nodeInfoList = append(nodeInfoList, info)
	}

	return nodeInfoList, nil
}

// Helper functions
func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func getMaxEFT(taskEFTs map[string]float64) float64 {
	maxEFT := 0.0
	for _, eft := range taskEFTs {
		if eft > maxEFT {
			maxEFT = eft
		}
	}
	return maxEFT
}

// Parse resource string
func parseResourceString(s string) int64 {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	if strings.HasSuffix(s, "m") || !strings.ContainsAny(s, "KMGTPE") {
		return q.MilliValue()
	}
	return q.Value()
}

// Process DAG and create pods for ready steps
func processDAG(client *kubernetes.Clientset, dag *unstructured.Unstructured, cfg *rest.Config) {
	namespace := dag.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}
	dagName := dag.GetName()

	// Get existing pods for this DAG
	pods, err := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("dag-name=%s", dagName),
	})
	if err != nil {
		log.Printf("Error listing pods for DAG %s: %v", dagName, err)
		return
	}

	// Build map of step -> node for completed steps
	stepNodes := getStepNodeMap(pods.Items)

	// Get steps from DAG spec
	steps, _, _ := unstructured.NestedSlice(dag.Object, "spec", "steps")
	log.Printf("[HEFT] Processing DAG %s: %d steps, %d existing pods", dagName, len(steps), len(pods.Items))

	for _, stepObj := range steps {
		step := stepObj.(map[string]interface{})
		stepName, _ := step["name"].(string)
		dependencies, _, _ := unstructured.NestedStringSlice(step, "dependencies")

		if isStepReady(stepName, dependencies, pods.Items) {
			log.Printf("[HEFT] Step %s is ready, creating pod", stepName)
			createStepPod(client, dag, step, namespace, stepNodes)
		}
	}
}

// Get map of completed step names to their node assignments
func getStepNodeMap(pods []corev1.Pod) map[string]string {
	stepNodes := make(map[string]string)
	for _, pod := range pods {
		stepName := pod.Labels["dag-step"]
		if stepName != "" && pod.Spec.NodeName != "" {
			if isMainContainerCompleted(&pod) {
				stepNodes[stepName] = pod.Spec.NodeName
			}
		}
	}
	return stepNodes
}

// Check if a step is ready to run
func isStepReady(stepName string, dependencies []string, pods []corev1.Pod) bool {
	for _, pod := range pods {
		if pod.Labels["dag-step"] == stepName {
			return false
		}
	}

	for _, dep := range dependencies {
		if !isDependencyCompleted(dep, pods) {
			return false
		}
	}
	return true
}

// Check if a dependency step has completed
func isDependencyCompleted(depName string, pods []corev1.Pod) bool {
	for _, pod := range pods {
		if pod.Labels["dag-step"] == depName {
			return isMainContainerCompleted(&pod)
		}
	}
	return false
}

// Check if main container completed successfully
func isMainContainerCompleted(pod *corev1.Pod) bool {
	stepName := pod.Labels["dag-step"]
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == stepName {
			if cs.State.Terminated != nil && cs.State.Terminated.ExitCode == 0 {
				return true
			}
		}
	}
	return false
}

// Create a pod for a DAG step using HEFT assignment
func createStepPod(client *kubernetes.Clientset, dag *unstructured.Unstructured, step map[string]interface{}, namespace string, stepNodes map[string]string) {
	dagName := dag.GetName()
	stepName, _ := step["name"].(string)
	image, _ := step["image"].(string)
	args, _, _ := unstructured.NestedStringSlice(step, "args")
	dependencies, _, _ := unstructured.NestedStringSlice(step, "dependencies")
	dataSize, _, _ := unstructured.NestedString(step, "dataSize")

	podName := fmt.Sprintf("%s-%s", dagName, stepName)

	// Get scheduler name - use dag-scheduler to enforce placement
	schedulerName, _, _ := unstructured.NestedString(dag.Object, "spec", "schedulerName")
	if schedulerName == "" {
		schedulerName = "dag-scheduler"
	}

	// Get HEFT-assigned node for this step
	heftSchedule.mu.RLock()
	assignments := heftSchedule.taskAssignments[dagName]
	heftSchedule.mu.RUnlock()

	assignedNode := ""
	if assignments != nil {
		assignedNode = assignments[stepName]
	}

	if assignedNode == "" {
		log.Printf("[HEFT] Warning: No HEFT assignment for step %s, will use scheduler default", stepName)
	} else {
		log.Printf("[HEFT] Step %s assigned to node %s by HEFT", stepName, assignedNode)
	}

	// Determine which dependencies are on which nodes
	depNodes := make(map[string]string)
	for _, dep := range dependencies {
		if node, ok := stepNodes[dep]; ok {
			depNodes[dep] = node
		}
	}

	// Build the data output path for this step
	outputPath := fmt.Sprintf("%s/%s/%s", DataOutputPath, dagName, stepName)

	// Build volume mounts
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "dag-output",
			MountPath: DataOutputPath,
		},
	}

	// Build the main command
	mainCmd := buildMainCommand(stepName, dagName, dependencies, depNodes, args, dataSize, outputPath)

	// Create the pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				"dag-name": dagName,
				"dag-step": stepName,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "workflow.example.com/v1",
				Kind:       "DAG",
				Name:       dagName,
				UID:        dag.GetUID(),
			}},
		},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:         stepName,
					Image:        image,
					Command:      []string{"sh", "-c"},
					Args:         []string{mainCmd},
					VolumeMounts: volumeMounts,
					Env:          buildEnvVars(dependencies, depNodes, dagName),
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "dag-output",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: DataOutputPath,
							Type: func() *corev1.HostPathType {
								t := corev1.HostPathDirectoryOrCreate
								return &t
							}(),
						},
					},
				},
			},
		},
	}

	// Set node affinity to the HEFT-assigned node
	// This uses Kubernetes native affinity which dag-scheduler respects
	if assignedNode != "" {
		pod.Spec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{assignedNode},
								},
							},
						},
					},
				},
			},
		}
		log.Printf("[HEFT] Set node affinity for step %s to node %s", stepName, assignedNode)
	}

	// Create the pod
	_, err := client.CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			log.Printf("Pod %s already exists", podName)
		} else {
			log.Printf("Error creating pod %s: %v", podName, err)
		}
		return
	}

	if len(dependencies) > 0 {
		depNodesList := make([]string, 0, len(depNodes))
		for dep, node := range depNodes {
			depNodesList = append(depNodesList, fmt.Sprintf("%s@%s", dep, node))
		}
		log.Printf("[HEFT] Created pod %s on %s (deps: %v)", podName, assignedNode, depNodesList)
	} else {
		log.Printf("[HEFT] Created pod %s on %s (no dependencies)", podName, assignedNode)
	}
}

// Build environment variables for dependency data locations
func buildEnvVars(dependencies []string, depNodes map[string]string, dagName string) []corev1.EnvVar {
	envVars := []corev1.EnvVar{
		{
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
		{
			Name:  "DAG_NAME",
			Value: dagName,
		},
	}

	for _, dep := range dependencies {
		if node, ok := depNodes[dep]; ok {
			envVars = append(envVars, corev1.EnvVar{
				Name:  fmt.Sprintf("DEP_%s_NODE", strings.ToUpper(dep)),
				Value: node,
			})
			envVars = append(envVars, corev1.EnvVar{
				Name:  fmt.Sprintf("DEP_%s_URL", strings.ToUpper(dep)),
				Value: fmt.Sprintf("http://%s:%d/%s/%s/output", node, DataAgentPort, dagName, dep),
			})
		}
	}

	return envVars
}

// Build main container command
func buildMainCommand(stepName, dagName string, dependencies []string, depNodes map[string]string, args []string, dataSize, outputPath string) string {
	var parts []string

	// Create output directory
	parts = append(parts, fmt.Sprintf("mkdir -p %s", outputPath))

	// Fetch data from dependencies - determine same-node vs cross-node at RUNTIME
	if len(dependencies) > 0 {
		parts = append(parts, "echo '=== Fetching input data ==='")
		parts = append(parts, "mkdir -p /tmp/inputs")

		for _, dep := range dependencies {
			depNode := depNodes[dep]
			if depNode == "" {
				continue
			}
			depOutputPath := fmt.Sprintf("%s/%s/%s/output", DataOutputPath, dagName, dep)
			localInputPath := fmt.Sprintf("/tmp/inputs/%s", dep)
			dataURL := fmt.Sprintf("http://%s:%d/%s/%s/output", depNode, DataAgentPort, dagName, dep)

			// Runtime check: if on same node, use hostPath; otherwise use Data Agent
			parts = append(parts, fmt.Sprintf(
				"DEP_NODE='%s'; "+
					"if [ \"$NODE_NAME\" = \"$DEP_NODE\" ]; then "+
					"  echo 'Reading %s from local hostPath (same node: '$NODE_NAME')...'; "+
					"  for i in 1 2 3 4 5 6 7 8 9 10; do "+
					"    if [ -f %s ]; then cp %s %s && echo 'Copied %s ('$(ls -lh %s | awk '{print $5}')')' && break; fi; "+
					"    echo 'Waiting for %s...'; sleep 2; "+
					"  done; "+
					"else "+
					"  echo 'Fetching %s from Data Agent at %s (cross-node)...'; "+
					"  for i in 1 2 3 4 5 6 7 8 9 10; do "+
					"    wget -q -O %s '%s' && echo 'Downloaded %s ('$(ls -lh %s | awk '{print $5}')')' && break; "+
					"    echo 'Retry '$i'...'; sleep 2; "+
					"  done; "+
					"fi",
				depNode,
				dep, depOutputPath, depOutputPath, localInputPath, dep, localInputPath, dep,
				dep, depNode, localInputPath, dataURL, dep, localInputPath,
			))
		}
		parts = append(parts, "echo '=== Input data ready ==='")
	}

	// Run user command
	if len(args) > 0 {
		parts = append(parts, args[0])
	}

	// Write output data
	if dataSize != "" && dataSize != "0" {
		parts = append(parts, fmt.Sprintf(
			"echo '=== Writing output data ===' && "+
				"SIZE='%s' && "+
				"NUM=$(echo $SIZE | sed 's/[^0-9]//g') && "+
				"UNIT=$(echo $SIZE | sed 's/[0-9]//g' | sed 's/B$//' | tr '[:lower:]' '[:upper:]') && "+
				"if [ -n \"$NUM\" ] && [ \"$NUM\" != '' ]; then "+
				"  dd if=/dev/zero of=%s/output bs=1${UNIT} count=${NUM} 2>/dev/null && "+
				"  echo 'Created output: '$(ls -lh %s/output | awk '{print $5}'); "+
				"else "+
				"  echo 'Output from %s' > %s/output; "+
				"fi",
			dataSize, outputPath, outputPath, stepName, outputPath,
		))
	} else {
		parts = append(parts, fmt.Sprintf(
			"echo 'Output from %s' > %s/output && echo 'Created output file'",
			stepName, outputPath,
		))
	}

	return strings.Join(parts, " && ")
}

// Process DAG completion and trigger next steps
func processDAGCompletion(client *kubernetes.Clientset, completedPod *corev1.Pod, cfg *rest.Config) {
	dagName := completedPod.Labels["dag-name"]
	if dagName == "" {
		return
	}

	dagGVR := schema.GroupVersionResource{
		Group:    "workflow.example.com",
		Version:  "v1",
		Resource: "dags",
	}

	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Printf("Error creating dynamic client: %v", err)
		return
	}

	dag, err := dynamicClient.Resource(dagGVR).Namespace(completedPod.Namespace).Get(context.Background(), dagName, metav1.GetOptions{})
	if err != nil {
		log.Printf("Error getting DAG %s: %v", dagName, err)
		return
	}

	processDAG(client, dag, cfg)
}
