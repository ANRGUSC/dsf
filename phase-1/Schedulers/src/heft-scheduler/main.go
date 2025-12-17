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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	metricsv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned"
)

// Constants
const (
	LinkBandwidthGbps = 0.1 // 100 Mbps link speed (reduced for realistic testing)
)

// DAG types
type DAG struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DAGSpec   `json:"spec,omitempty"`
	Status            DAGStatus `json:"status,omitempty"`
}

type DAGSpec struct {
	Steps         []DAGStep `json:"steps"`
	SchedulerName string    `json:"schedulerName"`
	Namespace     string    `json:"namespace"`
}

type DAGStep struct {
	Name         string                       `json:"name"`
	Image        string                       `json:"image"`
	Command      []string                     `json:"command"`
	Args         []string                     `json:"args"`
	Dependencies []string                     `json:"dependencies"`
	DataSize     string                       `json:"dataSize,omitempty"`
	Runtime      int                          `json:"runtime,omitempty"`
	CPU          string                       `json:"cpu,omitempty"`
	Memory       string                       `json:"memory,omitempty"`
	Resources    *corev1.ResourceRequirements `json:"resources,omitempty"`
	RetryPolicy  *RetryPolicy                 `json:"retryPolicy,omitempty"`
}

type RetryPolicy struct {
	MaxRetries int    `json:"maxRetries"`
	Backoff    string `json:"backoff"`
}

type DAGStatus struct {
	Phase        string       `json:"phase"`
	StepStatuses []StepStatus `json:"stepStatuses"`
}

type StepStatus struct {
	Name           string `json:"name"`
	Phase          string `json:"phase"`
	StartTime      string `json:"startTime,omitempty"`
	CompletionTime string `json:"completionTime,omitempty"`
	RetryCount     int    `json:"retryCount"`
}

// HEFT-specific structures
type HEFTSchedule struct {
	mu              sync.RWMutex
	taskAssignments map[string]map[string]string  // dagName -> taskName -> nodeName
	taskRanks       map[string]map[string]float64 // dagName -> taskName -> rank
	taskEFTs        map[string]map[string]float64 // dagName -> taskName -> EFT
}

type NodeInfo struct {
	Name              string
	AllocatableCPU    int64 // in millicores
	AllocatableMemory int64 // in bytes
	UsedCPU           int64 // in millicores
	UsedMemory        int64 // in bytes
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

	// Build config
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

	// Create dynamic client for CRD
	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}

	// Watch for DAG resources
	go watchDAGs(dynamicClient, client, cfg)

	// Watch for unscheduled Pods (filter by schedulerName in handler)
	watcher := cache.NewListWatchFromClient(
		client.CoreV1().RESTClient(),
		"pods",
		corev1.NamespaceAll,
		fields.Everything(), // Watch all pods, filter in handler
	)
	_, controller := cache.NewInformer(
		watcher,
		&corev1.Pod{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod := obj.(*corev1.Pod)
				// Only handle Pending pods with our scheduler name and no node assigned
				if pod.Spec.SchedulerName == "heft-scheduler" && pod.Spec.NodeName == "" && pod.Status.Phase == corev1.PodPending {
					scheduleHEFT(client, pod, cfg)
				}
			},
		},
	)

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

				// Check if this is a DAG pod whose main container just completed
				if newPod.Labels["dag-name"] != "" {
					oldMainCompleted := isMainContainerCompleted(oldPod)
					newMainCompleted := isMainContainerCompleted(newPod)

					if !oldMainCompleted && newMainCompleted {
						log.Printf("DAG pod %s main container completed, checking for next steps", newPod.Name)
						processDAGCompletion(client, newPod, cfg)
					}
				}
			},
		},
	)

	stop := make(chan struct{})
	defer close(stop)

	// Start both controllers
	go dagController.Run(stop)
	controller.Run(stop)
}

// Watch for DAG resources and calculate HEFT schedule
func watchDAGs(dynamicClient dynamic.Interface, client *kubernetes.Clientset, cfg *rest.Config) {
	// Define the DAG resource
	dagGVR := schema.GroupVersionResource{
		Group:    "workflow.example.com",
		Version:  "v1",
		Resource: "dags",
	}

	// Watch for DAG resources
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
			processDAGFromUnstructured(client, obj, cfg)
		case "MODIFIED":
			obj := event.Object.(*unstructured.Unstructured)
			log.Printf("DAG modified: %s", obj.GetName())
			processDAGFromUnstructured(client, obj, cfg)
		}
	}
}

// Compute HEFT schedule for entire DAG upfront
func computeHEFTSchedule(client *kubernetes.Clientset, obj *unstructured.Unstructured, cfg *rest.Config) {
	dagName := obj.GetName()
	steps, _, _ := unstructured.NestedSlice(obj.Object, "spec", "steps")

	// Get node information
	nodes, err := getNodeInfo(client, cfg)
	if err != nil {
		log.Printf("Error getting node info: %v", err)
		return
	}

	log.Printf("[HEFT] Computing schedule for DAG %s with %d tasks on %d nodes", dagName, len(steps), len(nodes))

	// Build task map for easy lookup
	taskMap := make(map[string]map[string]interface{})
	for _, stepObj := range steps {
		step := stepObj.(map[string]interface{})
		stepName, _ := step["name"].(string)
		taskMap[stepName] = step
	}

	// 1. Calculate average computation costs
	avgCompCosts := make(map[string]float64)
	for taskName, task := range taskMap {
		costs := make([]float64, 0)
		for _, node := range nodes {
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

	log.Printf("[HEFT] Task ranks: ")
	for _, task := range sortedTasks {
		log.Printf("  %s: %.2f", task, ranks[task])
	}

	// 4. Schedule tasks in order of rank
	assignments := make(map[string]string)
	taskEFTs := make(map[string]float64)
	taskStartTimes := make(map[string]float64)
	nodeAvailTime := make(map[string]float64)

	for _, taskName := range sortedTasks {
		task := taskMap[taskName]
		bestNode := ""
		bestEFT := math.MaxFloat64

		// Try each node
		for _, node := range nodes {
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
		// Find the actual node object for bestNode
		var bestNodeObj NodeInfo
		for _, n := range nodes {
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

	log.Printf("[HEFT] Schedule computed for DAG %s - Makespan: %.2f seconds", dagName, getMaxEFT(taskEFTs))
}

// Calculate upward rank recursively
func calculateUpwardRank(taskMap map[string]map[string]interface{}, avgCompCosts map[string]float64, ranks map[string]float64) {
	// Use memoization to avoid recalculating
	var calcRank func(taskName string) float64
	calcRank = func(taskName string) float64 {
		if rank, exists := ranks[taskName]; exists {
			return rank
		}

		task := taskMap[taskName]

		maxSuccRank := 0.0
		// Find successors (tasks that depend on this task)
		for succName, succTask := range taskMap {
			succDeps, _, _ := unstructured.NestedStringSlice(succTask, "dependencies")
			for _, dep := range succDeps {
				if dep == taskName {
					// This task is a successor
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

// Calculate Earliest Finish Time for a task on a node
func calculateEFT(task map[string]interface{}, node NodeInfo, taskMap map[string]map[string]interface{},
	assignments map[string]string, taskStartTimes, taskEFTs map[string]float64, nodeAvailTime map[string]float64) float64 {

	// Data ready time (when all inputs are available)
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

	// Earliest start time on this node
	est := math.Max(dataReadyTime, nodeAvailTime[node.Name])

	// Computation cost on this node
	compCost := calculateComputationCost(task, node)

	// EFT = EST + computation cost
	return est + compCost
}

// Calculate computation cost (weight) for a task on a node
func calculateComputationCost(task map[string]interface{}, node NodeInfo) float64 {
	// Use the actual runtime specified in the DAG spec (in seconds)
	runtime, ok := task["runtime"].(int)
	if !ok || runtime <= 0 {
		// Fallback to default if runtime not specified
		return 10.0
	}

	return float64(runtime)
}

// Calculate communication cost between nodes
func calculateCommunicationCost(task map[string]interface{}, sourceNode, destNode string) float64 {
	// If same node, no communication cost
	if sourceNode == destNode {
		return 0.0
	}

	dataSize, _ := task["dataSize"].(string)
	if dataSize == "" {
		return 0.0
	}

	// Parse data size (e.g., "100MB")
	sizeInBytes := parseDataSize(dataSize)

	// Communication time = data_size (bytes) / bandwidth (bytes/sec)
	// 1 Gbps = 125 MB/s = 125000000 bytes/sec
	bandwidthBytesPerSec := LinkBandwidthGbps * 125000000

	commTime := float64(sizeInBytes) / bandwidthBytesPerSec

	return commTime
}

// Get average communication cost (for rank calculation)
func getAvgCommunicationCost(task map[string]interface{}) float64 {
	// Average assumes 50% chance of different nodes
	return calculateCommunicationCost(task, "node1", "node2") * 0.5
}

// Parse resource string (e.g., "500m" -> 500, "1" -> 1000, "1Gi" -> bytes)
func parseResourceString(s string) int64 {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}

	// For CPU, return millivalue (millicores)
	if strings.HasSuffix(s, "m") || !strings.ContainsAny(s, "KMGTPE") {
		return q.MilliValue()
	}

	// For memory, return value in bytes
	return q.Value()
}

// Parse data size string (e.g., "100MB" -> bytes)
func parseDataSize(s string) int64 {
	s = strings.ToUpper(s)
	s = strings.ReplaceAll(s, " ", "")

	// Extract number
	var num int64
	fmt.Sscanf(s, "%d", &num)

	// Determine unit
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

	// Build metrics map
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

	// Build node info list
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

// Schedule pod using HEFT-computed assignment
func scheduleHEFT(client *kubernetes.Clientset, pod *corev1.Pod, cfg *rest.Config) {
	dagName := pod.Labels["dag-name"]
	stepName := pod.Labels["dag-step"]

	if dagName == "" || stepName == "" {
		log.Printf("Pod %s is not a DAG pod, skipping", pod.Name)
		return
	}

	// Get pre-computed node assignment
	heftSchedule.mu.RLock()
	assignments, exists := heftSchedule.taskAssignments[dagName]
	heftSchedule.mu.RUnlock()

	if !exists {
		log.Printf("No HEFT schedule found for DAG %s", dagName)
		return
	}

	assignedNode, exists := assignments[stepName]
	if !exists {
		log.Printf("No node assignment found for task %s in DAG %s", stepName, dagName)
		return
	}

	// Bind pod to the assigned node
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Target: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       assignedNode,
		},
	}

	err := client.CoreV1().Pods(pod.Namespace).Bind(context.Background(), binding, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error binding pod %s/%s to node %s: %v", pod.Namespace, pod.Name, assignedNode, err)
		return
	}
	log.Printf("[HEFT] Successfully bound pod %s to pre-assigned node %s", pod.Name, assignedNode)
}

// Process DAG and create pods for ready steps
func processDAGFromUnstructured(client *kubernetes.Clientset, obj *unstructured.Unstructured, cfg *rest.Config) {
	namespace := obj.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}

	// Get current pod statuses
	pods, err := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("dag-name=%s", obj.GetName()),
	})
	if err != nil {
		log.Printf("Error listing pods for DAG %s: %v", obj.GetName(), err)
		return
	}

	// Parse steps from unstructured object
	steps, _, _ := unstructured.NestedSlice(obj.Object, "spec", "steps")

	for _, stepObj := range steps {
		step := stepObj.(map[string]interface{})
		stepName, _ := step["name"].(string)
		dependencies, _, _ := unstructured.NestedStringSlice(step, "dependencies")

		if isStepReadyUnstructured(stepName, dependencies, pods.Items) {
			createStepPodFromUnstructured(client, obj, step, namespace)
		}
	}
}

// Rest of the functions are similar to random-scheduler...
// (createStepPodFromUnstructured, isStepReadyUnstructured, etc.)

// Check if a step is ready to run (dependencies completed) - unstructured version
func isStepReadyUnstructured(stepName string, dependencies []string, pods []corev1.Pod) bool {
	// Check if pod already exists
	for _, pod := range pods {
		if pod.Labels["dag-step"] == stepName {
			return false // Already exists
		}
	}

	// Check dependencies
	for _, dep := range dependencies {
		if !isDependencyCompleted(dep, pods) {
			return false
		}
	}
	return true
}

// Create a pod for a DAG step - unstructured version
func createStepPodFromUnstructured(client *kubernetes.Clientset, obj *unstructured.Unstructured, step map[string]interface{}, namespace string) {
	stepName, _ := step["name"].(string)
	image, _ := step["image"].(string)
	command, _, _ := unstructured.NestedStringSlice(step, "command")
	args, _, _ := unstructured.NestedStringSlice(step, "args")
	dependencies, _, _ := unstructured.NestedStringSlice(step, "dependencies")
	dataSize, _, _ := unstructured.NestedString(step, "dataSize")
	schedulerName, _, _ := unstructured.NestedString(obj.Object, "spec", "schedulerName")
	if schedulerName == "" {
		schedulerName = "heft-scheduler"
	}

	podName := fmt.Sprintf("%s-%s", obj.GetName(), stepName)

	// Get dependency node assignments from HEFT schedule
	depNodes := getDependencyNodesHEFT(obj.GetName(), dependencies)
	useSharedMemory, sharedNode := allDepsOnSameNode(depNodes)

	// Get current task's assigned node from HEFT schedule
	heftSchedule.mu.RLock()
	assignments, exists := heftSchedule.taskAssignments[obj.GetName()]
	heftSchedule.mu.RUnlock()

	var currentTaskNode string
	if exists {
		currentTaskNode = assignments[stepName]
		// Check if current task is also on the same node as all dependencies
		if useSharedMemory && currentTaskNode == sharedNode {
			log.Printf("[HEFT] Step %s and all dependencies are on node %s, using shared memory", stepName, sharedNode)
		} else if useSharedMemory && currentTaskNode != sharedNode {
			// Dependencies are on one node, but task is scheduled elsewhere - use TCP
			useSharedMemory = false
			log.Printf("[HEFT] Step %s is on %s but dependencies are on %s, using TCP networking", stepName, currentTaskNode, sharedNode)
		} else if len(depNodes) > 0 {
			log.Printf("[HEFT] Dependencies for step %s are on different nodes, using TCP networking", stepName)
		}
	}

	// Build command for main container that handles data transfer
	mainCommand := buildMainContainerCommand(dependencies, obj.GetName(), command, args, depNodes, useSharedMemory, sharedNode)

	// Determine if we need TCP server (only if there are cross-node dependencies)
	needsTCPServer := !useSharedMemory && len(dependencies) > 0

	// Prepare volume mounts for shared memory
	volumeMounts := []corev1.VolumeMount{}
	if useSharedMemory {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "shared-data",
			MountPath: "/shared-data",
		})
	}

	// Create containers: main container + optional sidecar TCP server
	containers := []corev1.Container{
		{
			Name:         stepName,
			Image:        image,
			Command:      []string{"sh", "-c"},
			Args:         []string{mainCommand},
			VolumeMounts: volumeMounts,
		},
	}

	// Add sidecar TCP server only if needed (cross-node dependencies)
	if needsTCPServer {
		containers = append(containers, corev1.Container{
			Name:    "data-server",
			Image:   "busybox",
			Command: []string{"sh", "-c"},
			Ports: []corev1.ContainerPort{
				{
					Name:          "data-port",
					ContainerPort: 8080,
					Protocol:      corev1.ProtocolTCP,
				},
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{
						Command: []string{"sh", "-c", "test -f /data/output.txt"},
					},
				},
				InitialDelaySeconds: 2,
				PeriodSeconds:       5,
			},
			Args: []string{
				fmt.Sprintf(`
					# Create output data file with actual size
					mkdir -p /data
					
					# Parse size (e.g., "100MB" -> 100 and M)
					SIZE="%s"
					echo "Creating data file of size $SIZE..."
					
					# Extract number and unit (e.g., "100MB" -> "100" and "M")
					NUM=$(echo $SIZE | sed 's/[^0-9]//g')
					UNIT=$(echo $SIZE | sed 's/[0-9]//g' | sed 's/B$//' | tr '[:lower:]' '[:upper:]')
					
					if [ -z "$NUM" ] || [ "$NUM" = "" ]; then
						# If no size specified, create small file
						echo "Data from %s" > /data/output.txt
					else
						# Create actual sized file with zeros (faster than urandom)
						# Use busybox-compatible syntax: bs=1M count=100
						dd if=/dev/zero of=/data/output.txt bs=1${UNIT} count=${NUM} 2>/dev/null || echo "Data from %s" > /data/output.txt
					fi
					
					FILE_SIZE=$(ls -lh /data/output.txt | awk '{print $5}')
					echo "Created output file: $FILE_SIZE"
					
					# Start TCP server on port 8080 for 300 seconds (5 minutes)
					# This gives enough time for GB file transfers at 100 Mbps
					echo "Starting TCP server on port 8080..."
					
					# Run nc in a loop with timeout check
					end_time=$(($(date +%%s) + 300))
					while [ $(date +%%s) -lt $end_time ]; do
						# Calculate remaining time
						remaining=$((end_time - $(date +%%s)))
						if [ $remaining -le 0 ]; then
							break
						fi
						
						# Run nc with timeout (max 10 seconds per connection)
						timeout 10 sh -c "cat /data/output.txt | nc -l -p 8080" >/dev/null 2>&1 && echo "Served $FILE_SIZE to dependent pod"
						
						# Small delay to let port be released (avoid TIME_WAIT issues)
						sleep 1
					done
					echo "Data server shutting down after timeout"
				`, dataSize, stepName, stepName),
			},
		})
	} else if useSharedMemory {
		// For shared memory, create data in hostPath volume
		containers = append(containers, corev1.Container{
			Name:         "data-writer",
			Image:        "busybox",
			Command:      []string{"sh", "-c"},
			VolumeMounts: volumeMounts,
			Args: []string{
				fmt.Sprintf(`
					# Create output data file in shared memory location
					SHARED_DIR="/shared-data/%s-%s"
					mkdir -p $SHARED_DIR
					
					# Parse size (e.g., "100MB" -> 100 and M)
					SIZE="%s"
					echo "Creating data file of size $SIZE in shared memory..."
					
					# Extract number and unit (e.g., "100MB" -> "100" and "M")
					NUM=$(echo $SIZE | sed 's/[^0-9]//g')
					UNIT=$(echo $SIZE | sed 's/[0-9]//g' | sed 's/B$//' | tr '[:lower:]' '[:upper:]')
					
					if [ -z "$NUM" ] || [ "$NUM" = "" ]; then
						# If no size specified, create small file
						echo "Data from %s" > $SHARED_DIR/output.txt
					else
						# Create actual sized file with zeros (faster than urandom)
						# Use busybox-compatible syntax: bs=1M count=100
						dd if=/dev/zero of=$SHARED_DIR/output.txt bs=1${UNIT} count=${NUM} 2>/dev/null || echo "Data from %s" > $SHARED_DIR/output.txt
					fi
					
					FILE_SIZE=$(ls -lh $SHARED_DIR/output.txt | awk '{print $5}')
					echo "Created output file in shared memory: $FILE_SIZE"
					
					# Keep container running until main container completes
					# Wait for main container to finish (check via shared file or timeout)
					sleep 300
				`, obj.GetName(), stepName, dataSize, stepName, stepName),
			},
		})
	}

	// Prepare volumes for shared memory
	volumes := []corev1.Volume{}

	if useSharedMemory {
		// Add hostPath volume for shared memory
		volumes = append(volumes, corev1.Volume{
			Name: "shared-data",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp/shared-data",
					Type: func() *corev1.HostPathType {
						dirOrCreate := corev1.HostPathDirectoryOrCreate
						return &dirOrCreate
					}(),
				},
			},
		})
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				"dag-name": obj.GetName(),
				"dag-step": stepName,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "workflow.example.com/v1",
				Kind:       "DAG",
				Name:       obj.GetName(),
				UID:        obj.GetUID(),
			}},
		},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Containers:    containers,
			RestartPolicy: corev1.RestartPolicyNever,
			Volumes:       volumes,
		},
	}

	_, err := client.CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error creating pod for step %s: %v", stepName, err)
		return
	}

	if useSharedMemory {
		log.Printf("[HEFT] Created pod for step %s in DAG %s with shared memory (node: %s)", stepName, obj.GetName(), sharedNode)
	} else if needsTCPServer {
		log.Printf("[HEFT] Created pod for step %s in DAG %s with TCP sidecar", stepName, obj.GetName())
	} else {
		log.Printf("[HEFT] Created pod for step %s in DAG %s", stepName, obj.GetName())
	}

	// Create Service for this pod only if TCP server is needed
	if needsTCPServer {
		createServiceForStep(client, obj.GetName(), stepName, namespace, obj.GetUID())
	}
}

// Get dependency node assignments from HEFT schedule
func getDependencyNodesHEFT(dagName string, dependencies []string) map[string]string {
	depNodes := make(map[string]string)

	heftSchedule.mu.RLock()
	assignments, exists := heftSchedule.taskAssignments[dagName]
	heftSchedule.mu.RUnlock()

	if !exists {
		return depNodes
	}

	for _, dep := range dependencies {
		if node, ok := assignments[dep]; ok {
			depNodes[dep] = node
		}
	}

	return depNodes
}

// Check if all dependencies are on the same node
func allDepsOnSameNode(depNodes map[string]string) (bool, string) {
	if len(depNodes) == 0 {
		return false, ""
	}

	var commonNode string
	for _, node := range depNodes {
		if commonNode == "" {
			commonNode = node
		} else if node != commonNode {
			return false, ""
		}
	}
	return true, commonNode
}

// Build command for main container that downloads data from dependencies first
func buildMainContainerCommand(dependencies []string, dagName string, command []string, args []string, depNodes map[string]string, useSharedMemory bool, sharedNode string) string {
	var cmdParts []string

	// Add data transfer logic if there are dependencies
	if len(dependencies) > 0 {
		cmdParts = append(cmdParts, "echo 'Fetching data from dependencies...'")
		for _, dep := range dependencies {
			if useSharedMemory && depNodes[dep] == sharedNode {
				// Use shared memory (hostPath volume)
				sharedPath := fmt.Sprintf("/shared-data/%s-%s/output.txt", dagName, dep)
				cmdParts = append(cmdParts, fmt.Sprintf(
					"echo 'Reading from shared memory for %s...'; "+
						"if [ -f %s ]; then "+
						"  cp %s /tmp/%s-data.txt && "+
						"  FILE_SIZE=$(ls -lh /tmp/%s-data.txt | awk '{print $5}'); "+
						"  echo 'Successfully read file from %s via shared memory (size: '$FILE_SIZE')'; "+
						"  echo 'First 10 bytes of received data:'; "+
						"  head -c 10 /tmp/%s-data.txt | od -An -tx1; "+
						"else "+
						"  echo 'ERROR: Shared file %s not found, waiting...'; "+
						"  for i in 1 2 3 4 5; do "+
						"    sleep 2; "+
						"    if [ -f %s ]; then cp %s /tmp/%s-data.txt && break; fi; "+
						"  done; "+
						"fi",
					dep, sharedPath, sharedPath, dep, dep, dep, dep, sharedPath, sharedPath, sharedPath, dep,
				))
			} else {
				// Use TCP networking
				serviceName := fmt.Sprintf("%s-%s-svc", dagName, dep)
				// Download the file with retry logic
				cmdParts = append(cmdParts, fmt.Sprintf(
					"echo 'Connecting to %s (%s:8080)...'; "+
						"for i in 1 2 3 4 5; do "+
						"  echo 'Attempt '$i' to connect to %s...'; "+
						"  nc -w 10 %s 8080 > /tmp/%s-data.txt 2>&1 && echo 'Connected successfully!' && break || echo 'Connection failed, retrying...'; "+
						"  sleep 2; "+
						"done",
					dep, serviceName, dep, serviceName, dep,
				))
				// Verify and log the downloaded file
				cmdParts = append(cmdParts, fmt.Sprintf(
					"if [ -f /tmp/%s-data.txt ]; then "+
						"FILE_SIZE=$(ls -lh /tmp/%s-data.txt | awk '{print $5}'); "+
						"echo 'Successfully received file from %s (size: '$FILE_SIZE')'; "+
						"echo 'First 10 bytes of received data:'; "+
						"head -c 10 /tmp/%s-data.txt | od -An -tx1; "+
						"else echo 'ERROR: File /tmp/%s-data.txt not created'; fi",
					dep, dep, dep, dep, dep,
				))
			}
		}
		cmdParts = append(cmdParts, "echo 'Data transfer complete'")
	}

	// Add the original user command
	if len(args) > 0 {
		cmdParts = append(cmdParts, args[0])
	} else {
		cmdParts = append(cmdParts, "echo 'No command specified'")
	}

	return fmt.Sprintf("%s", joinCommands(cmdParts))
}

// Helper to join commands with semicolons
func joinCommands(parts []string) string {
	result := ""
	for i, part := range parts {
		result += part
		if i < len(parts)-1 {
			result += "; "
		}
	}
	return result
}

// Create a Service for a step pod so dependents can connect to it
func createServiceForStep(client *kubernetes.Clientset, dagName, stepName, namespace string, dagUID types.UID) {
	serviceName := fmt.Sprintf("%s-%s-svc", dagName, stepName)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: namespace,
			Labels: map[string]string{
				"dag-name": dagName,
				"dag-step": stepName,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "workflow.example.com/v1",
				Kind:       "DAG",
				Name:       dagName,
				UID:        dagUID,
			}},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"dag-name": dagName,
				"dag-step": stepName,
			},
			PublishNotReadyAddresses: true, // Include not-ready pods (for sidecar access)
			Ports: []corev1.ServicePort{
				{
					Name:     "data-transfer",
					Protocol: corev1.ProtocolTCP,
					Port:     8080,
				},
			},
		},
	}

	_, err := client.CoreV1().Services(namespace).Create(context.Background(), service, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error creating service for step %s: %v", stepName, err)
	} else {
		log.Printf("Created service %s for step %s", serviceName, stepName)
	}
}

// Check if the main container (non-sidecar) of a pod has completed
func isMainContainerCompleted(pod *corev1.Pod) bool {
	for _, containerStatus := range pod.Status.ContainerStatuses {
		// Skip the sidecar container
		if containerStatus.Name == "data-server" {
			continue
		}
		// Check if main container completed successfully
		if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode == 0 {
			return true
		}
	}
	return false
}

// Check if a dependency is completed
func isDependencyCompleted(depName string, pods []corev1.Pod) bool {
	for _, pod := range pods {
		if pod.Labels["dag-step"] == depName {
			// Check if the main container (not the sidecar) has completed successfully
			for _, containerStatus := range pod.Status.ContainerStatuses {
				// Skip the sidecar container
				if containerStatus.Name == "data-server" {
					continue
				}
				// Check if main container completed successfully
				if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode == 0 {
					return true
				}
			}
		}
	}
	return false
}

// Process DAG completion and trigger next steps
func processDAGCompletion(client *kubernetes.Clientset, completedPod *corev1.Pod, cfg *rest.Config) {
	dagName := completedPod.Labels["dag-name"]
	if dagName == "" {
		return
	}

	log.Printf("Pod %s completed, checking all DAG steps for %s", completedPod.Name, dagName)

	// Get the DAG resource
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

	// Process the DAG to create next steps
	processDAGFromUnstructured(client, dag, cfg)
}
