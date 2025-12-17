package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	metricsv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned"

	// Add these for CRD support

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
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

func main() {
	rand.Seed(time.Now().UnixNano())
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

	// Watch for unscheduled Pods with our schedulerName
	watcher := cache.NewListWatchFromClient(
		client.CoreV1().RESTClient(),
		"pods",
		corev1.NamespaceAll,
		fields.ParseSelectorOrDie("spec.schedulerName=random-scheduler,status.phase=Pending"),
	)
	_, controller := cache.NewInformer(
		watcher,
		&corev1.Pod{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod := obj.(*corev1.Pod)
				scheduleRandom(client, pod, cfg)
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

// Watch for DAG resources and create pods
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
			log.Printf("New DAG detected: %s", obj.GetName())
			processDAGFromUnstructured(client, obj, cfg)
		case "MODIFIED":
			obj := event.Object.(*unstructured.Unstructured)
			log.Printf("DAG modified: %s", obj.GetName())
			processDAGFromUnstructured(client, obj, cfg)
		}
	}
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
	log.Printf("Processing DAG %s with %d steps, found %d existing pods", obj.GetName(), len(steps), len(pods.Items))

	for _, stepObj := range steps {
		step := stepObj.(map[string]interface{})
		stepName, _ := step["name"].(string)
		dependencies, _, _ := unstructured.NestedStringSlice(step, "dependencies")

		log.Printf("Checking step %s with dependencies: %v", stepName, dependencies)

		if isStepReadyUnstructured(stepName, dependencies, pods.Items) {
			log.Printf("Step %s is ready, creating pod", stepName)
			createStepPodFromUnstructured(client, obj, step, namespace)
		} else {
			log.Printf("Step %s is not ready yet", stepName)
		}
	}
}

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
		schedulerName = "random-scheduler"
	}

	podName := fmt.Sprintf("%s-%s", obj.GetName(), stepName)

	// Get dependency node assignments and check if all are on same node
	depNodes := getDependencyNodes(client, obj.GetName(), dependencies, namespace)
	useSharedMemory, sharedNode := allDepsOnSameNode(depNodes)

	if useSharedMemory {
		log.Printf("All dependencies for step %s are on node %s, using shared memory", stepName, sharedNode)
	} else if len(depNodes) > 0 {
		log.Printf("Dependencies for step %s are on different nodes, using TCP networking", stepName)
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

	// Add node affinity if using shared memory
	if useSharedMemory && sharedNode != "" {
		pod.Spec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{sharedNode},
								},
							},
						},
					},
				},
			},
		}
	}

	_, err := client.CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error creating pod for step %s: %v", stepName, err)
		return
	}

	if useSharedMemory {
		log.Printf("Created pod for step %s in DAG %s with shared memory (node: %s)", stepName, obj.GetName(), sharedNode)
	} else if needsTCPServer {
		log.Printf("Created pod for step %s in DAG %s with TCP sidecar", stepName, obj.GetName())
	} else {
		log.Printf("Created pod for step %s in DAG %s", stepName, obj.GetName())
	}

	// Create Service for this pod only if TCP server is needed
	if needsTCPServer {
		createServiceForStep(client, obj.GetName(), stepName, namespace, obj.GetUID())
	}
}

// Get dependency node assignments
func getDependencyNodes(client *kubernetes.Clientset, dagName string, dependencies []string, namespace string) map[string]string {
	depNodes := make(map[string]string)

	for _, dep := range dependencies {
		podName := fmt.Sprintf("%s-%s", dagName, dep)
		pod, err := client.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
		if err == nil && pod.Spec.NodeName != "" {
			depNodes[dep] = pod.Spec.NodeName
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

// Check if a step is ready to run (dependencies completed)
func isStepReady(step DAGStep, pods []corev1.Pod) bool {
	// Check if pod already exists
	for _, pod := range pods {
		if pod.Labels["dag-step"] == step.Name {
			return false // Already exists
		}
	}

	// Check dependencies
	for _, dep := range step.Dependencies {
		if !isDependencyCompleted(dep, pods) {
			return false
		}
	}
	return true
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

// Create a pod for a DAG step
func createStepPod(client *kubernetes.Clientset, dag *DAG, step DAGStep) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", dag.Name, step.Name),
			Namespace: dag.Spec.Namespace,
			Labels: map[string]string{
				"dag-name": dag.Name,
				"dag-step": step.Name,
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: dag.Spec.SchedulerName,
			Containers: []corev1.Container{
				{
					Name:      step.Name,
					Image:     step.Image,
					Command:   step.Command,
					Args:      step.Args,
					Resources: *step.Resources,
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	_, err := client.CoreV1().Pods(dag.Spec.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error creating pod for step %s: %v", step.Name, err)
	} else {
		log.Printf("Created pod for step %s in DAG %s", step.Name, dag.Name)
	}
}

// scheduleRandom picks a node at random and binds the pod
func scheduleRandom(client *kubernetes.Clientset, pod *corev1.Pod, cfg *rest.Config) {
	// List all Ready nodes
	nodes, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{
		FieldSelector: "spec.unschedulable!=true",
	})
	if err != nil {
		log.Printf("Error listing nodes: %v", err)
		return
	}
	if len(nodes.Items) == 0 {
		log.Printf("No eligible nodes found for scheduling")
		return
	}

	// --- Use the Metrics Server to get CPU and Memory usage in percent for each node ---
	metricsClient, err := metricsv1beta1.NewForConfig(cfg)
	if err != nil {
		log.Printf("Error creating metrics client: %v", err)
		return
	}

	nodeMetricsList, err := metricsClient.MetricsV1beta1().NodeMetricses().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("Error fetching node metrics: %v", err)
		return
	}

	// Add this after line 94 to debug:
	log.Printf("Found %d nodes total", len(nodes.Items))
	log.Printf("Found %d node metrics", len(nodeMetricsList.Items))

	// List all node names from metrics
	for _, metric := range nodeMetricsList.Items {
		log.Printf("Metrics available for node: %s", metric.Name)
	}

	// List all node names from nodes list
	for _, node := range nodes.Items {
		log.Printf("Node in cluster: %s", node.Name)
	}

	// Map for quick lookup by node name
	nodeUsage := make(map[string]map[string]string)
	nodeMetricsMap := make(map[string]metav1.Time)
	cpuPercentage := make(map[string]float64)
	memPercentage := make(map[string]float64)

	for _, node := range nodes.Items {
		nodeUsage[node.Name] = make(map[string]string)
		allocatableCPU := node.Status.Allocatable.Cpu()
		allocatableMem := node.Status.Allocatable.Memory()

		for _, metric := range nodeMetricsList.Items {
			if metric.Name == node.Name {
				cpuQuantity := metric.Usage.Cpu()    // cores as resource.Quantity
				memQuantity := metric.Usage.Memory() // bytes as resource.Quantity

				// Compute percent usage
				cpuPercent := float64(cpuQuantity.MilliValue()) / float64(allocatableCPU.MilliValue()) * 100
				memPercent := float64(memQuantity.Value()) / float64(allocatableMem.Value()) * 100

				nodeUsage[node.Name]["cpu_percent"] = fmt.Sprintf("%.2f", cpuPercent)
				nodeUsage[node.Name]["memory_percent"] = fmt.Sprintf("%.2f", memPercent)
				nodeUsage[node.Name]["cpu"] = allocatableCPU.String()
				nodeUsage[node.Name]["memory"] = allocatableMem.String()
				cpuPercentage[node.Name] = cpuPercent
				memPercentage[node.Name] = memPercent
				nodeMetricsMap[node.Name] = metric.Timestamp
			}
		}
	}

	// Pretty-print node info with proper indentation and spacing
	log.Printf("Available nodes and their CPU/memory allocatable and percent usage:\n")
	for _, node := range nodes.Items {
		usage := nodeUsage[node.Name]
		log.Printf("  Node: %s\n    CPU:    %s (Used: %s%%)\n    Memory: %s (Used: %s%%)\n",
			node.Name,
			usage["cpu"], usage["cpu_percent"],
			usage["memory"], usage["memory_percent"],
		)
	}

	// Pick a random node
	choice := nodes.Items[rand.Intn(len(nodes.Items))].Name

	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Target: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       choice,
		},
	}

	// Create the binding subresource to assign node
	err = client.CoreV1().Pods(pod.Namespace).Bind(context.Background(), binding, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error binding pod %s/%s to node %s: %v", pod.Namespace, pod.Name, choice, err)
		return
	}
	log.Printf("Successfully bound pod %s/%s to node %s (CPU: %s%%, Mem: %s%%)", pod.Namespace, pod.Name, choice, nodeUsage[choice]["cpu_percent"], nodeUsage[choice]["memory_percent"])
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
