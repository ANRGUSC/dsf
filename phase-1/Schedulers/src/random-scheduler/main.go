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

				// Check if this is a DAG pod that just completed
				if oldPod.Status.Phase != corev1.PodSucceeded &&
					newPod.Status.Phase == corev1.PodSucceeded &&
					newPod.Labels["dag-name"] != "" {
					log.Printf("DAG pod %s completed, checking for next steps", newPod.Name)
					processDAGCompletion(client, newPod, cfg)
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
	schedulerName, _, _ := unstructured.NestedString(obj.Object, "spec", "schedulerName")
	if schedulerName == "" {
		schedulerName = "random-scheduler"
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", obj.GetName(), stepName),
			Namespace: namespace,
			Labels: map[string]string{
				"dag-name": obj.GetName(),
				"dag-step": stepName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Containers: []corev1.Container{
				{
					Name:    stepName,
					Image:   image,
					Command: command,
					Args:    args,
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	_, err := client.CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error creating pod for step %s: %v", stepName, err)
	} else {
		log.Printf("Created pod for step %s in DAG %s", stepName, obj.GetName())
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

// Check if a dependency is completed
func isDependencyCompleted(depName string, pods []corev1.Pod) bool {
	for _, pod := range pods {
		if pod.Labels["dag-step"] == depName && pod.Status.Phase == corev1.PodSucceeded {
			return true
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
