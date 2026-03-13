package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// Track deployed task graphs
type TaskGraphState struct {
	mu     sync.RWMutex
	graphs map[string]*GraphState // graphName -> state
}

type GraphState struct {
	mu        sync.RWMutex
	Name      string
	Namespace string
	Tasks     map[string]bool // taskName -> deployed
}

var taskGraphState = &TaskGraphState{
	graphs: make(map[string]*GraphState),
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

	// Watch for ContinuousTaskGraph resources
	go watchTaskGraphs(dynamicClient, client, cfg)

	// Watch for unscheduled Pods
	watcher := cache.NewListWatchFromClient(
		client.CoreV1().RESTClient(),
		"pods",
		corev1.NamespaceAll,
		fields.Everything(),
	)
	_, controller := cache.NewInformer(
		watcher,
		&corev1.Pod{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod := obj.(*corev1.Pod)
				// Only handle Pending pods with our scheduler name and no node assigned
				if pod.Spec.SchedulerName == "ctg-scheduler" && pod.Spec.NodeName == "" && pod.Status.Phase == corev1.PodPending {
					schedulePod(client, pod)
				}
			},
		},
	)

	stop := make(chan struct{})
	defer close(stop)

	controller.Run(stop)
}

// Watch for ContinuousTaskGraph resources
func watchTaskGraphs(dynamicClient dynamic.Interface, client *kubernetes.Clientset, cfg *rest.Config) {
	// Define the ContinuousTaskGraph resource
	ctgGVR := schema.GroupVersionResource{
		Group:    "dsf.example.com",
		Version:  "v1",
		Resource: "continuoustaskgraphs",
	}

	// Watch for ContinuousTaskGraph resources
	watcher, err := dynamicClient.Resource(ctgGVR).Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Printf("Error watching ContinuousTaskGraphs: %v", err)
		return
	}

	for event := range watcher.ResultChan() {
		switch event.Type {
		case "ADDED", "MODIFIED":
			obj := event.Object.(*unstructured.Unstructured)
			log.Printf("ContinuousTaskGraph detected: %s - Deploying tasks", obj.GetName())
			deployTaskGraph(client, obj, cfg)
		case "DELETED":
			obj := event.Object.(*unstructured.Unstructured)
			log.Printf("ContinuousTaskGraph deleted: %s", obj.GetName())
			// Cleanup handled by owner references
		}
	}
}

// Deploy all tasks for a ContinuousTaskGraph
func deployTaskGraph(client *kubernetes.Clientset, obj *unstructured.Unstructured, cfg *rest.Config) {
	graphName := obj.GetName()
	namespace := obj.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}

	// Get spec
	spec, found, _ := unstructured.NestedMap(obj.Object, "spec")
	if !found {
		log.Printf("No spec found for ContinuousTaskGraph %s", graphName)
		return
	}

	// Get tasks
	tasks, found, _ := unstructured.NestedSlice(spec, "tasks")
	if !found {
		log.Printf("No tasks found for ContinuousTaskGraph %s", graphName)
		return
	}

	// Get global ZeroMQ config
	zeromqConfig, _, _ := unstructured.NestedMap(spec, "zeromq")
	schedulerConfig, _, _ := unstructured.NestedMap(spec, "scheduler")
	schedulerName := "ctg-scheduler"
	if schedulerConfig != nil {
		if name, ok := schedulerConfig["name"].(string); ok && name != "" {
			schedulerName = name
		}
	}

	// Get namespace from spec or use default
	specNamespace := namespace
	if ns, ok := spec["namespace"].(string); ok && ns != "" {
		specNamespace = ns
	}

	log.Printf("Deploying %d tasks for ContinuousTaskGraph %s in namespace %s", len(tasks), graphName, specNamespace)

	// Track state
	taskGraphState.mu.Lock()
	if _, exists := taskGraphState.graphs[graphName]; !exists {
		taskGraphState.graphs[graphName] = &GraphState{
			Name:      graphName,
			Namespace: specNamespace,
			Tasks:     make(map[string]bool),
		}
	}
	graphState := taskGraphState.graphs[graphName]
	taskGraphState.mu.Unlock()

	// Deploy each task
	for _, taskObj := range tasks {
		task := taskObj.(map[string]interface{})
		taskName, _ := task["name"].(string)
		if taskName == "" {
			continue
		}

		// Check if pods already exist (don't rely on in-memory state)
		pods, err := client.CoreV1().Pods(specNamespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("ctg-name=%s,ctg-task=%s", graphName, taskName),
		})
		if err == nil && len(pods.Items) > 0 {
			// Pods exist, mark as deployed and skip
			graphState.mu.Lock()
			graphState.Tasks[taskName] = true
			graphState.mu.Unlock()
			continue
		}

		// Get replicas
		replicas := 1
		if r, ok := task["replicas"].(int64); ok {
			replicas = int(r)
		} else if r, ok := task["replicas"].(int); ok {
			replicas = r
		}

		// Get ZeroMQ config for this task
		taskZMQ, _, _ := unstructured.NestedMap(task, "zeromq")

		// Deploy replicas
		for i := 0; i < replicas; i++ {
			podName := fmt.Sprintf("%s-%s-%d", graphName, taskName, i)
			createTaskPod(client, obj, task, graphName, taskName, podName, specNamespace, schedulerName, zeromqConfig, i)
		}

		// Get port for service
		port := int32(5555) // default
		if taskZMQ != nil {
			if p, ok := taskZMQ["port"].(int64); ok {
				port = int32(p)
			}
		} else if zeromqConfig != nil {
			if p, ok := zeromqConfig["defaultPort"].(int64); ok {
				port = int32(p)
			}
		}

		// Create Service for ZeroMQ discovery
		createTaskService(client, graphName, taskName, specNamespace, obj.GetUID(), port)

		// Mark as deployed
		graphState.mu.Lock()
		graphState.Tasks[taskName] = true
		graphState.mu.Unlock()
	}

	log.Printf("Finished deploying tasks for ContinuousTaskGraph %s", graphName)
}

// Create a pod for a task
func createTaskPod(client *kubernetes.Clientset, obj *unstructured.Unstructured, task map[string]interface{},
	graphName, taskName, podName, namespace, schedulerName string, zeromqConfig map[string]interface{}, replicaIndex int) {

	image, _ := task["image"].(string)
	command, _, _ := unstructured.NestedStringSlice(task, "command")
	args, _, _ := unstructured.NestedStringSlice(task, "args")

	// Get resources
	var resources corev1.ResourceRequirements
	if res, ok := task["resources"].(map[string]interface{}); ok {
		resources = parseResources(res)
	}

	// Get ZeroMQ config for this task
	taskZMQ, _, _ := unstructured.NestedMap(task, "zeromq")
	
	// Build environment variables for ZeroMQ
	envVars := buildZeroMQEnvVars(graphName, taskName, zeromqConfig, taskZMQ, namespace, replicaIndex)

	// Build container
	container := corev1.Container{
		Name:      taskName,
		Image:     image,
		Command:   command,
		Args:      args,
		Resources: resources,
		Env:       envVars,
	}

	// Add ZeroMQ port if specified
	if taskZMQ != nil {
		if port, ok := taskZMQ["port"].(int64); ok {
			container.Ports = []corev1.ContainerPort{
				{
					Name:          "zeromq",
					ContainerPort: int32(port),
					Protocol:      corev1.ProtocolTCP,
				},
			}
		}
	}

	// Get constraints
	var nodeSelector map[string]string
	var affinity *corev1.Affinity
	var tolerations []corev1.Toleration

	if constraints, ok := task["constraints"].(map[string]interface{}); ok {
		if ns, ok := constraints["nodeSelector"].(map[string]interface{}); ok {
			nodeSelector = make(map[string]string)
			for k, v := range ns {
				if s, ok := v.(string); ok {
					nodeSelector[k] = s
				}
			}
		}
		// TODO: Parse affinity and tolerations if needed
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				"ctg-name": graphName,
				"ctg-task": taskName,
				"app":      graphName,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "dsf.example.com/v1",
				Kind:       "ContinuousTaskGraph",
				Name:       graphName,
				UID:        obj.GetUID(),
			}},
		},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Containers:    []corev1.Container{container},
			RestartPolicy: corev1.RestartPolicyAlways, // Continuous execution
			NodeSelector:  nodeSelector,
			Affinity:      affinity,
			Tolerations:   tolerations,
		},
	}

	_, err := client.CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error creating pod %s/%s: %v", namespace, podName, err)
		return
	}

	log.Printf("Created pod %s/%s for task %s (replica %d)", namespace, podName, taskName, replicaIndex)
}

// Build ZeroMQ environment variables
func buildZeroMQEnvVars(graphName, taskName string, globalZMQ, taskZMQ map[string]interface{}, namespace string, replicaIndex int) []corev1.EnvVar {
	envVars := []corev1.EnvVar{}

	// Topic prefix
	topicPrefix := graphName
	if globalZMQ != nil {
		if prefix, ok := globalZMQ["topicPrefix"].(string); ok && prefix != "" {
			topicPrefix = prefix
		}
	}

	// Default port
	defaultPort := int64(5555)
	if globalZMQ != nil {
		if port, ok := globalZMQ["defaultPort"].(int64); ok {
			defaultPort = port
		}
	}

	// Task-specific port
	port := defaultPort
	if taskZMQ != nil {
		if p, ok := taskZMQ["port"].(int64); ok {
			port = p
		}
	}

	// Transport
	transport := "tcp"
	if globalZMQ != nil {
		if t, ok := globalZMQ["defaultTransport"].(string); ok && t != "" {
			transport = t
		}
	}
	if taskZMQ != nil {
		if t, ok := taskZMQ["transport"].(string); ok && t != "" {
			transport = t
		}
	}

	// Publish topics
	publishTopics := []string{}
	if taskZMQ != nil {
		if topics, ok := taskZMQ["publishTopics"].([]interface{}); ok {
			for _, t := range topics {
				if topic, ok := t.(string); ok {
					publishTopics = append(publishTopics, topic)
				}
			}
		}
	}

	// Subscribe topics
	subscribeTopics := []string{}
	if taskZMQ != nil {
		if topics, ok := taskZMQ["subscribeTopics"].([]interface{}); ok {
			for _, t := range topics {
				if topic, ok := t.(string); ok {
					subscribeTopics = append(subscribeTopics, topic)
				}
			}
		}
	}

	// Pattern
	pattern := "PUB"
	if taskZMQ != nil {
		if p, ok := taskZMQ["pattern"].(string); ok && p != "" {
			pattern = p
		}
	}

	// Bind
	bind := true
	if taskZMQ != nil {
		if b, ok := taskZMQ["bind"].(bool); ok {
			bind = b
		}
	}

	// Service name for discovery
	serviceName := fmt.Sprintf("%s-%s-service", graphName, taskName)

	// Add environment variables
	envVars = append(envVars,
		corev1.EnvVar{Name: "ZMQ_GRAPH_NAME", Value: graphName},
		corev1.EnvVar{Name: "ZMQ_TASK_NAME", Value: taskName},
		corev1.EnvVar{Name: "ZMQ_TOPIC_PREFIX", Value: topicPrefix},
		corev1.EnvVar{Name: "ZMQ_PORT", Value: fmt.Sprintf("%d", port)},
		corev1.EnvVar{Name: "ZMQ_TRANSPORT", Value: transport},
		corev1.EnvVar{Name: "ZMQ_PATTERN", Value: pattern},
		corev1.EnvVar{Name: "ZMQ_BIND", Value: fmt.Sprintf("%t", bind)},
		corev1.EnvVar{Name: "ZMQ_SERVICE_NAME", Value: serviceName},
		corev1.EnvVar{Name: "ZMQ_NAMESPACE", Value: namespace},
		corev1.EnvVar{Name: "ZMQ_REPLICA_INDEX", Value: fmt.Sprintf("%d", replicaIndex)},
		// High throughput configuration for 100 MB/s
		corev1.EnvVar{Name: "TARGET_DATA_RATE_MBPS", Value: "100"},
		corev1.EnvVar{Name: "RATE", Value: "100"},
	)

	// Publish topics as comma-separated
	if len(publishTopics) > 0 {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "ZMQ_PUBLISH_TOPICS",
			Value: joinStrings(publishTopics, ","),
		})
	}

	// Subscribe topics as comma-separated
	if len(subscribeTopics) > 0 {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "ZMQ_SUBSCRIBE_TOPICS",
			Value: joinStrings(subscribeTopics, ","),
		})
	}

	// Discovery config
	if globalZMQ != nil {
		if discovery, ok := globalZMQ["discovery"].(map[string]interface{}); ok {
			if method, ok := discovery["method"].(string); ok {
				envVars = append(envVars, corev1.EnvVar{Name: "ZMQ_DISCOVERY_METHOD", Value: method})
			}
			if ns, ok := discovery["namespace"].(string); ok {
				envVars = append(envVars, corev1.EnvVar{Name: "ZMQ_DISCOVERY_NAMESPACE", Value: ns})
			}
		}
	}

	return envVars
}

// Create a Service for ZeroMQ discovery
func createTaskService(client *kubernetes.Clientset, graphName, taskName, namespace string, graphUID types.UID, port int32) {
	serviceName := fmt.Sprintf("%s-%s-service", graphName, taskName)

	// Check if service already exists
	existing, err := client.CoreV1().Services(namespace).Get(context.Background(), serviceName, metav1.GetOptions{})
	if err == nil {
		// Service already exists, check if port needs updating
		if len(existing.Spec.Ports) > 0 && existing.Spec.Ports[0].Port != port {
			existing.Spec.Ports[0].Port = port
			existing.Spec.Ports[0].TargetPort = intstr.FromInt32(port)
			_, err = client.CoreV1().Services(namespace).Update(context.Background(), existing, metav1.UpdateOptions{})
			if err != nil {
				log.Printf("Error updating service %s/%s: %v", namespace, serviceName, err)
			} else {
				log.Printf("Updated service %s/%s port to %d", namespace, serviceName, port)
			}
		}
		return
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: namespace,
			Labels: map[string]string{
				"ctg-name": graphName,
				"ctg-task": taskName,
				"app":      graphName,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "dsf.example.com/v1",
				Kind:       "ContinuousTaskGraph",
				Name:       graphName,
				UID:        graphUID,
			}},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"ctg-name": graphName,
				"ctg-task": taskName,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "zeromq",
					Protocol:   corev1.ProtocolTCP,
					Port:       port,
					TargetPort: intstr.FromInt32(port),
				},
			},
		},
	}

	_, err = client.CoreV1().Services(namespace).Create(context.Background(), service, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error creating service %s/%s: %v", namespace, serviceName, err)
	} else {
		log.Printf("Created service %s/%s for task %s on port %d", namespace, serviceName, taskName, port)
	}
}

// Schedule pod to a random node (for now)
func schedulePod(client *kubernetes.Clientset, pod *corev1.Pod) {
	// Get all nodes
	nodes, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{
		FieldSelector: "spec.unschedulable!=true",
	})
	if err != nil {
		log.Printf("Error listing nodes: %v", err)
		return
	}

	if len(nodes.Items) == 0 {
		log.Printf("No available nodes")
		return
	}

	// Random node selection (for now)
	rand.Seed(time.Now().UnixNano())
	selectedNode := nodes.Items[rand.Intn(len(nodes.Items))]

	// Bind pod to the selected node
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Target: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       selectedNode.Name,
		},
	}

	err = client.CoreV1().Pods(pod.Namespace).Bind(context.Background(), binding, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Error binding pod %s/%s to node %s: %v", pod.Namespace, pod.Name, selectedNode.Name, err)
		return
	}
	log.Printf("[SCHEDULER] Bound pod %s/%s to node %s", pod.Namespace, pod.Name, selectedNode.Name)
}

// Parse resources from unstructured map
func parseResources(res map[string]interface{}) corev1.ResourceRequirements {
	req := corev1.ResourceRequirements{
		Requests: make(corev1.ResourceList),
		Limits:   make(corev1.ResourceList),
	}

	if requests, ok := res["requests"].(map[string]interface{}); ok {
		if cpu, ok := requests["cpu"].(string); ok {
			req.Requests[corev1.ResourceCPU] = resource.MustParse(cpu)
		}
		if memory, ok := requests["memory"].(string); ok {
			req.Requests[corev1.ResourceMemory] = resource.MustParse(memory)
		}
	}

	if limits, ok := res["limits"].(map[string]interface{}); ok {
		if cpu, ok := limits["cpu"].(string); ok {
			req.Limits[corev1.ResourceCPU] = resource.MustParse(cpu)
		}
		if memory, ok := limits["memory"].(string); ok {
			req.Limits[corev1.ResourceMemory] = resource.MustParse(memory)
		}
	}

	return req
}

// Helper to join strings
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}
