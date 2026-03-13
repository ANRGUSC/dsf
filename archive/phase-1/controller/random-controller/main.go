package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// DataAgentPort is the port where the Data Agent DaemonSet listens
	DataAgentPort = 8080
	// DataOutputPath is where pods write their output data (hostPath)
	DataOutputPath = "/data/dag-outputs"
)

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

// Watch for DAG resources
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
		case "ADDED", "MODIFIED":
			obj := event.Object.(*unstructured.Unstructured)
			log.Printf("DAG event [%s]: %s", event.Type, obj.GetName())
			processDAG(client, obj, cfg)
		}
	}
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
	log.Printf("Processing DAG %s: %d steps, %d existing pods", dagName, len(steps), len(pods.Items))

	for _, stepObj := range steps {
		step := stepObj.(map[string]interface{})
		stepName, _ := step["name"].(string)
		dependencies, _, _ := unstructured.NestedStringSlice(step, "dependencies")

		if isStepReady(stepName, dependencies, pods.Items) {
			log.Printf("Step %s is ready, creating pod", stepName)
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
			// Only include if main container completed successfully
			if isMainContainerCompleted(&pod) {
				stepNodes[stepName] = pod.Spec.NodeName
			}
		}
	}
	return stepNodes
}

// Check if a step is ready to run
func isStepReady(stepName string, dependencies []string, pods []corev1.Pod) bool {
	// Check if pod already exists
	for _, pod := range pods {
		if pod.Labels["dag-step"] == stepName {
			return false
		}
	}

	// Check all dependencies are completed
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

// Create a pod for a DAG step
func createStepPod(client *kubernetes.Clientset, dag *unstructured.Unstructured, step map[string]interface{}, namespace string, stepNodes map[string]string) {
	dagName := dag.GetName()
	stepName, _ := step["name"].(string)
	image, _ := step["image"].(string)
	args, _, _ := unstructured.NestedStringSlice(step, "args")
	dependencies, _, _ := unstructured.NestedStringSlice(step, "dependencies")
	dataSize, _, _ := unstructured.NestedString(step, "dataSize")

	podName := fmt.Sprintf("%s-%s", dagName, stepName)

	// Get scheduler name
	schedulerName, _, _ := unstructured.NestedString(dag.Object, "spec", "schedulerName")
	if schedulerName == "" {
		schedulerName = "default-scheduler"
	}

	// Get node constraints
	nodeNames, _, _ := unstructured.NestedStringSlice(step, "constraints", "nodeNames")

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

	// Add node affinity if constraints specified
	if len(nodeNames) > 0 {
		pod.Spec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: corev1.NodeSelectorOpIn,
									Values:   nodeNames,
								},
							},
						},
					},
				},
			},
		}
		log.Printf("Step %s: node affinity set to %v", stepName, nodeNames)
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
		log.Printf("Created pod %s (deps: %v)", podName, depNodesList)
	} else {
		log.Printf("Created pod %s (no dependencies)", podName)
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

	// Add env vars for each dependency's data location
	for _, dep := range dependencies {
		if node, ok := depNodes[dep]; ok {
			// DEP_<NAME>_NODE - the node where dependency ran
			envVars = append(envVars, corev1.EnvVar{
				Name:  fmt.Sprintf("DEP_%s_NODE", strings.ToUpper(dep)),
				Value: node,
			})
			// DEP_<NAME>_URL - URL to fetch data from Data Agent
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
		// Default: create small output file
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
