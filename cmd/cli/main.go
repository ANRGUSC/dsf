package main

// dsf — command-line interface for the DSF framework.
//
// Commands:
//
//	dsf odag submit  -f <file>           Submit an ODAG from a YAML file
//	dsf odag list    [-n <ns>]           List all ODAGs
//	dsf odag status  <name> [-n <ns>]    Show detailed status of an ODAG
//	dsf odag delete  <name> [-n <ns>]    Delete an ODAG and its resources
//	dsf odag logs    <name> <task> [-n <ns>]  Stream logs from a task pod
//	dsf odag run     <template> [-n <ns>]     Create a new run from a template
//	dsf odag runs    <template> [-n <ns>]     List all runs of a template
//
//	dsf cdag submit  -f <file>           Submit a CDAG from a YAML file
//	dsf cdag list    [-n <ns>]           List all CDAGs
//	dsf cdag status  <name> [-n <ns>]    Show detailed status of a CDAG
//	dsf cdag delete  <name> [-n <ns>]    Delete a CDAG and its resources
//	dsf cdag logs    <name> <task> [-n <ns>]  Stream logs from a task pod
//
//	dsf template apply   -f <file>       Register an ODAGTemplate
//	dsf template list    [-n <ns>]       List ODAGTemplates
//	dsf template show    <name> [-n <ns>]  Show template detail + profile summary
//	dsf template delete  <name> [-n <ns>]  Delete an ODAGTemplate

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	odagGVR         = schema.GroupVersionResource{Group: "dsf.io", Version: "v1", Resource: "odags"}
	cdagGVR         = schema.GroupVersionResource{Group: "dsf.io", Version: "v1", Resource: "cdags"}
	odagTemplateGVR = schema.GroupVersionResource{Group: "dsf.io", Version: "v1", Resource: "odagtemplates"}
)

// globals set by persistent flags
var (
	kubeconfig string
	namespace  string
	filename   string
)

func main() {
	root := &cobra.Command{
		Use:   "dsf",
		Short: "DSF — DAG Scheduling Framework CLI",
		Long: `DSF (DAG Scheduling Framework) schedules and runs task graphs on Kubernetes.

Two kinds of DAG are supported:

  odag  One-shot DAG   — runs to completion; tasks execute once and the DAG
                         transitions to Succeeded or Failed.

  cdag  Continuous DAG — runs indefinitely; tasks are long-lived pods that
                         restart automatically if they crash.

Both kinds use HEFT scheduling to assign tasks to cluster nodes and ZMQ for
inter-task communication. Specs are applied as Kubernetes custom resources
(dsf.io/v1) and managed by the odag-controller / cdag-controller.`,
	}
	root.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", defaultKubeconfig(), "path to kubeconfig")

	root.AddCommand(odagCmd(), cdagCmd(), templateCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// ─── ODAG commands ────────────────────────────────────────────────────────────

func odagCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "odag",
		Short: "Manage one-shot DAGs",
		Long: `Manage ODAGs (One-shot DAGs).

An ODAG runs its task graph once: all pods start simultaneously, upstream tasks
push results to downstream tasks over ZMQ, and the DAG transitions to Succeeded
or Failed when all tasks finish.`,
	}

	submit := &cobra.Command{
		Use:   "submit -f <file>",
		Short: "Submit an ODAG from a YAML file",
		Long:  "Create an ODAG custom resource from a YAML spec file.",
		Example: `  dsf odag submit -f examples/dag-pipeline/odag.yml
  dsf odag submit -f my-dag.yml`,
		RunE: submitResource(odagGVR),
	}
	submit.Flags().StringVarP(&filename, "file", "f", "", "path to ODAG YAML (required)")
	submit.MarkFlagRequired("file")

	list := &cobra.Command{
		Use:   "list",
		Short: "List ODAGs",
		Long:  "List all ODAGs in a namespace, showing name, phase, and age.",
		Example: `  dsf odag list
  dsf odag list -n production`,
		RunE: listResources(odagGVR),
	}
	list.Flags().StringVarP(&namespace, "namespace", "n", "default", "namespace")

	status := &cobra.Command{
		Use:   "status <name>",
		Short: "Show ODAG status",
		Long:  "Show detailed status of an ODAG including per-task phase, assigned node, and timing.",
		Example: `  dsf odag status dag-pipeline
  dsf odag status dag-pipeline -n production`,
		Args: cobra.ExactArgs(1),
		RunE: odagStatus,
	}
	status.Flags().StringVarP(&namespace, "namespace", "n", "default", "namespace")

	del := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an ODAG and its task resources",
		Long:  "Delete an ODAG custom resource along with all associated pods and services.",
		Example: `  dsf odag delete dag-pipeline
  dsf odag delete dag-pipeline -n production`,
		Args: cobra.ExactArgs(1),
		RunE: deleteResource(odagGVR),
	}
	del.Flags().StringVarP(&namespace, "namespace", "n", "default", "namespace")

	logs := &cobra.Command{
		Use:   "logs <odag-name> <task-name>",
		Short: "Stream logs from an ODAG task pod",
		Long:  "Stream stdout/stderr from the pod running the specified task.",
		Example: `  dsf odag logs dag-pipeline generate
  dsf odag logs dag-pipeline transform -n production`,
		Args: cobra.ExactArgs(2),
		RunE: streamLogs("dsf-odag"),
	}
	logs.Flags().StringVarP(&namespace, "namespace", "n", "default", "namespace")

	run := &cobra.Command{
		Use:   "run <template-name>",
		Short: "Create a new run from an ODAGTemplate",
		Long:  "Fetches the named ODAGTemplate and creates a new ODAG run with an auto-incremented ID.",
		Example: `  dsf odag run dag-pipeline
  dsf odag run dag-pipeline -n dsf-system`,
		Args: cobra.ExactArgs(1),
		RunE: odagRunFromTemplate,
	}
	run.Flags().StringVarP(&namespace, "namespace", "n", "dsf-system", "namespace")

	runs := &cobra.Command{
		Use:   "runs <template-name>",
		Short: "List all runs of an ODAGTemplate",
		Long:  "List all ODAG runs created from a template, showing run number, phase, makespan, and age.",
		Example: `  dsf odag runs dag-pipeline
  dsf odag runs dag-pipeline -n dsf-system`,
		Args: cobra.ExactArgs(1),
		RunE: odagListRuns,
	}
	runs.Flags().StringVarP(&namespace, "namespace", "n", "dsf-system", "namespace")

	cmd.AddCommand(submit, list, status, del, logs, run, runs)
	return cmd
}

// ─── CDAG commands ─────────────────────────────────────────────────────────────

func cdagCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cdag",
		Short: "Manage continuous DAGs",
		Long: `Manage CDAGs (Continuous DAGs).

A CDAG runs its task graph indefinitely: tasks are long-lived pods that
communicate over ZMQ pub/sub. The controller reconciles every 30 seconds,
recreating any crashed pods automatically.`,
	}

	submit := &cobra.Command{
		Use:   "submit -f <file>",
		Short: "Submit a CDAG from a YAML file",
		Long:  "Create a CDAG custom resource from a YAML spec file.",
		Example: `  dsf cdag submit -f examples/pipeline-ctg/cdag.yml
  dsf cdag submit -f my-pipeline.yml`,
		RunE: submitResource(cdagGVR),
	}
	submit.Flags().StringVarP(&filename, "file", "f", "", "path to CDAG YAML (required)")
	submit.MarkFlagRequired("file")

	list := &cobra.Command{
		Use:   "list",
		Short: "List CDAGs",
		Long:  "List all CDAGs in a namespace, showing name, phase, and age.",
		Example: `  dsf cdag list
  dsf cdag list -n production`,
		RunE: listResources(cdagGVR),
	}
	list.Flags().StringVarP(&namespace, "namespace", "n", "default", "namespace")

	status := &cobra.Command{
		Use:   "status <name>",
		Short: "Show CDAG status",
		Long:  "Show detailed status of a CDAG including per-task replica counts and assigned nodes.",
		Example: `  dsf cdag status pipeline-ctg
  dsf cdag status pipeline-ctg -n production`,
		Args: cobra.ExactArgs(1),
		RunE: cdagStatus,
	}
	status.Flags().StringVarP(&namespace, "namespace", "n", "default", "namespace")

	del := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a CDAG and its task resources",
		Long:  "Delete a CDAG custom resource along with all associated pods and services.",
		Example: `  dsf cdag delete pipeline-ctg
  dsf cdag delete pipeline-ctg -n production`,
		Args: cobra.ExactArgs(1),
		RunE: deleteResource(cdagGVR),
	}
	del.Flags().StringVarP(&namespace, "namespace", "n", "default", "namespace")

	logs := &cobra.Command{
		Use:   "logs <cdag-name> <task-name>",
		Short: "Stream logs from a CDAG task pod",
		Long:  "Stream stdout/stderr from the pod running the specified task.",
		Example: `  dsf cdag logs pipeline-ctg producer
  dsf cdag logs pipeline-ctg processor -n production`,
		Args: cobra.ExactArgs(2),
		RunE: streamLogs("dsf-cdag"),
	}
	logs.Flags().StringVarP(&namespace, "namespace", "n", "default", "namespace")

	cmd.AddCommand(submit, list, status, del, logs)
	return cmd
}

// ─── Implementations ─────────────────────────────────────────────────────────

func submitResource(gvr schema.GroupVersionResource) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("reading %s: %w", filename, err)
		}
		dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
		obj := &unstructured.Unstructured{}
		if _, _, err = dec.Decode(data, nil, obj); err != nil {
			return fmt.Errorf("parsing YAML: %w", err)
		}
		ns := obj.GetNamespace()
		if ns == "" {
			ns = "default"
		}
		dc, err := dynClient()
		if err != nil {
			return err
		}
		result, err := dc.Resource(gvr).Namespace(ns).Create(context.Background(), obj, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create: %w", err)
		}
		fmt.Printf("%s/%s submitted\n", gvr.Resource, result.GetName())
		return nil
	}
}

func listResources(gvr schema.GroupVersionResource) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		dc, err := dynClient()
		if err != nil {
			return err
		}
		list, err := dc.Resource(gvr).Namespace(namespace).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "NAME\tNAMESPACE\tPHASE\tAGE")
		for _, item := range list.Items {
			phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
			if phase == "" {
				phase = "Pending"
			}
			age := fmtAge(item.GetCreationTimestamp().Time)
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", item.GetName(), item.GetNamespace(), phase, age)
		}
		return w.Flush()
	}
}

func odagStatus(cmd *cobra.Command, args []string) error {
	name := args[0]
	dc, err := dynClient()
	if err != nil {
		return err
	}
	obj, err := dc.Resource(odagGVR).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	makespan, _, _ := unstructured.NestedFloat64(obj.Object, "status", "makespan")
	startTime, _, _ := unstructured.NestedString(obj.Object, "status", "startTime")
	completionTime, _, _ := unstructured.NestedString(obj.Object, "status", "completionTime")
	message, _, _ := unstructured.NestedString(obj.Object, "status", "message")

	fmt.Printf("Name:        %s\n", name)
	fmt.Printf("Namespace:   %s\n", namespace)
	fmt.Printf("Phase:       %s\n", phase)
	if makespan > 0 {
		fmt.Printf("Makespan:    %.1fs\n", makespan)
	}
	if startTime != "" {
		fmt.Printf("Start:       %s\n", startTime)
	}
	if completionTime != "" {
		fmt.Printf("Completion:  %s\n", completionTime)
	}
	if message != "" {
		fmt.Printf("Message:     %s\n", message)
	}

	// Print per-task status if available
	tasks, found, _ := unstructured.NestedSlice(obj.Object, "status", "tasks")
	if found && len(tasks) > 0 {
		fmt.Println("\nTasks:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "  NAME\tPHASE\tNODE")
		for _, t := range tasks {
			tm, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			tname, _ := tm["name"].(string)
			tphase, _ := tm["phase"].(string)
			tnode, _ := tm["node"].(string)
			fmt.Fprintf(w, "  %s\t%s\t%s\n", tname, tphase, tnode)
		}
		w.Flush()
	}

	// Show spec tasks from spec
	specTasks, found, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	if found {
		fmt.Printf("\nSpec tasks (%d):\n", len(specTasks))
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "  NAME\tIMAGE\tDEPS")
		for _, t := range specTasks {
			tm, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			tname, _ := tm["name"].(string)
			timage, _ := tm["image"].(string)
			deps, _ := tm["dependencies"].([]interface{})
			depNames := ""
			for _, d := range deps {
				if depNames != "" {
					depNames += ","
				}
				depNames += fmt.Sprint(d)
			}
			if depNames == "" {
				depNames = "-"
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\n", tname, timage, depNames)
		}
		w.Flush()
	}
	return nil
}

func cdagStatus(cmd *cobra.Command, args []string) error {
	name := args[0]
	dc, err := dynClient()
	if err != nil {
		return err
	}
	obj, err := dc.Resource(cdagGVR).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	message, _, _ := unstructured.NestedString(obj.Object, "status", "message")

	fmt.Printf("Name:        %s\n", name)
	fmt.Printf("Namespace:   %s\n", namespace)
	fmt.Printf("Phase:       %s\n", phase)
	if message != "" {
		fmt.Printf("Message:     %s\n", message)
	}

	tasks, found, _ := unstructured.NestedSlice(obj.Object, "status", "tasks")
	if found && len(tasks) > 0 {
		fmt.Println("\nTasks:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "  NAME\tDESIRED\tREADY\tPODS")
		for _, t := range tasks {
			tm, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			tname, _ := tm["name"].(string)
			desired, _ := tm["desiredReplicas"].(int64)
			ready, _ := tm["readyReplicas"].(int64)
			pods, _ := tm["podNames"].([]interface{})
			podList := ""
			for _, p := range pods {
				if podList != "" {
					podList += ","
				}
				podList += fmt.Sprint(p)
			}
			fmt.Fprintf(w, "  %s\t%d\t%d\t%s\n", tname, desired, ready, podList)
		}
		w.Flush()
	}

	specTasks, found, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	if found {
		fmt.Printf("\nSpec tasks (%d):\n", len(specTasks))
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "  NAME\tIMAGE\tREPLICAS\tDEPS")
		for _, t := range specTasks {
			tm, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			tname, _ := tm["name"].(string)
			timage, _ := tm["image"].(string)
			replicas, _ := tm["replicas"].(int64)
			if replicas == 0 {
				replicas = 1
			}
			deps, _ := tm["dependencies"].([]interface{})
			depNames := ""
			for _, d := range deps {
				if depNames != "" {
					depNames += ","
				}
				depNames += fmt.Sprint(d)
			}
			if depNames == "" {
				depNames = "-"
			}
			fmt.Fprintf(w, "  %s\t%s\t%d\t%s\n", tname, timage, replicas, depNames)
		}
		w.Flush()
	}
	return nil
}

func deleteResource(gvr schema.GroupVersionResource) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		name := args[0]
		dc, err := dynClient()
		if err != nil {
			return err
		}
		if err = dc.Resource(gvr).Namespace(namespace).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("delete %s %s: %w", gvr.Resource, name, err)
		}

		// Also delete associated pods and services
		kc, err := k8sClient()
		if err != nil {
			return err
		}
		labelKey := "dsf-odag"
		if gvr == cdagGVR {
			labelKey = "dsf-cdag"
		}
		sel := fmt.Sprintf("%s=%s", labelKey, name)
		_ = kc.CoreV1().Pods(namespace).DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: sel})
		svcs, _ := kc.CoreV1().Services(namespace).List(context.Background(), metav1.ListOptions{LabelSelector: sel})
		for _, svc := range svcs.Items {
			_ = kc.CoreV1().Services(namespace).Delete(context.Background(), svc.Name, metav1.DeleteOptions{})
		}

		fmt.Printf("%s/%s deleted\n", gvr.Resource, name)
		return nil
	}
}

func streamLogs(labelKey string) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		dagName, taskName := args[0], args[1]
		kc, err := k8sClient()
		if err != nil {
			return err
		}

		// Find pod: name is {dag-name}-{task-name} or use label selector
		podName := fmt.Sprintf("%s-%s", dagName, taskName)
		pods, err := kc.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s,dsf-task=%s", labelKey, dagName, taskName),
		})
		if err == nil && len(pods.Items) > 0 {
			podName = pods.Items[0].Name
		}

		req := kc.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{Follow: true})
		stream, err := req.Stream(context.Background())
		if err != nil {
			return fmt.Errorf("logs for %s: %w", podName, err)
		}
		defer stream.Close()

		scanner := bufio.NewScanner(stream)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
		return scanner.Err()
	}
}

// ─── Template commands ────────────────────────────────────────────────────────

func templateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "template",
		Short: "Manage ODAG templates",
		Long: `Manage ODAGTemplates — reusable DAG definitions with profiling.

An ODAGTemplate defines a DAG structure without executing it. Use "dsf odag run"
to create runs from a template. The profiler records actual runtimes per
(task, node) pair after each run, improving HEFT scheduling over time.`,
	}

	apply := &cobra.Command{
		Use:   "apply -f <file>",
		Short: "Register an ODAGTemplate from a YAML file",
		Long:  "Create or update an ODAGTemplate custom resource from a YAML spec file.",
		Example: `  dsf template apply -f examples/dag-pipeline/template.yml
  dsf template apply -f my-template.yml`,
		RunE: submitResource(odagTemplateGVR),
	}
	apply.Flags().StringVarP(&filename, "file", "f", "", "path to ODAGTemplate YAML (required)")
	apply.MarkFlagRequired("file")

	list := &cobra.Command{
		Use:   "list",
		Short: "List ODAGTemplates",
		Long:  "List all ODAGTemplates in a namespace.",
		Example: `  dsf template list
  dsf template list -n dsf-system`,
		RunE: templateList,
	}
	list.Flags().StringVarP(&namespace, "namespace", "n", "dsf-system", "namespace")

	show := &cobra.Command{
		Use:   "show <name>",
		Short: "Show ODAGTemplate detail",
		Long:  "Show detailed information about an ODAGTemplate including profiling config, tasks, and profile summary.",
		Example: `  dsf template show dag-pipeline
  dsf template show dag-pipeline -n dsf-system`,
		Args: cobra.ExactArgs(1),
		RunE: templateShow,
	}
	show.Flags().StringVarP(&namespace, "namespace", "n", "dsf-system", "namespace")

	del := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an ODAGTemplate",
		Long:  "Delete an ODAGTemplate custom resource.",
		Example: `  dsf template delete dag-pipeline
  dsf template delete dag-pipeline -n dsf-system`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			dc, err := dynClient()
			if err != nil {
				return err
			}
			if err = dc.Resource(odagTemplateGVR).Namespace(namespace).Delete(
				context.Background(), name, metav1.DeleteOptions{}); err != nil {
				return fmt.Errorf("delete template %s: %w", name, err)
			}
			fmt.Printf("template/%s deleted\n", name)
			return nil
		},
	}
	del.Flags().StringVarP(&namespace, "namespace", "n", "dsf-system", "namespace")

	cmd.AddCommand(apply, list, show, del)
	return cmd
}

func templateList(cmd *cobra.Command, args []string) error {
	dc, err := dynClient()
	if err != nil {
		return err
	}
	list, err := dc.Resource(odagTemplateGVR).Namespace(namespace).List(
		context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tSCHEDULER\tTASKS\tRUNS\tLAST MAKESPAN\tPROFILING\tAGE")
	for _, item := range list.Items {
		scheduler, _, _ := unstructured.NestedString(item.Object, "spec", "scheduler")
		tasks, _, _ := unstructured.NestedSlice(item.Object, "spec", "tasks")
		runCount, _, _ := unstructured.NestedInt64(item.Object, "status", "runCount")
		makespan, _, _ := unstructured.NestedFloat64(item.Object, "status", "lastRunMakespan")
		profiling, _, _ := unstructured.NestedBool(item.Object, "spec", "profiling", "enabled")
		age := fmtAge(item.GetCreationTimestamp().Time)

		makespanStr := "-"
		if makespan > 0 {
			makespanStr = fmt.Sprintf("%.1fs", makespan)
		}
		profilingStr := "yes"
		// If explicitly set to false
		if p, ok, _ := unstructured.NestedBool(item.Object, "spec", "profiling", "enabled"); ok && !p {
			profilingStr = "no"
		}
		_ = profiling

		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\t%s\n",
			item.GetName(), scheduler, len(tasks), runCount, makespanStr, profilingStr, age)
	}
	return w.Flush()
}

func templateShow(cmd *cobra.Command, args []string) error {
	name := args[0]
	dc, err := dynClient()
	if err != nil {
		return err
	}
	obj, err := dc.Resource(odagTemplateGVR).Namespace(namespace).Get(
		context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	scheduler, _, _ := unstructured.NestedString(obj.Object, "spec", "scheduler")
	desc, _, _ := unstructured.NestedString(obj.Object, "spec", "description")
	runCount, _, _ := unstructured.NestedInt64(obj.Object, "status", "runCount")
	lastRunName, _, _ := unstructured.NestedString(obj.Object, "status", "lastRunName")
	lastRunPhase, _, _ := unstructured.NestedString(obj.Object, "status", "lastRunPhase")
	lastMakespan, _, _ := unstructured.NestedFloat64(obj.Object, "status", "lastRunMakespan")

	fmt.Printf("Name:         %s\n", name)
	fmt.Printf("Namespace:    %s\n", namespace)
	if desc != "" {
		fmt.Printf("Description:  %s\n", desc)
	}
	fmt.Printf("Scheduler:    %s\n", scheduler)
	fmt.Printf("Runs:         %d\n", runCount)
	if lastRunName != "" {
		fmt.Printf("Last Run:     %s (%s, %.1fs)\n", lastRunName, lastRunPhase, lastMakespan)
	}

	// Profiling config
	fmt.Println("\nProfiling:")
	enabled := true
	if v, ok, _ := unstructured.NestedBool(obj.Object, "spec", "profiling", "enabled"); ok {
		enabled = v
	}
	fmt.Printf("  Enabled:      %v\n", enabled)
	if warmup, ok, _ := unstructured.NestedInt64(obj.Object, "spec", "profiling", "warmupRuns"); ok {
		fmt.Printf("  Warmup Runs:  %d\n", warmup)
	}
	if minS, ok, _ := unstructured.NestedInt64(obj.Object, "spec", "profiling", "minSamples"); ok {
		fmt.Printf("  Min Samples:  %d\n", minS)
	}
	if alpha, ok, _ := unstructured.NestedFloat64(obj.Object, "spec", "profiling", "emaAlpha"); ok {
		fmt.Printf("  EMA Alpha:    %.2f\n", alpha)
	}
	if maxS, ok, _ := unstructured.NestedInt64(obj.Object, "spec", "profiling", "maxSamples"); ok {
		fmt.Printf("  Max Samples:  %d\n", maxS)
	}

	// Tasks
	specTasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	fmt.Printf("\nTasks (%d):\n", len(specTasks))
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "  NAME\tIMAGE\tRUNTIME\tDATA SIZE\tDEPS\tCONSTRAINTS")
	for _, t := range specTasks {
		tm, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		tname, _ := tm["name"].(string)
		timage, _ := tm["image"].(string)

		runtimeStr := "-"
		if rt, ok := tm["runtime"].(int64); ok {
			runtimeStr = fmt.Sprintf("%ds", rt)
		} else if rt, ok := tm["runtime"].(float64); ok {
			runtimeStr = fmt.Sprintf("%.0fs", rt)
		}

		dataSize, _ := tm["dataSize"].(string)
		if dataSize == "" {
			dataSize = "-"
		}

		deps, _ := tm["dependencies"].([]interface{})
		depStr := "-"
		if len(deps) > 0 {
			names := make([]string, len(deps))
			for i, d := range deps {
				names[i] = fmt.Sprint(d)
			}
			depStr = fmt.Sprintf("%v", names)
		}

		constraints, _, _ := unstructured.NestedStringSlice(tm, "constraints", "nodeNames")
		constraintStr := "-"
		if len(constraints) > 0 {
			constraintStr = fmt.Sprintf("%v", constraints)
		}

		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\t%s\n",
			tname, timage, runtimeStr, dataSize, depStr, constraintStr)
	}
	w.Flush()

	// Profile summary
	profileSummary, ok, _ := unstructured.NestedMap(obj.Object, "status", "profileSummary")
	if ok && len(profileSummary) > 0 {
		fmt.Println("\nProfile Summary (task → node → runtime):")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "  TASK\tNODE\tRUNTIME (EMA)")
		for task, nodeMap := range profileSummary {
			nm, ok := nodeMap.(map[string]interface{})
			if !ok {
				continue
			}
			for node, runtime := range nm {
				fmt.Fprintf(w, "  %s\t%s\t%.2fs\n", task, node, toFloat64(runtime))
			}
		}
		w.Flush()
	}

	return nil
}

// ─── ODAG run from template ──────────────────────────────────────────────────

func odagRunFromTemplate(cmd *cobra.Command, args []string) error {
	templateName := args[0]
	dc, err := dynClient()
	if err != nil {
		return err
	}

	// Fetch the template.
	tmpl, err := dc.Resource(odagTemplateGVR).Namespace(namespace).Get(
		context.Background(), templateName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get template %s: %w", templateName, err)
	}

	// Find the highest existing run number to determine the next one.
	existing, err := dc.Resource(odagGVR).Namespace(namespace).List(
		context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("dsf.io/template=%s", templateName),
		})
	if err != nil {
		return fmt.Errorf("list runs: %w", err)
	}
	maxRun := 0
	for _, item := range existing.Items {
		labels := item.GetLabels()
		if n, err := fmt.Sscanf(labels["dsf.io/run"], "%d", new(int)); err == nil && n > 0 {
			v := 0
			fmt.Sscanf(labels["dsf.io/run"], "%d", &v)
			if v > maxRun {
				maxRun = v
			}
		}
	}
	runNum := maxRun + 1

	odagName := fmt.Sprintf("%s-run-%03d", templateName, runNum)

	// Extract spec from template, stripping template-only fields.
	spec, _, err := unstructured.NestedMap(tmpl.Object, "spec")
	if err != nil {
		return fmt.Errorf("extract spec: %w", err)
	}
	delete(spec, "profiling")
	delete(spec, "defaults")
	delete(spec, "retention")
	delete(spec, "description")

	// Create ODAG CR.
	odag := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "dsf.io/v1",
			"kind":       "ODAG",
			"metadata": map[string]interface{}{
				"name":      odagName,
				"namespace": namespace,
				"labels": map[string]interface{}{
					"dsf.io/template": templateName,
					"dsf.io/run":      fmt.Sprintf("%d", runNum),
				},
			},
			"spec": spec,
		},
	}

	if _, err := dc.Resource(odagGVR).Namespace(namespace).Create(
		context.Background(), odag, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create run: %w", err)
	}

	fmt.Printf("Created run %s (run #%d from template %s)\n", odagName, runNum, templateName)
	return nil
}

func odagListRuns(cmd *cobra.Command, args []string) error {
	templateName := args[0]
	dc, err := dynClient()
	if err != nil {
		return err
	}

	list, err := dc.Resource(odagGVR).Namespace(namespace).List(
		context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("dsf.io/template=%s", templateName),
		})
	if err != nil {
		return err
	}

	if len(list.Items) == 0 {
		fmt.Printf("No runs found for template %s in namespace %s\n", templateName, namespace)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tRUN\tPHASE\tMAKESPAN\tAGE")
	for _, item := range list.Items {
		labels := item.GetLabels()
		runNum := labels["dsf.io/run"]
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		if phase == "" {
			phase = "Pending"
		}
		makespan, _, _ := unstructured.NestedFloat64(item.Object, "status", "makespan")
		makespanStr := "-"
		if makespan > 0 {
			makespanStr = fmt.Sprintf("%.1fs", makespan)
		}
		age := fmtAge(item.GetCreationTimestamp().Time)
		fmt.Fprintf(w, "%s\t#%s\t%s\t%s\t%s\n", item.GetName(), runNum, phase, makespanStr, age)
	}
	return w.Flush()
}

// toFloat64 converts an interface{} (int64, float64, json.Number) to float64.
func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		return 0
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func dynClient() (dynamic.Interface, error) {
	cfg, err := buildConfig()
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(cfg)
}

func k8sClient() (*kubernetes.Clientset, error) {
	cfg, err := buildConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func buildConfig() (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	return clientcmd.BuildConfigFromFlags("", defaultKubeconfig())
}

func defaultKubeconfig() string {
	if kc := os.Getenv("KUBECONFIG"); kc != "" {
		return kc
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".kube", "config")
}

func fmtAge(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// avoid unused import — json is used for potential future marshalling
var _ = json.Marshal
var _ = io.EOF
