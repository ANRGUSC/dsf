package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// CRD reference for dsf.io/v1/CDAGTemplate
var cdagTemplateGVR = schema.GroupVersionResource{
	Group:    "dsf.io",
	Version:  "v1",
	Resource: "cdagtemplates",
}

// cdagTemplateCache stores the latest CDAGTemplate objects, keyed by "ns/name".
var cdagTemplateCache sync.Map

// --------------------------------------------------------------------------
// Template watcher
// --------------------------------------------------------------------------

// watchCDAGTemplates watches CDAGTemplate CRs and caches them in memory.
func watchCDAGTemplates(dynClient dynamic.Interface) {
	for {
		watcher, err := dynClient.Resource(cdagTemplateGVR).Namespace("").Watch(
			context.Background(), metav1.ListOptions{},
		)
		if err != nil {
			log.Printf("[cdag-template] error watching CDAGTemplates: %v; retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("[cdag-template] watching CDAGTemplate resources")
		for event := range watcher.ResultChan() {
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			key := obj.GetNamespace() + "/" + obj.GetName()
			switch string(event.Type) {
			case "ADDED", "MODIFIED":
				cdagTemplateCache.Store(key, obj.DeepCopy())
				log.Printf("[cdag-template] cached template %s", key)
			case "DELETED":
				cdagTemplateCache.Delete(key)
				log.Printf("[cdag-template] removed template %s", key)
			}
		}
		log.Println("[cdag-template] CDAGTemplate watcher closed; reconnecting in 2s")
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// Create an instance from a template
// --------------------------------------------------------------------------

// createInstanceFromTemplate creates a new CDAG CR from a CDAGTemplate.
// It auto-increments the instance number and names the CDAG "<template>-inst-NNN".
func createInstanceFromTemplate(dynClient dynamic.Interface,
	templateObj *unstructured.Unstructured) (string, error) {

	templateName := templateObj.GetName()
	namespace := templateObj.GetNamespace()

	// Find the max existing instance number by scanning CDAGs with this template label.
	instNum, err := nextInstanceNumber(dynClient, namespace, templateName)
	if err != nil {
		return "", fmt.Errorf("get next instance number: %w", err)
	}
	cdagName := fmt.Sprintf("%s-inst-%03d", templateName, instNum)

	// Extract spec from template, stripping template-only fields.
	spec, _, err := unstructured.NestedMap(templateObj.Object, "spec")
	if err != nil {
		return "", fmt.Errorf("extract template spec: %w", err)
	}
	delete(spec, "description")
	delete(spec, "retention")

	// Build the CDAG CR.
	cdag := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "dsf.io/v1",
			"kind":       "CDAG",
			"metadata": map[string]interface{}{
				"name":      cdagName,
				"namespace": namespace,
				"labels": map[string]interface{}{
					"dsf.io/cdag-template": templateName,
					"dsf.io/instance":      fmt.Sprintf("%d", instNum),
				},
			},
			"spec": spec,
		},
	}

	if _, err := dynClient.Resource(cdagGVR).Namespace(namespace).Create(
		context.Background(), cdag, metav1.CreateOptions{},
	); err != nil {
		return "", fmt.Errorf("create CDAG instance: %w", err)
	}

	log.Printf("[cdag-template] created instance %s/%s (instance #%d) from template %s",
		namespace, cdagName, instNum, templateName)
	return cdagName, nil
}

// nextInstanceNumber scans existing CDAG instances for a template and returns max+1.
func nextInstanceNumber(dynClient dynamic.Interface, namespace, templateName string) (int, error) {
	list, err := dynClient.Resource(cdagGVR).Namespace(namespace).List(
		context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("dsf.io/cdag-template=%s", templateName),
		},
	)
	if err != nil {
		return 1, err
	}

	maxNum := 0
	for _, item := range list.Items {
		labels := item.GetLabels()
		s := labels["dsf.io/instance"]
		if s == "" {
			continue
		}
		var n int
		fmt.Sscanf(s, "%d", &n)
		if n > maxNum {
			maxNum = n
		}
	}
	return maxNum + 1, nil
}

// --------------------------------------------------------------------------
// Retention config extraction
// --------------------------------------------------------------------------

// extractRetentionMaxInstances reads spec.retention.maxInstances from a template.
func extractRetentionMaxInstances(templateObj *unstructured.Unstructured) int {
	if templateObj == nil {
		return 20
	}
	v, ok, _ := unstructured.NestedInt64(templateObj.Object, "spec", "retention", "maxInstances")
	if !ok {
		return 20
	}
	return int(v)
}

// --------------------------------------------------------------------------
// Template status updates
// --------------------------------------------------------------------------

// updateCDAGTemplateStatus updates the CDAGTemplate's status with the latest instance info.
func updateCDAGTemplateStatus(dynClient dynamic.Interface, namespace, templateName, instanceName, phase string) {
	tmpl, err := dynClient.Resource(cdagTemplateGVR).Namespace(namespace).Get(
		context.Background(), templateName, metav1.GetOptions{},
	)
	if err != nil {
		log.Printf("[cdag-template] failed to get template %s for status update: %v", templateName, err)
		return
	}

	instanceCount, _, _ := unstructured.NestedInt64(tmpl.Object, "status", "instanceCount")

	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"instanceCount":     instanceCount + 1,
			"lastInstanceName":  instanceName,
			"lastInstancePhase": phase,
		},
	}

	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(cdagTemplateGVR).Namespace(namespace).Patch(
		context.Background(), templateName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

// updateCDAGTemplatePhase updates just the lastInstancePhase on the template status,
// without incrementing the instance count. Called during reconciliation.
func updateCDAGTemplatePhase(dynClient dynamic.Interface, namespace, templateName, instanceName, phase string) {
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"lastInstanceName":  instanceName,
			"lastInstancePhase": phase,
		},
	}
	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(cdagTemplateGVR).Namespace(namespace).Patch(
		context.Background(), templateName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

// --------------------------------------------------------------------------
// Garbage collection of old instances
// --------------------------------------------------------------------------

// gcOldInstances deletes the oldest Failed CDAG instances for a template
// if the total count exceeds maxInstances.
func gcOldInstances(dynClient dynamic.Interface, namespace, templateName string, maxInstances int) {
	list, err := dynClient.Resource(cdagGVR).Namespace(namespace).List(
		context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("dsf.io/cdag-template=%s", templateName),
		},
	)
	if err != nil || len(list.Items) <= maxInstances {
		return
	}

	// Sort by creation time (oldest first).
	sort.Slice(list.Items, func(i, j int) bool {
		ti := list.Items[i].GetCreationTimestamp().Time
		tj := list.Items[j].GetCreationTimestamp().Time
		return ti.Before(tj)
	})

	// Only GC Failed instances (never touch Running/Pending/Degraded).
	var candidates []unstructured.Unstructured
	for _, item := range list.Items {
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		if phase == "Failed" {
			candidates = append(candidates, item)
		}
	}

	toDelete := len(list.Items) - maxInstances
	if toDelete <= 0 {
		return
	}
	if toDelete > len(candidates) {
		toDelete = len(candidates)
	}

	for i := 0; i < toDelete; i++ {
		name := candidates[i].GetName()
		if err := dynClient.Resource(cdagGVR).Namespace(namespace).Delete(
			context.Background(), name, metav1.DeleteOptions{},
		); err != nil {
			log.Printf("[cdag-template] GC: failed to delete instance %s: %v", name, err)
		} else {
			log.Printf("[cdag-template] GC: deleted old instance %s", name)
		}
	}
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// getTemplateForCDAG returns the cached CDAGTemplate for a CDAG if it has the
// dsf.io/cdag-template label. Returns nil if no template is associated.
func getTemplateForCDAG(obj *unstructured.Unstructured) *unstructured.Unstructured {
	labels := obj.GetLabels()
	templateName := labels["dsf.io/cdag-template"]
	if templateName == "" {
		return nil
	}
	key := obj.GetNamespace() + "/" + templateName
	raw, ok := cdagTemplateCache.Load(key)
	if !ok {
		return nil
	}
	return raw.(*unstructured.Unstructured)
}

// getInstanceNumber returns the instance number from a CDAG's dsf.io/instance label, or 0.
func getInstanceNumber(obj *unstructured.Unstructured) int {
	labels := obj.GetLabels()
	s := labels["dsf.io/instance"]
	if s == "" {
		return 0
	}
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}

// cdagTemplateNameFromLabels extracts the template name from CDAG labels.
func cdagTemplateNameFromLabels(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	return labels["dsf.io/cdag-template"]
}

// isTemplateInstance returns true if the CDAG was created from a template.
func isTemplateInstance(labels map[string]string) bool {
	return cdagTemplateNameFromLabels(labels) != ""
}

// filterNonEmptyStrings removes empty strings from a slice.
func filterNonEmptyStrings(ss []string) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}
