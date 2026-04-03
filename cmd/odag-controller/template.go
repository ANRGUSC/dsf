package main

import (
	"context"
	"database/sql"
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

// CRD reference for dsf.io/v1/ODAGTemplate
var odagTemplateGVR = schema.GroupVersionResource{
	Group:    "dsf.io",
	Version:  "v1",
	Resource: "odagtemplates",
}

// templateCache stores the latest ODAGTemplate objects, keyed by "ns/name".
var templateCache sync.Map

// --------------------------------------------------------------------------
// Template watcher
// --------------------------------------------------------------------------

// watchODAGTemplates watches ODAGTemplate CRs and caches them in memory.
func watchODAGTemplates(dynClient dynamic.Interface) {
	for {
		watcher, err := dynClient.Resource(odagTemplateGVR).Namespace("").Watch(
			context.Background(), metav1.ListOptions{},
		)
		if err != nil {
			log.Printf("[template] error watching ODAGTemplates: %v; retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("[template] watching ODAGTemplate resources")
		for event := range watcher.ResultChan() {
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			key := obj.GetNamespace() + "/" + obj.GetName()
			switch string(event.Type) {
			case "ADDED", "MODIFIED":
				templateCache.Store(key, obj.DeepCopy())
				log.Printf("[template] cached template %s", key)
			case "DELETED":
				templateCache.Delete(key)
				log.Printf("[template] removed template %s", key)
			}
		}
		log.Println("[template] ODAGTemplate watcher closed; reconnecting in 2s")
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// Create a run from a template
// --------------------------------------------------------------------------

// createRunFromTemplate creates a new ODAG CR from an ODAGTemplate.
// It auto-increments the run number and names the ODAG "<template>-run-NNN".
func createRunFromTemplate(dynClient dynamic.Interface, db *sql.DB,
	templateObj *unstructured.Unstructured) (string, error) {

	templateName := templateObj.GetName()
	namespace := templateObj.GetNamespace()

	// Get next run number.
	runNum, err := nextRunID(db, templateName)
	if err != nil {
		return "", fmt.Errorf("get next run ID: %w", err)
	}
	odagName := fmt.Sprintf("%s-run-%03d", templateName, runNum)

	// Extract spec from template, stripping template-only fields.
	spec, _, err := unstructured.NestedMap(templateObj.Object, "spec")
	if err != nil {
		return "", fmt.Errorf("extract template spec: %w", err)
	}
	delete(spec, "profiling")
	delete(spec, "defaults")
	delete(spec, "retention")
	delete(spec, "description")

	// Build the ODAG CR.
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

	if _, err := dynClient.Resource(odagGVR).Namespace(namespace).Create(
		context.Background(), odag, metav1.CreateOptions{},
	); err != nil {
		return "", fmt.Errorf("create ODAG run: %w", err)
	}

	log.Printf("[template] created run %s/%s (run #%d) from template %s",
		namespace, odagName, runNum, templateName)
	return odagName, nil
}

// --------------------------------------------------------------------------
// Profiling config extraction
// --------------------------------------------------------------------------

// profilingConfig holds the profiling settings extracted from an ODAGTemplate.
type profilingConfig struct {
	Enabled         bool
	WarmupRuns      int
	MinSamples      int
	EmaAlpha        float64
	MaxSamples      int
	RuntimeSource   string // "manual" | "profiler" | "hybrid"
	BandwidthSource string // "external" | "profiler" | "hybrid"
}

// defaultProfilingConfig returns the default profiling configuration.
func defaultProfilingConfig() profilingConfig {
	return profilingConfig{
		Enabled:         true,
		WarmupRuns:      0,
		MinSamples:      3,
		EmaAlpha:        0.3,
		MaxSamples:      100,
		RuntimeSource:   "profiler",
		BandwidthSource: "external",
	}
}

// extractProfilingConfig reads the profiling settings from a template object.
func extractProfilingConfig(templateObj *unstructured.Unstructured) profilingConfig {
	cfg := defaultProfilingConfig()
	if templateObj == nil {
		return cfg
	}

	prof, ok, _ := unstructured.NestedMap(templateObj.Object, "spec", "profiling")
	if !ok {
		return cfg
	}

	if v, ok := prof["enabled"].(bool); ok {
		cfg.Enabled = v
	}
	if v, ok, _ := unstructured.NestedInt64(prof, "warmupRuns"); ok {
		cfg.WarmupRuns = int(v)
	}
	if v, ok, _ := unstructured.NestedInt64(prof, "minSamples"); ok {
		cfg.MinSamples = int(v)
	}
	if v, ok := prof["emaAlpha"].(float64); ok {
		cfg.EmaAlpha = v
	} else if v, ok, _ := unstructured.NestedInt64(prof, "emaAlpha"); ok {
		cfg.EmaAlpha = float64(v)
	}
	if v, ok, _ := unstructured.NestedInt64(prof, "maxSamples"); ok {
		cfg.MaxSamples = int(v)
	}
	if v, ok := prof["runtimeSource"].(string); ok && (v == "manual" || v == "profiler" || v == "hybrid") {
		cfg.RuntimeSource = v
	}
	if v, ok := prof["bandwidthSource"].(string); ok && (v == "external" || v == "profiler" || v == "hybrid") {
		cfg.BandwidthSource = v
	}

	return cfg
}

// extractDefaultRuntime reads spec.defaults.runtime from a template.
func extractDefaultRuntime(templateObj *unstructured.Unstructured) float64 {
	if templateObj == nil {
		return 10.0
	}
	v, ok, _ := unstructured.NestedFloat64(templateObj.Object, "spec", "defaults", "runtime")
	if !ok {
		// Try int64 (CRD stores numbers as int64 when they have no decimal).
		if iv, ok, _ := unstructured.NestedInt64(templateObj.Object, "spec", "defaults", "runtime"); ok {
			return float64(iv)
		}
		return 10.0
	}
	return v
}

// extractDefaultDataSize reads spec.defaults.dataSize from a template.
func extractDefaultDataSize(templateObj *unstructured.Unstructured) string {
	if templateObj == nil {
		return "0"
	}
	v, ok, _ := unstructured.NestedString(templateObj.Object, "spec", "defaults", "dataSize")
	if !ok {
		return "0"
	}
	return v
}

// extractRetentionMaxRuns reads spec.retention.maxRuns from a template.
func extractRetentionMaxRuns(templateObj *unstructured.Unstructured) int {
	if templateObj == nil {
		return 50
	}
	v, ok, _ := unstructured.NestedInt64(templateObj.Object, "spec", "retention", "maxRuns")
	if !ok {
		return 50
	}
	return int(v)
}

// --------------------------------------------------------------------------
// Post-completion profiling
// --------------------------------------------------------------------------

// profileCompletedRun records profiler observations from a completed ODAG run.
// Called from checkODAGCompletion when the ODAG has the dsf.io/template label.
func profileCompletedRun(dynClient dynamic.Interface, db *sql.DB,
	namespace, odagName, templateName string, runNum int,
	tasks []taskSpec, assignMap map[string]nodeInfo,
	taskStartTimes, taskCompletionTimes map[string]time.Time,
	makespan float64) {

	// Look up the template from cache.
	key := namespace + "/" + templateName
	raw, ok := templateCache.Load(key)
	if !ok {
		log.Printf("[profiler] template %s not in cache; skipping profiling", key)
		return
	}
	templateObj := raw.(*unstructured.Unstructured)
	cfg := extractProfilingConfig(templateObj)

	if !cfg.Enabled {
		log.Printf("[profiler] profiling disabled for template %s", templateName)
		return
	}

	// Check warmup: skip profiling for early runs.
	if runNum <= cfg.WarmupRuns {
		log.Printf("[profiler] run %d <= warmupRuns %d for %s; skipping", runNum, cfg.WarmupRuns, templateName)
		return
	}

	// Record task profiles.
	for _, t := range tasks {
		ni := assignMap[t.Name]
		start, hasStart := taskStartTimes[t.Name]
		end, hasEnd := taskCompletionTimes[t.Name]
		if !hasStart || !hasEnd {
			continue
		}
		observedRuntime := end.Sub(start).Seconds()

		// Query actual output bytes from the data-agent on the task's node.
		// Falls back to spec hint if the agent doesn't have the data.
		observedDataBytes := float64(queryTaskBytes(ni.ip, odagName, t.Name))
		if observedDataBytes <= 0 {
			observedDataBytes = float64(parseDataSizeBytes(t.DataSize))
		}

		if err := recordTaskProfile(db, templateName, t.Name, ni.name,
			observedRuntime, observedDataBytes, cfg.EmaAlpha, cfg.MaxSamples); err != nil {
			log.Printf("[profiler] error recording task %s: %v", t.Name, err)
		} else {
			log.Printf("[profiler] recorded %s/%s on %s: %.2fs", templateName, t.Name, ni.name, observedRuntime)
		}
		// Also record image-based profile (shared across templates).
		if err := recordImageProfile(db, t.Image, ni.name,
			observedRuntime, observedDataBytes, cfg.EmaAlpha, cfg.MaxSamples); err != nil {
			log.Printf("[profiler] error recording image profile %s: %v", t.Image, err)
		}
	}

	// Record link profiles (data transfers between dependent tasks on different nodes).
	taskByName := make(map[string]*taskSpec, len(tasks))
	for i := range tasks {
		taskByName[tasks[i].Name] = &tasks[i]
	}
	for _, t := range tasks {
		for _, dep := range t.Dependencies {
			srcNode := assignMap[dep].name
			dstNode := assignMap[t.Name].name
			if srcNode == dstNode {
				continue
			}
			depEnd, hasDep := taskCompletionTimes[dep]
			childStart, hasChild := taskStartTimes[t.Name]
			if !hasDep || !hasChild {
				continue
			}
			transferSec := childStart.Sub(depEnd).Seconds()
			if transferSec < 0 {
				transferSec = 0
			}
			dataBytes := float64(parseDataSizeBytes(taskByName[dep].DataSize))

			if err := recordLinkProfile(db, templateName, dep, t.Name, srcNode, dstNode,
				dataBytes, transferSec, cfg.EmaAlpha, cfg.MaxSamples); err != nil {
				log.Printf("[profiler] error recording link %s→%s: %v", dep, t.Name, err)
			}
		}
	}

	// Update template status.
	updateTemplateStatus(dynClient, namespace, templateName, odagName, makespan)

	// Run GC if needed.
	maxRuns := extractRetentionMaxRuns(templateObj)
	gcOldRuns(dynClient, namespace, templateName, maxRuns)
}

// --------------------------------------------------------------------------
// Template status updates
// --------------------------------------------------------------------------

// updateTemplateStatus updates the ODAGTemplate's status with the latest run info
// and a condensed profile summary.
func updateTemplateStatus(dynClient dynamic.Interface, namespace, templateName, lastRunName string, makespan float64) {
	// Read current runCount.
	tmpl, err := dynClient.Resource(odagTemplateGVR).Namespace(namespace).Get(
		context.Background(), templateName, metav1.GetOptions{},
	)
	if err != nil {
		log.Printf("[template] failed to get template %s for status update: %v", templateName, err)
		return
	}

	runCount, _, _ := unstructured.NestedInt64(tmpl.Object, "status", "runCount")

	// Build profile summary from profiler DB if available.
	var profileSummary map[string]interface{}
	if profilerDB != nil {
		profiles := getTaskProfiles(profilerDB, templateName)
		if len(profiles) > 0 {
			profileSummary = make(map[string]interface{})
			for task, nodeMap := range profiles {
				nodeRuntimes := make(map[string]interface{})
				for node, p := range nodeMap {
					nodeRuntimes[node] = p.Runtime
				}
				profileSummary[task] = nodeRuntimes
			}
		}
	}

	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"runCount":        runCount + 1,
			"lastRunName":     lastRunName,
			"lastRunPhase":    "Succeeded",
			"lastRunMakespan": makespan,
		},
	}
	if profileSummary != nil {
		patch["status"].(map[string]interface{})["profileSummary"] = profileSummary
	}

	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagTemplateGVR).Namespace(namespace).Patch(
		context.Background(), templateName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}

// --------------------------------------------------------------------------
// Garbage collection of old runs
// --------------------------------------------------------------------------

// gcOldRuns deletes the oldest ODAG runs for a template if the count exceeds maxRuns.
func gcOldRuns(dynClient dynamic.Interface, namespace, templateName string, maxRuns int) {
	list, err := dynClient.Resource(odagGVR).Namespace(namespace).List(
		context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("dsf.io/template=%s", templateName),
		},
	)
	if err != nil || len(list.Items) <= maxRuns {
		return
	}

	// Sort by creation time (oldest first).
	sort.Slice(list.Items, func(i, j int) bool {
		ti := list.Items[i].GetCreationTimestamp().Time
		tj := list.Items[j].GetCreationTimestamp().Time
		return ti.Before(tj)
	})

	// Only GC completed runs.
	var candidates []unstructured.Unstructured
	for _, item := range list.Items {
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		if phase == "Succeeded" || phase == "Failed" {
			candidates = append(candidates, item)
		}
	}

	// Keep maxRuns total (including running ones). Delete oldest completed.
	running := len(list.Items) - len(candidates)
	toDelete := len(list.Items) - maxRuns
	if toDelete <= 0 {
		return
	}
	// Don't delete more than completed candidates.
	if toDelete > len(candidates) {
		toDelete = len(candidates)
	}
	// Keep at least enough room for running ones.
	_ = running

	for i := 0; i < toDelete; i++ {
		name := candidates[i].GetName()
		if err := dynClient.Resource(odagGVR).Namespace(namespace).Delete(
			context.Background(), name, metav1.DeleteOptions{},
		); err != nil {
			log.Printf("[template] GC: failed to delete run %s: %v", name, err)
		} else {
			log.Printf("[template] GC: deleted old run %s", name)
		}
	}
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// getTemplateForODAG returns the cached ODAGTemplate for an ODAG if it has the
// dsf.io/template label. Returns nil if no template is associated.
func getTemplateForODAG(obj *unstructured.Unstructured) *unstructured.Unstructured {
	labels := obj.GetLabels()
	templateName := labels["dsf.io/template"]
	if templateName == "" {
		return nil
	}
	key := obj.GetNamespace() + "/" + templateName
	raw, ok := templateCache.Load(key)
	if !ok {
		return nil
	}
	return raw.(*unstructured.Unstructured)
}

// getRunNumber returns the run number from an ODAG's dsf.io/run label, or 0.
func getRunNumber(obj *unstructured.Unstructured) int {
	labels := obj.GetLabels()
	s := labels["dsf.io/run"]
	if s == "" {
		return 0
	}
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}

// templateNameFromLabels extracts the template name from ODAG labels.
func templateNameFromLabels(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	return labels["dsf.io/template"]
}

// isTemplateRun returns true if the ODAG labels indicate it was created from a template.
func isTemplateRun(labels map[string]string) bool {
	return templateNameFromLabels(labels) != ""
}

// getODAGLabels extracts labels from an unstructured object.
func getODAGLabels(obj *unstructured.Unstructured) map[string]string {
	return obj.GetLabels()
}

// filterNonEmpty removes empty strings from a slice. Useful for split results.
func filterNonEmpty(ss []string) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}
