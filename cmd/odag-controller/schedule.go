package main

import (
	"context"
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

type predictedTaskEntry struct {
	Name     string  `json:"name"`
	Node     string  `json:"node"`
	EstStart float64 `json:"estStart"` // seconds from t=0 (deployment time)
	EstEnd   float64 `json:"estEnd"`
}

// computePredictedSchedule computes estimated start/end times for each task
// given a fixed node assignment. Processes tasks in topological order.
//
// Communication cost: dataSize(dep) / heftBandwidth for cross-node, 0 for same-node.
// Node contention: if two tasks share a node, the later one waits for the earlier.
func computePredictedSchedule(tasks []taskSpec, assignMap map[string]nodeInfo) []predictedTaskEntry {
	taskByName := make(map[string]*taskSpec, len(tasks))
	for i := range tasks {
		taskByName[tasks[i].Name] = &tasks[i]
	}

	nodeAvail := make(map[string]float64)
	taskFinish := make(map[string]float64)
	scheduled := make(map[string]bool)
	result := make([]predictedTaskEntry, 0, len(tasks))

	// Iterate until all tasks are scheduled (topological order via repeated passes).
	for len(result) < len(tasks) {
		progress := false
		for _, t := range tasks {
			if scheduled[t.Name] {
				continue
			}
			allDepsDone := true
			for _, dep := range t.Dependencies {
				if !scheduled[dep] {
					allDepsDone = false
					break
				}
			}
			if !allDepsDone {
				continue
			}

			nodeName := assignMap[t.Name].name
			est := nodeAvail[nodeName]

			for _, dep := range t.Dependencies {
				depFinish := taskFinish[dep]
				depNode := assignMap[dep].name
				var commCost float64
				if depNode != nodeName {
					bytes := parseDataSizeBytes(taskByName[dep].DataSize)
					bw := linkBandwidth(depNode, nodeName)
					commCost = float64(bytes) / bw
				}
				if ready := depFinish + commCost; ready > est {
					est = ready
				}
			}

			eft := est + t.Runtime
			nodeAvail[nodeName] = eft
			taskFinish[t.Name] = eft
			scheduled[t.Name] = true
			progress = true
			result = append(result, predictedTaskEntry{
				Name:     t.Name,
				Node:     nodeName,
				EstStart: est,
				EstEnd:   eft,
			})
		}
		if !progress {
			break // cycle or unresolvable deps — stop to avoid infinite loop
		}
	}
	return result
}

func writePredictedSchedule(dynClient dynamic.Interface, namespace, odagName string, predicted []predictedTaskEntry) {
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"predictedTasks": predicted,
		},
	}
	data, _ := json.Marshal(patch)
	_, _ = dynClient.Resource(odagGVR).Namespace(namespace).Patch(
		context.Background(), odagName, types.MergePatchType, data,
		metav1.PatchOptions{}, "status",
	)
}
