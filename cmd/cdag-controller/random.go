package main

import (
	"log"
	"math/rand"
)

// --------------------------------------------------------------------------
// Random scheduler: constraint-aware uniform random placement.
// --------------------------------------------------------------------------
//
// CDAG tasks run continuously, so this scheduler ignores makespan and
// picks each task independently: uniform random over the intersection of
// the task's constraint list and the cluster's schedulable nodes, falling
// back to any cluster node if the constraint list is empty or no
// constrained node is currently schedulable.
//
// `pickNode` is also used by `locality` and `throughput` as a fallback
// (e.g., for unassignable tasks in a broken topology) and by the replica
// re-creation path in main.go.

// randomAssignTasks assigns each CDAG task to a node using constraint-aware
// random placement.
func randomAssignTasks(tasks []cdagTaskSpec, clusterNodes []string) map[string]string {
	result := make(map[string]string, len(tasks))
	for _, t := range tasks {
		result[t.Name] = pickNode(t.Constraints, clusterNodes)
	}
	return result
}

// pickNode selects a random node from the constraint list intersected with
// clusterNodes. Falls back to any cluster node if no constraint nodes are
// available.
func pickNode(constraints []string, clusterNodes []string) string {
	if len(clusterNodes) == 0 {
		return ""
	}
	if len(constraints) > 0 {
		nodeSet := make(map[string]bool, len(clusterNodes))
		for _, n := range clusterNodes {
			nodeSet[n] = true
		}
		var allowed []string
		for _, c := range constraints {
			if nodeSet[c] {
				allowed = append(allowed, c)
			}
		}
		if len(allowed) > 0 {
			return allowed[rand.Intn(len(allowed))]
		}
		log.Printf("[cdag-ctrl] no constraint nodes available in cluster; using any node")
	}
	return clusterNodes[rand.Intn(len(clusterNodes))]
}
