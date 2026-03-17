package main

import (
	"log"
	"sort"
)

// heftBandwidth is the measured cross-node throughput in bytes/second.
// Derived from observation: merge task pushes 250 MB in ~6.5s ≈ 38 MB/s, rounded to 40 MB/s.
const heftBandwidth = 40_000_000.0 // 40 MB/s

// heftAssignTasks implements the HEFT (Heterogeneous Earliest Finish Time) algorithm.
//
// Steps:
//  1. Compute upward rank: rank(t) = runtime(t) + max_succ(commCost(t→succ) + rank(succ))
//     commCost uses cross-node bandwidth for rank computation (average-case assumption).
//  2. Sort tasks by decreasing rank.
//  3. For each task in priority order, pick the node that minimises EFT:
//     EST(t,p) = max(nodeAvail[p], max_pred(taskFinish[pred] + commCost(pred→t)))
//     commCost = 0 when pred and t land on the same node, cross-node otherwise.
//     EFT(t,p) = EST(t,p) + runtime(t)
//
// Constraints (spec.tasks[].constraints.nodeNames) are respected: the algorithm
// only considers allowed nodes for each task.
func heftAssignTasks(tasks []taskSpec, nodeMap map[string]nodeInfo) map[string]nodeInfo {
	if len(tasks) == 0 {
		return map[string]nodeInfo{}
	}

	// Index tasks by name for quick lookup.
	taskByName := make(map[string]*taskSpec, len(tasks))
	for i := range tasks {
		taskByName[tasks[i].Name] = &tasks[i]
	}

	// Build successors map: successors[t] = list of tasks that depend on t.
	successors := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		if _, ok := successors[t.Name]; !ok {
			successors[t.Name] = nil
		}
		for _, dep := range t.Dependencies {
			successors[dep] = append(successors[dep], t.Name)
		}
	}

	// commCostEstimate returns the communication cost (seconds) for data produced
	// by taskName, assuming cross-node transfer (used during rank computation).
	commCostEstimate := func(taskName string) float64 {
		bytes := parseDataSizeBytes(taskByName[taskName].DataSize)
		if bytes == 0 {
			return 0
		}
		return float64(bytes) / heftBandwidth
	}

	// Compute upward rank via memoized recursion.
	rank := make(map[string]float64, len(tasks))
	var computeRank func(name string) float64
	computeRank = func(name string) float64 {
		if v, ok := rank[name]; ok {
			return v
		}
		t := taskByName[name]
		maxSuccCost := 0.0
		for _, s := range successors[name] {
			cost := commCostEstimate(name) + computeRank(s)
			if cost > maxSuccCost {
				maxSuccCost = cost
			}
		}
		r := t.Runtime + maxSuccCost
		rank[name] = r
		return r
	}
	for _, t := range tasks {
		computeRank(t.Name)
	}

	// Sort tasks by decreasing upward rank (highest rank = most critical path first).
	sorted := make([]string, 0, len(tasks))
	for _, t := range tasks {
		sorted = append(sorted, t.Name)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return rank[sorted[i]] > rank[sorted[j]]
	})

	// Schedule each task in priority order.
	nodeAvail := make(map[string]float64, len(nodeMap)) // node → earliest available time (seconds)
	taskFinish := make(map[string]float64, len(tasks))  // task → actual finish time
	taskAssigned := make(map[string]string, len(tasks)) // task → assigned node name
	result := make(map[string]nodeInfo, len(tasks))

	for _, name := range sorted {
		t := taskByName[name]

		// Determine candidate nodes, respecting per-task constraints.
		var candidates []string
		if len(t.Constraints) > 0 {
			for _, c := range t.Constraints {
				if _, ok := nodeMap[c]; ok {
					candidates = append(candidates, c)
				}
			}
			if len(candidates) == 0 {
				log.Printf("[heft] task %s: constraint nodes not in cluster; using all nodes", name)
				for n := range nodeMap {
					candidates = append(candidates, n)
				}
			}
		} else {
			for n := range nodeMap {
				candidates = append(candidates, n)
			}
		}

		// For each candidate node, compute EST and EFT.
		bestNode := ""
		bestEFT := -1.0

		for _, nodeName := range candidates {
			// EST is at least when the node becomes free.
			est := nodeAvail[nodeName]

			// EST is also constrained by when all predecessors finish + comm cost.
			for _, dep := range t.Dependencies {
				depFinish := taskFinish[dep]
				depNode := taskAssigned[dep]

				var commCost float64
				if depNode != "" && depNode == nodeName {
					commCost = 0 // same node: no transfer cost
				} else {
					bytes := parseDataSizeBytes(taskByName[dep].DataSize)
					commCost = float64(bytes) / heftBandwidth
				}

				if ready := depFinish + commCost; ready > est {
					est = ready
				}
			}

			eft := est + t.Runtime
			if bestNode == "" || eft < bestEFT {
				bestNode = nodeName
				bestEFT = eft
			}
		}

		taskAssigned[name] = bestNode
		taskFinish[name] = bestEFT
		nodeAvail[bestNode] = bestEFT
		result[name] = nodeMap[bestNode]

		log.Printf("[heft] %-20s rank=%.1f -> %-10s EFT=%.1fs", name, rank[name], bestNode, bestEFT)
	}

	return result
}
