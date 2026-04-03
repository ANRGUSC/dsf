package main

import (
	"log"
	"sort"
)

// heftAssignTasks implements the HEFT (Heterogeneous Earliest Finish Time) algorithm
// with heterogeneous link bandwidth awareness.
//
// Steps:
//  1. Compute upward rank: rank(t) = runtime(t) + max_succ(commCost(t→succ) + rank(succ))
//     commCost for rank uses average bandwidth across all node pairs.
//  2. Sort tasks by decreasing rank.
//  3. For each task in priority order, pick the node that minimises EFT:
//     EST(t,p) = max(nodeAvail[p], max_pred(taskFinish[pred] + commCost(pred→t)))
//     commCost uses actual per-link bandwidth between the assigned pred node and candidate node.
//     EFT(t,p) = EST(t,p) + runtime(t)

// runtimeResolver returns the expected runtime (seconds) for a task on a given node.
// If nil is passed, the task's scalar Runtime field is used (backward compatible).
type runtimeResolver func(taskName, nodeName string) float64

// dataSizeResolver returns the expected output data size (bytes) for a task on a given node.
// If nil is passed, the task's DataSize spec field is used (backward compatible).
type dataSizeResolver func(taskName, nodeName string) int64

func heftAssignTasks(tasks []taskSpec, nodeMap map[string]nodeInfo, rtResolver runtimeResolver, dsResolver dataSizeResolver, bwResolver bandwidthResolver) map[string]nodeInfo {
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

	// Helper closures for resolving runtime, data size, and bandwidth.
	resolveRuntime := func(taskName, nodeName string) float64 {
		if rtResolver != nil {
			return rtResolver(taskName, nodeName)
		}
		return taskByName[taskName].Runtime
	}
	resolveDataSizeBytes := func(taskName, nodeName string) int64 {
		if dsResolver != nil {
			return dsResolver(taskName, nodeName)
		}
		return parseDataSizeBytes(taskByName[taskName].DataSize)
	}
	resolveBandwidth := func(src, dst string) float64 {
		if src == dst {
			return 0
		}
		if bwResolver != nil {
			return bwResolver(src, dst)
		}
		return heftDefaultBandwidth
	}

	// Compute average bandwidth across all node pairs for rank estimation.
	avgBandwidth := computeAvgBandwidth(nodeMap, resolveBandwidth)

	// Average runtime across all candidate nodes (used for rank computation
	// where placement is not yet known).
	avgRuntime := func(taskName string) float64 {
		if rtResolver == nil {
			return taskByName[taskName].Runtime
		}
		total := 0.0
		count := 0
		for nodeName := range nodeMap {
			total += resolveRuntime(taskName, nodeName)
			count++
		}
		if count == 0 {
			return taskByName[taskName].Runtime
		}
		return total / float64(count)
	}

	// commCostEstimate returns the communication cost (seconds) for data produced
	// by taskName, assuming average cross-node bandwidth (used during rank computation).
	// Uses average data size across nodes when a resolver is available.
	commCostEstimate := func(taskName string) float64 {
		var bytes int64
		if dsResolver != nil {
			// Average across nodes for rank estimation.
			total := int64(0)
			count := 0
			for nodeName := range nodeMap {
				total += resolveDataSizeBytes(taskName, nodeName)
				count++
			}
			if count > 0 {
				bytes = total / int64(count)
			}
		} else {
			bytes = parseDataSizeBytes(taskByName[taskName].DataSize)
		}
		if bytes == 0 {
			return 0
		}
		return float64(bytes) / avgBandwidth
	}

	// Compute upward rank via memoized recursion.
	rank := make(map[string]float64, len(tasks))
	var computeRank func(name string) float64
	computeRank = func(name string) float64 {
		if v, ok := rank[name]; ok {
			return v
		}
		maxSuccCost := 0.0
		for _, s := range successors[name] {
			cost := commCostEstimate(name) + computeRank(s)
			if cost > maxSuccCost {
				maxSuccCost = cost
			}
		}
		r := avgRuntime(name) + maxSuccCost
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
					bytes := resolveDataSizeBytes(dep, depNode)
					bw := resolveBandwidth(depNode, nodeName)
					commCost = float64(bytes) / bw
				}

				if ready := depFinish + commCost; ready > est {
					est = ready
				}
			}

			eft := est + resolveRuntime(name, nodeName)
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

// computeAvgBandwidth returns the mean bandwidth across all distinct node pairs.
// Used for rank estimation where we don't yet know placements.
func computeAvgBandwidth(nodeMap map[string]nodeInfo, resolveBandwidth func(string, string) float64) float64 {
	nodes := make([]string, 0, len(nodeMap))
	for n := range nodeMap {
		nodes = append(nodes, n)
	}
	if len(nodes) < 2 {
		return heftDefaultBandwidth
	}
	total := 0.0
	count := 0
	for i, a := range nodes {
		for _, b := range nodes[i+1:] {
			total += resolveBandwidth(a, b)
			count++
		}
	}
	return total / float64(count)
}
