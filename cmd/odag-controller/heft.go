package main

import (
	"log"
	"sort"
)

// Default bandwidth for node pairs not in the bandwidth matrix.
const heftDefaultBandwidth = 125_000_000.0 // 125 MB/s (1 Gbps)

// heftBandwidthMatrix defines per-link bandwidth in bytes/second.
// Key: "srcNode->dstNode". If a pair is missing, heftDefaultBandwidth is used.
// This reflects the actual tc-shaped network topology.
var heftBandwidthMatrix = map[string]float64{
	// anrg-3 ↔ anrg-5: 500 Mbps = 62.5 MB/s
	"anrg-3->anrg-5": 62_500_000,
	"anrg-5->anrg-3": 62_500_000,
	// anrg-4 ↔ anrg-5: 500 Mbps = 62.5 MB/s
	"anrg-4->anrg-5": 62_500_000,
	"anrg-5->anrg-4": 62_500_000,
	// anrg-3 ↔ anrg-6: 100 Mbps = 12.5 MB/s
	"anrg-3->anrg-6": 12_500_000,
	"anrg-6->anrg-3": 12_500_000,
	// anrg-4 ↔ anrg-6: 100 Mbps = 12.5 MB/s
	"anrg-4->anrg-6": 12_500_000,
	"anrg-6->anrg-4": 12_500_000,
	// anrg-5 ↔ anrg-6: 100 Mbps = 12.5 MB/s
	"anrg-5->anrg-6": 12_500_000,
	"anrg-6->anrg-5": 12_500_000,
	// anrg-3 ↔ anrg-4: 1 Gbps (default, no entry needed)
}

// linkBandwidth returns the bandwidth in bytes/sec between two nodes.
func linkBandwidth(src, dst string) float64 {
	if src == dst {
		return 0 // same node: no transfer
	}
	key := src + "->" + dst
	if bw, ok := heftBandwidthMatrix[key]; ok {
		return bw
	}
	return heftDefaultBandwidth
}

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

	// Compute average bandwidth across all node pairs for rank estimation.
	avgBandwidth := computeAvgBandwidth(nodeMap)

	// commCostEstimate returns the communication cost (seconds) for data produced
	// by taskName, assuming average cross-node bandwidth (used during rank computation).
	commCostEstimate := func(taskName string) float64 {
		bytes := parseDataSizeBytes(taskByName[taskName].DataSize)
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
					bw := linkBandwidth(depNode, nodeName)
					commCost = float64(bytes) / bw
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

// computeAvgBandwidth returns the mean bandwidth across all distinct node pairs.
// Used for rank estimation where we don't yet know placements.
func computeAvgBandwidth(nodeMap map[string]nodeInfo) float64 {
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
			total += linkBandwidth(a, b)
			count++
		}
	}
	return total / float64(count)
}
