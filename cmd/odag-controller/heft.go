package main

import (
	"log"
	"sort"
)

// heftAssignTasks implements resource-aware HEFT scheduling.
//
// Unlike classical HEFT which assumes sequential execution on each node,
// this version models parallel execution: independent tasks on the same
// node can overlap as long as the node has sufficient CPU and memory.
//
// Steps:
//  1. Compute upward rank using average bandwidth/runtime (same as classic HEFT).
//  2. Sort tasks by decreasing rank (critical path first).
//  3. For each task in priority order, pick the node that minimises EFT:
//     EST(t,p) = max(depsReady, earliestResourceSlot(p, t.cpu, t.mem))
//     EFT(t,p) = EST(t,p) + runtime(t)
//
// Tasks with no dependency chain between them CAN overlap on the same node
// if resources allow. Tasks with a dependency always wait for the predecessor.

// runtimeResolver returns the expected runtime (seconds) for a task on a given node.
type runtimeResolver func(taskName, nodeName string) float64

// dataSizeResolver returns the expected output data size (bytes) for a task on a given node.
type dataSizeResolver func(taskName, nodeName string) int64

// nodeTimeline tracks resource commitments on a single node over time.
// Each scheduled task occupies a time interval consuming some CPU and memory.
type nodeTimeline struct {
	totalCPU int64 // allocatable millicores
	totalMem int64 // allocatable bytes
	slots    []timeSlot
}

type timeSlot struct {
	start, end float64
	cpuMillis  int64
	memBytes   int64
	taskName   string
}

// earliestStart finds the earliest time >= minStart where the node has enough
// free CPU and memory to run a task of the given duration.
func (nt *nodeTimeline) earliestStart(cpuNeed, memNeed int64, duration, minStart float64) float64 {
	if cpuNeed <= 0 && memNeed <= 0 {
		return minStart // no resource constraints
	}

	// Check if we can start at minStart.
	t := minStart
	for {
		if nt.canFit(t, t+duration, cpuNeed, memNeed) {
			return t
		}
		// Advance to the next slot boundary after t.
		nextBoundary := nt.nextBoundaryAfter(t)
		if nextBoundary <= t {
			return t // no more boundaries — must be free
		}
		t = nextBoundary
	}
}

// canFit checks if a task with given resource needs can fit in [start, end]
// without exceeding the node's capacity at any point.
func (nt *nodeTimeline) canFit(start, end float64, cpuNeed, memNeed int64) bool {
	for _, s := range nt.slots {
		// Check overlap.
		if s.end <= start || s.start >= end {
			continue // no overlap
		}
		// This slot overlaps with [start, end]. Check resource capacity.
		// We need: existing usage + new task <= total capacity.
		// Simplification: check peak usage at the overlap region.
		usedCPU := nt.cpuUsedAt((max(s.start, start) + min(s.end, end)) / 2)
		usedMem := nt.memUsedAt((max(s.start, start) + min(s.end, end)) / 2)
		if usedCPU+cpuNeed > nt.totalCPU || usedMem+memNeed > nt.totalMem {
			return false
		}
	}
	// Also check that just our task alone fits.
	return cpuNeed <= nt.totalCPU && memNeed <= nt.totalMem
}

// cpuUsedAt returns total CPU committed at time t.
func (nt *nodeTimeline) cpuUsedAt(t float64) int64 {
	total := int64(0)
	for _, s := range nt.slots {
		if s.start <= t && t < s.end {
			total += s.cpuMillis
		}
	}
	return total
}

// memUsedAt returns total memory committed at time t.
func (nt *nodeTimeline) memUsedAt(t float64) int64 {
	total := int64(0)
	for _, s := range nt.slots {
		if s.start <= t && t < s.end {
			total += s.memBytes
		}
	}
	return total
}

// nextBoundaryAfter returns the next slot start or end time strictly after t.
func (nt *nodeTimeline) nextBoundaryAfter(t float64) float64 {
	best := t + 1e9 // sentinel
	for _, s := range nt.slots {
		if s.start > t && s.start < best {
			best = s.start
		}
		if s.end > t && s.end < best {
			best = s.end
		}
	}
	return best
}

// commit adds a task to the timeline.
func (nt *nodeTimeline) commit(taskName string, start, end float64, cpuMillis, memBytes int64) {
	nt.slots = append(nt.slots, timeSlot{
		start: start, end: end,
		cpuMillis: cpuMillis, memBytes: memBytes,
		taskName: taskName,
	})
}

// parseTaskCPUMillis parses "500m" → 500, "2" → 2000, "" → 0.
func parseTaskCPUMillis(s string) int64 {
	if s == "" {
		return 0
	}
	q, err := parseResourceQuantity(s)
	if err != nil {
		return 0
	}
	return q.MilliValue()
}

// parseTaskMemBytes parses "256Mi" → bytes, "" → 0.
func parseTaskMemBytes(s string) int64 {
	if s == "" {
		return 0
	}
	q, err := parseResourceQuantity(s)
	if err != nil {
		return 0
	}
	return q.Value()
}

// heftScheduleEntry holds the scheduling decision for a single task.
type heftScheduleEntry struct {
	Node     string
	EstStart float64
	EstEnd   float64
}

// heftResult holds both the node assignment map and the predicted schedule.
type heftResult struct {
	assignMap map[string]nodeInfo
	schedule  map[string]heftScheduleEntry
}

func heftAssignTasks(tasks []taskSpec, nodeMap map[string]nodeInfo, rtResolver runtimeResolver, dsResolver dataSizeResolver, bwResolver bandwidthResolver) heftResult {
	if len(tasks) == 0 {
		return heftResult{assignMap: map[string]nodeInfo{}, schedule: map[string]heftScheduleEntry{}}
	}

	taskByName := make(map[string]*taskSpec, len(tasks))
	for i := range tasks {
		taskByName[tasks[i].Name] = &tasks[i]
	}

	// Build successors map.
	successors := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		if _, ok := successors[t.Name]; !ok {
			successors[t.Name] = nil
		}
		for _, dep := range t.Dependencies {
			successors[dep] = append(successors[dep], t.Name)
		}
	}

	// Resolver closures.
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

	// Average bandwidth/runtime for rank estimation.
	avgBandwidth := computeAvgBandwidth(nodeMap, resolveBandwidth)

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

	commCostEstimate := func(taskName string) float64 {
		var bytes int64
		if dsResolver != nil {
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

	// Compute upward rank (same as classic HEFT).
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

	// Sort by decreasing rank.
	sorted := make([]string, 0, len(tasks))
	for _, t := range tasks {
		sorted = append(sorted, t.Name)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return rank[sorted[i]] > rank[sorted[j]]
	})

	// Initialize per-node resource timelines.
	timelines := make(map[string]*nodeTimeline, len(nodeMap))
	for name, ni := range nodeMap {
		timelines[name] = &nodeTimeline{
			totalCPU: ni.cpuMillis,
			totalMem: ni.memBytes,
		}
	}

	taskFinish := make(map[string]float64, len(tasks))
	taskAssigned := make(map[string]string, len(tasks))
	result := make(map[string]nodeInfo, len(tasks))
	schedule := make(map[string]heftScheduleEntry, len(tasks))

	for _, name := range sorted {
		t := taskByName[name]

		// Determine candidate nodes.
		var candidates []string
		if len(t.Constraints) > 0 {
			for _, c := range t.Constraints {
				if _, ok := nodeMap[c]; ok {
					candidates = append(candidates, c)
				}
			}
			if len(candidates) == 0 {
				log.Printf("[heft] task %s: no constraint nodes in cluster; using all", name)
				for n := range nodeMap {
					candidates = append(candidates, n)
				}
			}
		} else {
			for n := range nodeMap {
				candidates = append(candidates, n)
			}
		}

		// Task resource requirements.
		taskCPU := parseTaskCPUMillis(t.CPU)
		taskMem := parseTaskMemBytes(t.Memory)

		bestNode := ""
		bestEFT := -1.0

		for _, nodeName := range candidates {
			tl := timelines[nodeName]

			// Minimum start: all deps must be done + comm cost.
			depsReady := 0.0
			for _, dep := range t.Dependencies {
				depFinish := taskFinish[dep]
				depNode := taskAssigned[dep]

				var commCost float64
				if depNode != "" && depNode == nodeName {
					commCost = 0
				} else {
					bytes := resolveDataSizeBytes(dep, depNode)
					bw := resolveBandwidth(depNode, nodeName)
					commCost = float64(bytes) / bw
				}

				if ready := depFinish + commCost; ready > depsReady {
					depsReady = ready
				}
			}

			// Find earliest start where node has enough resources.
			runtime := resolveRuntime(name, nodeName)
			est := tl.earliestStart(taskCPU, taskMem, runtime, depsReady)
			eft := est + runtime

			if bestNode == "" || eft < bestEFT {
				bestNode = nodeName
				bestEFT = eft
			}
		}

		// Commit resources on the chosen node.
		taskCPUFinal := parseTaskCPUMillis(t.CPU)
		taskMemFinal := parseTaskMemBytes(t.Memory)
		runtime := resolveRuntime(name, bestNode)
		estFinal := timelines[bestNode].earliestStart(taskCPUFinal, taskMemFinal, runtime, bestEFT-runtime)
		timelines[bestNode].commit(name, estFinal, bestEFT, taskCPUFinal, taskMemFinal)

		taskAssigned[name] = bestNode
		taskFinish[name] = bestEFT
		result[name] = nodeMap[bestNode]
		schedule[name] = heftScheduleEntry{Node: bestNode, EstStart: estFinal, EstEnd: bestEFT}

		log.Printf("[heft] %-20s rank=%.1f -> %-10s EST=%.1fs EFT=%.1fs (cpu=%dm mem=%dMi)",
			name, rank[name], bestNode, estFinal, bestEFT, taskCPU, taskMem/(1<<20))
	}

	return heftResult{assignMap: result, schedule: schedule}
}

// computeAvgBandwidth returns the mean bandwidth across all distinct node pairs.
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
