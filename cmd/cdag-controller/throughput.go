package main

import (
	"log"
	"math"
)

// --------------------------------------------------------------------------
// Throughput scheduler: maximize the minimum headroom across CDAG edges.
// --------------------------------------------------------------------------
//
// Where `locality` minimizes Σ_edges (rate / effective_bandwidth) — a sum of
// per-edge latency costs — the throughput scheduler minimizes the maximum
// per-edge utilization: max_e (rate(e) / effective_bandwidth(e)). Equivalent
// to maximizing the minimum headroom (bandwidth − rate). This is a
// maximin / min-cut objective: the sustained throughput of a streaming
// pipeline is capped by its bottleneck edge, so bounding the bottleneck is
// what actually preserves end-to-end throughput at saturation.
//
// Ties on max utilization (common with this objective) are broken by the
// locality sum-cost. So `throughput` reduces to "locality with a saturation
// guard" — it never picks a placement that saturates one link to save
// aggregate cost.
//
// Shares flowTracker, effectiveBW, getBandwidth, parseDataRate with
// locality.go.

// throughputAssignTasks assigns CDAG tasks to nodes minimizing the maximum
// edge utilization. Uses two phases (greedy topological placement + swap
// refinement) matching the locality scheduler.
func throughputAssignTasks(tasks []cdagTaskSpec, clusterNodes []string) map[string]string {
	// Data rate per upstream task (the rate its outgoing edges carry).
	dataRateMap := make(map[string]float64, len(tasks))
	for _, t := range tasks {
		dataRateMap[t.Name] = parseDataRate(t.DataRate)
	}

	// Successor map for refinement-phase cost evaluation.
	successors := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		for _, dep := range t.Dependencies {
			successors[dep] = append(successors[dep], t.Name)
		}
	}

	// Candidate node set per task (constraint ∩ cluster).
	constraintMap := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		constraintMap[t.Name] = t.Constraints
	}
	nodeSet := make(map[string]bool, len(clusterNodes))
	for _, n := range clusterNodes {
		nodeSet[n] = true
	}
	candidatesFor := func(taskName string) []string {
		if len(constraintMap[taskName]) > 0 {
			var filtered []string
			for _, c := range constraintMap[taskName] {
				if nodeSet[c] {
					filtered = append(filtered, c)
				}
			}
			if len(filtered) > 0 {
				return filtered
			}
		}
		return clusterNodes
	}

	assigned := make(map[string]string, len(tasks))
	remaining := make(map[string]bool, len(tasks))
	for _, t := range tasks {
		remaining[t.Name] = true
	}
	flows := newFlowTracker()

	// maxUtilIfOn returns the max edge utilization (over committed edges
	// plus the hypothetical edges introduced by placing `t` on `node`),
	// using the current flowTracker state augmented with those hypothetical
	// flows.
	maxUtilIfOn := func(t cdagTaskSpec, node string) float64 {
		// Collect hypothetical new flows from this placement.
		type hyp struct{ src, dst string }
		var newFlows []hyp
		// Incoming from deps already placed.
		for _, dep := range t.Dependencies {
			depNode := assigned[dep]
			if depNode == "" || depNode == node || dataRateMap[dep] <= 0 {
				continue
			}
			newFlows = append(newFlows, hyp{src: depNode, dst: node})
		}
		// Outgoing to successors already placed.
		for _, succ := range successors[t.Name] {
			sNode := assigned[succ]
			if sNode == "" || sNode == node || dataRateMap[t.Name] <= 0 {
				continue
			}
			newFlows = append(newFlows, hyp{src: node, dst: sNode})
		}

		// Apply the hypothetical flows so effectiveBW sees them.
		for _, f := range newFlows {
			flows.addFlow(f.src, f.dst)
		}

		maxUtil := 0.0

		// Score every edge that would exist in the placed graph after
		// this hypothetical move: (a) edges among already-committed
		// placements, (b) the new flows we just added.
		scoreEdge := func(src, dst string, rate float64) {
			if rate <= 0 || src == "" || dst == "" || src == dst {
				return
			}
			eff := flows.effectiveBW(src, dst, 0, 0)
			if eff <= 0 {
				// treat a dead link as fully utilized to reject it
				maxUtil = math.Inf(1)
				return
			}
			u := rate / eff
			if u > maxUtil {
				maxUtil = u
			}
		}
		for _, u := range tasks {
			uNode := assigned[u.Name]
			if u.Name == t.Name {
				uNode = node
			}
			if uNode == "" {
				continue
			}
			for _, dep := range u.Dependencies {
				depNode := assigned[dep]
				if dep == t.Name {
					depNode = node
				}
				if depNode == "" {
					continue
				}
				scoreEdge(depNode, uNode, dataRateMap[dep])
			}
		}

		// Undo the hypothetical flows.
		for _, f := range newFlows {
			flows.removeFlow(f.src, f.dst)
		}
		return maxUtil
	}

	// sumCostOnPlacement: tie-break metric matching locality.
	sumCostOnPlacement := func() float64 {
		c := 0.0
		for _, t := range tasks {
			tNode := assigned[t.Name]
			if tNode == "" {
				continue
			}
			for _, dep := range t.Dependencies {
				depNode := assigned[dep]
				if depNode == "" || depNode == tNode || dataRateMap[dep] <= 0 {
					continue
				}
				eff := flows.effectiveBW(depNode, tNode, 0, 0)
				if eff > 0 {
					c += dataRateMap[dep] / eff
				}
			}
		}
		return c
	}

	// --------------------------------------------------------------
	// Phase 1: greedy topological placement.
	// --------------------------------------------------------------
	for len(remaining) > 0 {
		progress := false
		for _, t := range tasks {
			if !remaining[t.Name] {
				continue
			}
			allDepsAssigned := true
			for _, dep := range t.Dependencies {
				if _, ok := assigned[dep]; !ok {
					allDepsAssigned = false
					break
				}
			}
			if !allDepsAssigned {
				continue
			}

			candidates := candidatesFor(t.Name)
			bestNode := candidates[0]
			bestMaxUtil := math.Inf(1)

			for _, n := range candidates {
				u := maxUtilIfOn(t, n)
				if u < bestMaxUtil {
					bestMaxUtil = u
					bestNode = n
				}
			}

			assigned[t.Name] = bestNode
			for _, dep := range t.Dependencies {
				depNode := assigned[dep]
				if depNode != bestNode && dataRateMap[dep] > 0 {
					flows.addFlow(depNode, bestNode)
				}
			}
			delete(remaining, t.Name)
			progress = true
		}
		if !progress {
			// Dependency cycle or unreachable; fall back to random for the rest.
			for _, t := range tasks {
				if remaining[t.Name] {
					assigned[t.Name] = pickNode(t.Constraints, clusterNodes)
					delete(remaining, t.Name)
				}
			}
		}
	}

	// --------------------------------------------------------------
	// Phase 2: swap refinement. For each task, try every candidate; keep
	// the node with the lowest (maxUtil, sumCost) pair. Iterate until
	// nothing moves.
	// --------------------------------------------------------------
	currentMaxUtil := func() float64 {
		u := 0.0
		for _, t := range tasks {
			tNode := assigned[t.Name]
			if tNode == "" {
				continue
			}
			for _, dep := range t.Dependencies {
				depNode := assigned[dep]
				if depNode == "" || depNode == tNode || dataRateMap[dep] <= 0 {
					continue
				}
				eff := flows.effectiveBW(depNode, tNode, 0, 0)
				if eff <= 0 {
					return math.Inf(1)
				}
				e := dataRateMap[dep] / eff
				if e > u {
					u = e
				}
			}
		}
		return u
	}

	const maxIters = 10
	for iter := 0; iter < maxIters; iter++ {
		improved := false
		for _, t := range tasks {
			candidates := candidatesFor(t.Name)
			if len(candidates) <= 1 {
				continue
			}
			curNode := assigned[t.Name]
			// Remove flows owned by this task from the tracker.
			for _, dep := range t.Dependencies {
				depNode := assigned[dep]
				if depNode != curNode && dataRateMap[dep] > 0 {
					flows.removeFlow(depNode, curNode)
				}
			}
			for _, succ := range successors[t.Name] {
				sNode := assigned[succ]
				if sNode != curNode && dataRateMap[t.Name] > 0 {
					flows.removeFlow(curNode, sNode)
				}
			}

			bestNode := curNode
			// Start the best at curNode's state.
			assigned[t.Name] = curNode
			for _, dep := range t.Dependencies {
				depNode := assigned[dep]
				if depNode != curNode && dataRateMap[dep] > 0 {
					flows.addFlow(depNode, curNode)
				}
			}
			for _, succ := range successors[t.Name] {
				sNode := assigned[succ]
				if sNode != curNode && dataRateMap[t.Name] > 0 {
					flows.addFlow(curNode, sNode)
				}
			}
			bestMaxU := currentMaxUtil()
			bestSum := sumCostOnPlacement()
			// Then tear down curNode's flows to try alternatives cleanly.
			for _, dep := range t.Dependencies {
				depNode := assigned[dep]
				if depNode != curNode && dataRateMap[dep] > 0 {
					flows.removeFlow(depNode, curNode)
				}
			}
			for _, succ := range successors[t.Name] {
				sNode := assigned[succ]
				if sNode != curNode && dataRateMap[t.Name] > 0 {
					flows.removeFlow(curNode, sNode)
				}
			}

			for _, n := range candidates {
				if n == curNode {
					continue
				}
				assigned[t.Name] = n
				for _, dep := range t.Dependencies {
					depNode := assigned[dep]
					if depNode != n && dataRateMap[dep] > 0 {
						flows.addFlow(depNode, n)
					}
				}
				for _, succ := range successors[t.Name] {
					sNode := assigned[succ]
					if sNode != n && dataRateMap[t.Name] > 0 {
						flows.addFlow(n, sNode)
					}
				}
				mu := currentMaxUtil()
				sc := sumCostOnPlacement()
				// Lexicographic: lower maxUtil first, tiebreak by sum-cost.
				if mu < bestMaxU-1e-9 || (math.Abs(mu-bestMaxU) < 1e-9 && sc < bestSum-1e-9) {
					bestMaxU = mu
					bestSum = sc
					bestNode = n
				}
				for _, dep := range t.Dependencies {
					depNode := assigned[dep]
					if depNode != n && dataRateMap[dep] > 0 {
						flows.removeFlow(depNode, n)
					}
				}
				for _, succ := range successors[t.Name] {
					sNode := assigned[succ]
					if sNode != n && dataRateMap[t.Name] > 0 {
						flows.removeFlow(n, sNode)
					}
				}
			}

			// Commit bestNode.
			assigned[t.Name] = bestNode
			for _, dep := range t.Dependencies {
				depNode := assigned[dep]
				if depNode != bestNode && dataRateMap[dep] > 0 {
					flows.addFlow(depNode, bestNode)
				}
			}
			for _, succ := range successors[t.Name] {
				sNode := assigned[succ]
				if sNode != bestNode && dataRateMap[t.Name] > 0 {
					flows.addFlow(bestNode, sNode)
				}
			}
			if bestNode != curNode {
				improved = true
				log.Printf("[throughput] refinement: moved %s from %s to %s (maxUtil=%.3f)",
					t.Name, curNode, bestNode, bestMaxU)
			}
		}
		if !improved {
			break
		}
	}

	// --------------------------------------------------------------
	// Log final placement and flag any saturated/overloaded edges.
	// --------------------------------------------------------------
	finalMax := currentMaxUtil()
	log.Printf("[throughput] final maxUtil=%.3f  sumCost=%.4f", finalMax, sumCostOnPlacement())
	log.Printf("[throughput] task placement:")
	for _, t := range tasks {
		log.Printf("[throughput]   %-20s -> %s", t.Name, assigned[t.Name])
	}
	for _, t := range tasks {
		tNode := assigned[t.Name]
		for _, dep := range t.Dependencies {
			depNode := assigned[dep]
			if depNode == "" || depNode == tNode || dataRateMap[dep] <= 0 {
				continue
			}
			eff := flows.effectiveBW(depNode, tNode, 0, 0)
			if eff <= 0 {
				continue
			}
			util := dataRateMap[dep] / eff
			if util > 0.8 {
				log.Printf("[throughput]   edge %s->%s (%s->%s): rate=%.1fKB/s eff_bw=%.0fMB/s util=%.2f%s",
					dep, t.Name, depNode, tNode,
					dataRateMap[dep]/1000, eff/1e6, util,
					map[bool]string{true: " SATURATED", false: ""}[util >= 1.0])
			}
		}
	}
	if finalMax >= 1.0 {
		log.Printf("[throughput] WARNING: pipeline cannot sustain source rate — bottleneck edge is saturated (util=%.2f)", finalMax)
	}

	return assigned
}
