package main

import (
	"context"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// --------------------------------------------------------------------------
// Bandwidth ConfigMap cache (same pattern as odag-controller/bandwidth.go)
// --------------------------------------------------------------------------

const (
	defaultBandwidth     = 125_000_000.0 // 125 MB/s (1 Gbps)
	networkConfigMapName = "dsf-network-profile"
	networkConfigMapNS   = "dsf-system"
)

var (
	bwMu      sync.RWMutex
	bwCache   = make(map[string]float64) // "srcNode->dstNode" -> bytes/sec
	bwDefault = defaultBandwidth
)

// watchBandwidthConfigMap watches the dsf-network-profile ConfigMap and
// updates the in-memory bandwidth cache on every change.
func watchBandwidthConfigMap(client *kubernetes.Clientset) {
	loadBandwidthConfigMap(client)
	for {
		watcher, err := client.CoreV1().ConfigMaps(networkConfigMapNS).Watch(
			context.Background(), metav1.ListOptions{
				FieldSelector: "metadata.name=" + networkConfigMapName,
			},
		)
		if err != nil {
			log.Printf("[bandwidth] error watching ConfigMap: %v; retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("[bandwidth] watching dsf-network-profile ConfigMap")
		for event := range watcher.ResultChan() {
			if string(event.Type) == "DELETED" {
				bwMu.Lock()
				bwCache = make(map[string]float64)
				bwDefault = defaultBandwidth
				bwMu.Unlock()
				continue
			}
			loadBandwidthConfigMap(client)
		}
		time.Sleep(2 * time.Second)
	}
}

func loadBandwidthConfigMap(client *kubernetes.Clientset) {
	cm, err := client.CoreV1().ConfigMaps(networkConfigMapNS).Get(
		context.Background(), networkConfigMapName, metav1.GetOptions{},
	)
	if err != nil {
		log.Printf("[bandwidth] ConfigMap not found; using defaults")
		return
	}
	newCache := make(map[string]float64)
	newDefault := defaultBandwidth
	for key, val := range cm.Data {
		if key == "defaultBandwidth" {
			if f, err := strconv.ParseFloat(val, 64); err == nil && f > 0 {
				newDefault = f
			}
			continue
		}
		if !strings.Contains(key, "_to_") {
			continue
		}
		internalKey := strings.Replace(key, "_to_", "->", 1)
		if f, err := strconv.ParseFloat(val, 64); err == nil && f > 0 {
			newCache[internalKey] = f
		}
	}
	bwMu.Lock()
	bwCache = newCache
	bwDefault = newDefault
	bwMu.Unlock()
	log.Printf("[bandwidth] loaded %d link entries (default: %.0f B/s)", len(newCache), newDefault)
}

func getBandwidth(src, dst string) float64 {
	if src == dst {
		return math.MaxFloat64 // same node — effectively infinite (local)
	}
	bwMu.RLock()
	defer bwMu.RUnlock()
	if bw, ok := bwCache[src+"->"+dst]; ok {
		return bw
	}
	if bw, ok := bwCache[dst+"->"+src]; ok {
		return bw // symmetric
	}
	return bwDefault
}

// --------------------------------------------------------------------------
// Data rate parser
// --------------------------------------------------------------------------

// parseDataRate parses a data rate string like "1MB/s", "500KB/s" and returns bytes/sec.
func parseDataRate(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0
	}
	// Strip "/s" or "/sec" suffix.
	s = strings.TrimSuffix(s, "/sec")
	s = strings.TrimSuffix(s, "/s")
	// Now parse as a data size to get bytes.
	return float64(parseDataSize(s))
}

// parseDataSize parses "500KB", "1MB", "2GB" etc. to bytes.
func parseDataSize(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" {
		return 0
	}
	type entry struct {
		suffix string
		mult   int64
	}
	for _, e := range []entry{
		{"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
		{"GB", 1_000_000_000}, {"MB", 1_000_000}, {"KB", 1_000}, {"B", 1},
	} {
		if strings.HasSuffix(s, e.suffix) {
			numStr := strings.TrimSpace(s[:len(s)-len(e.suffix)])
			if v, err := strconv.ParseFloat(numStr, 64); err == nil {
				return int64(v * float64(e.mult))
			}
		}
	}
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v
	}
	return 0
}

// --------------------------------------------------------------------------
// Locality scheduler: minimize cross-node communication cost
// --------------------------------------------------------------------------

// --------------------------------------------------------------------------
// Flow tracking for bandwidth contention
// --------------------------------------------------------------------------

// flowTracker counts active steady-state flows per node for contention.
type flowTracker struct {
	egress  map[string]int // node -> number of outgoing flows
	ingress map[string]int // node -> number of incoming flows
}

func newFlowTracker() *flowTracker {
	return &flowTracker{
		egress:  make(map[string]int),
		ingress: make(map[string]int),
	}
}

// addFlow records a new steady-state flow from src to dst.
func (ft *flowTracker) addFlow(src, dst string) {
	ft.egress[src]++
	ft.ingress[dst]++
}

// removeFlow removes a previously recorded flow.
func (ft *flowTracker) removeFlow(src, dst string) {
	ft.egress[src]--
	if ft.egress[src] <= 0 {
		delete(ft.egress, src)
	}
	ft.ingress[dst]--
	if ft.ingress[dst] <= 0 {
		delete(ft.ingress, dst)
	}
}

// effectiveBW returns the bandwidth a flow from src to dst would get,
// accounting for TCP fair-sharing at both the sender's egress and
// receiver's ingress. extraEgress/extraIngress count flows being
// evaluated but not yet committed (e.g., other deps of the same task).
func (ft *flowTracker) effectiveBW(src, dst string, extraEgress, extraIngress int) float64 {
	rawBW := getBandwidth(src, dst)
	nE := ft.egress[src] + 1 + extraEgress  // +1 for this flow
	nI := ft.ingress[dst] + 1 + extraIngress // +1 for this flow
	return min(rawBW/float64(nE), rawBW/float64(nI))
}

// localityAssignTasks assigns CDAG tasks to nodes by minimizing the total
// communication cost between dependent tasks, accounting for TCP fair-sharing
// of bandwidth when multiple flows share a node's egress or ingress.
//
// For each task in topological order, it picks the node where the
// contention-adjusted cost is minimized. After initial placement, a
// refinement pass checks if swapping any task to a different node
// reduces the total system cost.
func localityAssignTasks(tasks []cdagTaskSpec, clusterNodes []string) map[string]string {
	// Build data rate map.
	dataRateMap := make(map[string]float64, len(tasks))
	for _, t := range tasks {
		dataRateMap[t.Name] = parseDataRate(t.DataRate)
	}

	// Build dependency and successor maps.
	deps := make(map[string][]string, len(tasks))
	successors := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		deps[t.Name] = t.Dependencies
		for _, dep := range t.Dependencies {
			successors[dep] = append(successors[dep], t.Name)
		}
	}

	// Build constraint sets.
	constraintMap := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		constraintMap[t.Name] = t.Constraints
	}

	nodeSet := make(map[string]bool, len(clusterNodes))
	for _, n := range clusterNodes {
		nodeSet[n] = true
	}

	// candidatesFor returns the allowed nodes for a task.
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

	// ----------------------------------------------------------------
	// Phase 1: greedy placement in topological order with progressive
	// contention awareness.
	// ----------------------------------------------------------------
	assigned := make(map[string]string, len(tasks))
	remaining := make(map[string]bool, len(tasks))
	for _, t := range tasks {
		remaining[t.Name] = true
	}
	flows := newFlowTracker()

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
			bestCost := math.MaxFloat64

			for _, node := range candidates {
				cost := 0.0
				// Count how many of this task's deps would share the
				// same ingress node, so we account for self-contention
				// among sibling flows.
				ingressPeers := 0
				for _, dep := range t.Dependencies {
					depNode := assigned[dep]
					if depNode != node && dataRateMap[dep] > 0 {
						ingressPeers++
					}
				}

				peersSeen := 0
				for _, dep := range t.Dependencies {
					depNode := assigned[dep]
					rate := dataRateMap[dep]
					if rate <= 0 || depNode == node {
						continue
					}
					// extraIngress = remaining sibling flows not yet counted.
					extraIngress := ingressPeers - peersSeen - 1
					effBW := flows.effectiveBW(depNode, node, 0, extraIngress)
					cost += rate / effBW
					peersSeen++
				}
				if cost < bestCost {
					bestCost = cost
					bestNode = node
				}
			}

			// Commit placement and flows.
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
			for _, t := range tasks {
				if remaining[t.Name] {
					assigned[t.Name] = pickNode(t.Constraints, clusterNodes)
					delete(remaining, t.Name)
				}
			}
		}
	}

	// ----------------------------------------------------------------
	// Phase 2: refinement — with all flows known, check if swapping
	// any single task to a different node reduces total system cost.
	// ----------------------------------------------------------------
	totalCost := func() float64 {
		c := 0.0
		for _, t := range tasks {
			for _, dep := range t.Dependencies {
				depNode := assigned[dep]
				tNode := assigned[t.Name]
				rate := dataRateMap[dep]
				if rate <= 0 || depNode == tNode {
					continue
				}
				bw := getBandwidth(depNode, tNode)
				nE := float64(flows.egress[depNode])
				nI := float64(flows.ingress[tNode])
				if nE < 1 {
					nE = 1
				}
				if nI < 1 {
					nI = 1
				}
				effBW := min(bw/nE, bw/nI)
				c += rate / effBW
			}
		}
		return c
	}

	const maxRefinementIters = 10
	for iter := 0; iter < maxRefinementIters; iter++ {
		improved := false
		for _, t := range tasks {
			candidates := candidatesFor(t.Name)
			if len(candidates) <= 1 {
				continue
			}
			curNode := assigned[t.Name]
			curCost := totalCost()

			// Remove current flows for this task.
			for _, dep := range t.Dependencies {
				depNode := assigned[dep]
				if depNode != curNode && dataRateMap[dep] > 0 {
					flows.removeFlow(depNode, curNode)
				}
			}
			for _, succ := range successors[t.Name] {
				succNode := assigned[succ]
				if succNode != curNode && dataRateMap[t.Name] > 0 {
					flows.removeFlow(curNode, succNode)
				}
			}

			bestNode := curNode
			bestCost := curCost

			for _, node := range candidates {
				if node == curNode {
					continue
				}
				// Tentatively add flows for this candidate.
				for _, dep := range t.Dependencies {
					depNode := assigned[dep]
					if depNode != node && dataRateMap[dep] > 0 {
						flows.addFlow(depNode, node)
					}
				}
				for _, succ := range successors[t.Name] {
					succNode := assigned[succ]
					if succNode != node && dataRateMap[t.Name] > 0 {
						flows.addFlow(node, succNode)
					}
				}

				assigned[t.Name] = node
				c := totalCost()
				if c < bestCost {
					bestCost = c
					bestNode = node
				}

				// Undo tentative flows.
				for _, dep := range t.Dependencies {
					depNode := assigned[dep]
					if depNode != node && dataRateMap[dep] > 0 {
						flows.removeFlow(depNode, node)
					}
				}
				for _, succ := range successors[t.Name] {
					succNode := assigned[succ]
					if succNode != node && dataRateMap[t.Name] > 0 {
						flows.removeFlow(node, succNode)
					}
				}
			}

			// Re-add flows for the best node.
			assigned[t.Name] = bestNode
			for _, dep := range t.Dependencies {
				depNode := assigned[dep]
				if depNode != bestNode && dataRateMap[dep] > 0 {
					flows.addFlow(depNode, bestNode)
				}
			}
			for _, succ := range successors[t.Name] {
				succNode := assigned[succ]
				if succNode != bestNode && dataRateMap[t.Name] > 0 {
					flows.addFlow(bestNode, succNode)
				}
			}

			if bestNode != curNode {
				improved = true
				log.Printf("[locality] refinement: moved %s from %s to %s (cost %.4f -> %.4f)",
					t.Name, curNode, bestNode, curCost, bestCost)
			}
		}
		if !improved {
			break
		}
	}

	// Log final placement and contention.
	log.Printf("[locality] task placement:")
	for _, t := range tasks {
		log.Printf("[locality]   %-20s -> %s", t.Name, assigned[t.Name])
	}
	for _, t := range tasks {
		for _, dep := range t.Dependencies {
			depNode := assigned[dep]
			tNode := assigned[t.Name]
			rate := dataRateMap[dep]
			if rate <= 0 || depNode == tNode {
				continue
			}
			rawBW := getBandwidth(depNode, tNode)
			nE := flows.egress[depNode]
			nI := flows.ingress[tNode]
			effBW := min(rawBW/float64(nE), rawBW/float64(nI))
			if effBW < rawBW*0.99 {
				log.Printf("[locality]   flow %s->%s (%s->%s): %.1fKB/s raw_bw=%.0fMB/s eff_bw=%.0fMB/s (egress=%d ingress=%d)",
					dep, t.Name, depNode, tNode,
					rate/1000, rawBW/1e6, effBW/1e6, nE, nI)
			}
		}
	}

	return assigned
}
