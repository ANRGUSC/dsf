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

// localityAssignTasks assigns CDAG tasks to nodes by minimizing the total
// communication cost between dependent tasks. For each task in topological
// order, it picks the node where the sum of (depDataRate / bandwidth) is
// minimized. Same-node placement has zero cost.
func localityAssignTasks(tasks []cdagTaskSpec, clusterNodes []string) map[string]string {
	// Build data rate map.
	dataRateMap := make(map[string]float64, len(tasks))
	for _, t := range tasks {
		dataRateMap[t.Name] = parseDataRate(t.DataRate)
	}

	// Build dependency list per task.
	deps := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		deps[t.Name] = t.Dependencies
	}

	// Build constraint sets.
	constraintMap := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		constraintMap[t.Name] = t.Constraints
	}

	// Topological sort (simple: process in BFS layer order).
	assigned := make(map[string]string, len(tasks))
	remaining := make(map[string]bool, len(tasks))
	for _, t := range tasks {
		remaining[t.Name] = true
	}

	nodeSet := make(map[string]bool, len(clusterNodes))
	for _, n := range clusterNodes {
		nodeSet[n] = true
	}

	for len(remaining) > 0 {
		progress := false
		for _, t := range tasks {
			if !remaining[t.Name] {
				continue
			}
			// Check all deps are assigned.
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

			// Compute candidates.
			candidates := clusterNodes
			if len(constraintMap[t.Name]) > 0 {
				var filtered []string
				for _, c := range constraintMap[t.Name] {
					if nodeSet[c] {
						filtered = append(filtered, c)
					}
				}
				if len(filtered) > 0 {
					candidates = filtered
				}
			}

			// Pick node with lowest communication cost.
			bestNode := candidates[0]
			bestCost := math.MaxFloat64

			for _, node := range candidates {
				cost := 0.0
				for _, dep := range t.Dependencies {
					depNode := assigned[dep]
					rate := dataRateMap[dep]
					if rate <= 0 {
						continue // unknown rate — no penalty
					}
					bw := getBandwidth(depNode, node)
					if depNode == node {
						// Same node — zero network cost.
						continue
					}
					cost += rate / bw
				}
				if cost < bestCost {
					bestCost = cost
					bestNode = node
				}
			}

			assigned[t.Name] = bestNode
			delete(remaining, t.Name)
			progress = true
		}
		if !progress {
			// Circular dependency or bug — assign remaining randomly.
			for _, t := range tasks {
				if remaining[t.Name] {
					assigned[t.Name] = pickNode(t.Constraints, clusterNodes)
					delete(remaining, t.Name)
				}
			}
		}
	}

	log.Printf("[locality] task placement:")
	for _, t := range tasks {
		log.Printf("[locality]   %-20s -> %s", t.Name, assigned[t.Name])
	}

	return assigned
}
