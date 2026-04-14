package main

import (
	"math"
	"testing"
)

const testEps = 0.01

func approxEqual(a, b, tol float64) bool {
	return math.Abs(a-b) < tol
}

func constBW(bw float64) func(string, string) float64 {
	return func(src, dst string) float64 {
		if src == dst {
			return 0
		}
		return bw
	}
}

// ---------------------------------------------------------------------------
// simulateTransfers unit tests
// ---------------------------------------------------------------------------

func TestSimulate_SingleTransfer(t *testing.T) {
	nt := &networkTimeline{}
	results := nt.simulateTransfers([]pendingTransfer{
		{srcNode: "A", dstNode: "B", start: 0, dataSize: 100_000_000, taskName: "t1"},
	}, constBW(100e6))

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	// 100 MB / 100 MB/s = 1.0 s
	if !approxEqual(results[0].end, 1.0, testEps) {
		t.Errorf("end=%.3f, want 1.000", results[0].end)
	}
}

func TestSimulate_FanIn(t *testing.T) {
	nt := &networkTimeline{}
	// Two sources -> same dest: ingress contention, each gets BW/2.
	results := nt.simulateTransfers([]pendingTransfer{
		{srcNode: "A", dstNode: "C", start: 0, dataSize: 100_000_000, taskName: "t1"},
		{srcNode: "B", dstNode: "C", start: 0, dataSize: 100_000_000, taskName: "t2"},
	}, constBW(100e6))

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	// Each: 100 MB / 50 MB/s = 2.0 s
	for i, r := range results {
		if !approxEqual(r.end, 2.0, testEps) {
			t.Errorf("result[%d] end=%.3f, want 2.000", i, r.end)
		}
	}
}

func TestSimulate_FanOut(t *testing.T) {
	nt := &networkTimeline{}
	// Same source -> two dests: egress contention, each gets BW/2.
	results := nt.simulateTransfers([]pendingTransfer{
		{srcNode: "A", dstNode: "B", start: 0, dataSize: 100_000_000, taskName: "t1"},
		{srcNode: "A", dstNode: "C", start: 0, dataSize: 100_000_000, taskName: "t2"},
	}, constBW(100e6))

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	for i, r := range results {
		if !approxEqual(r.end, 2.0, testEps) {
			t.Errorf("result[%d] end=%.3f, want 2.000", i, r.end)
		}
	}
}

func TestSimulate_StaggeredOverlap(t *testing.T) {
	nt := &networkTimeline{}
	// A->C: 100 MB at t=0, B->C: 50 MB at t=0.5. BW=100 MB/s.
	// [0, 0.5): A alone at 100 MB/s -> 50 MB sent, 50 MB left.
	// [0.5, 1.5): A+B share ingress at 50 MB/s -> both finish at 1.5.
	results := nt.simulateTransfers([]pendingTransfer{
		{srcNode: "A", dstNode: "C", start: 0.0, dataSize: 100_000_000, taskName: "t1"},
		{srcNode: "B", dstNode: "C", start: 0.5, dataSize: 50_000_000, taskName: "t2"},
	}, constBW(100e6))

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	for i, r := range results {
		if !approxEqual(r.end, 1.5, testEps) {
			t.Errorf("result[%d] end=%.3f, want 1.500", i, r.end)
		}
	}
}

func TestSimulate_UnequalSizes(t *testing.T) {
	nt := &networkTimeline{}
	// A->C: 50 MB, B->C: 100 MB, both at t=0. BW=100 MB/s.
	// Both at 50 MB/s. A finishes at 1.0 s.
	// Then B alone at 100 MB/s, 50 MB left -> 0.5 s -> finishes at 1.5 s.
	results := nt.simulateTransfers([]pendingTransfer{
		{srcNode: "A", dstNode: "C", start: 0, dataSize: 50_000_000, taskName: "t1"},
		{srcNode: "B", dstNode: "C", start: 0, dataSize: 100_000_000, taskName: "t2"},
	}, constBW(100e6))

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if !approxEqual(results[0].end, 1.0, testEps) {
		t.Errorf("result[0] (50MB) end=%.3f, want 1.000", results[0].end)
	}
	if !approxEqual(results[1].end, 1.5, testEps) {
		t.Errorf("result[1] (100MB) end=%.3f, want 1.500", results[1].end)
	}
}

func TestSimulate_CommittedFlowContention(t *testing.T) {
	nt := &networkTimeline{}
	// Pre-existing committed flow: A->X during [0, 2.0].
	nt.commitFlow("A", "X", "old", 0, 2.0)

	// New: A->Y, 100 MB at t=0.5, BW=100 MB/s.
	// [0.5, 2.0): egress shared -> 50 MB/s -> 75 MB sent, 25 MB left.
	// [2.0, 2.25): alone -> 100 MB/s -> 25 MB / 100 = 0.25 s.
	results := nt.simulateTransfers([]pendingTransfer{
		{srcNode: "A", dstNode: "Y", start: 0.5, dataSize: 100_000_000, taskName: "t1"},
	}, constBW(100e6))

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if !approxEqual(results[0].end, 2.25, testEps) {
		t.Errorf("end=%.3f, want 2.250", results[0].end)
	}
}

func TestSimulate_ZeroDataSize(t *testing.T) {
	nt := &networkTimeline{}
	results := nt.simulateTransfers([]pendingTransfer{
		{srcNode: "A", dstNode: "B", start: 5.0, dataSize: 0, taskName: "t1"},
	}, constBW(100e6))

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if !approxEqual(results[0].end, 5.0, testEps) {
		t.Errorf("end=%.3f, want 5.000", results[0].end)
	}
}

func TestSimulate_NoContention_DifferentPairs(t *testing.T) {
	nt := &networkTimeline{}
	// A->B and C->D: no shared nodes, no contention.
	results := nt.simulateTransfers([]pendingTransfer{
		{srcNode: "A", dstNode: "B", start: 0, dataSize: 100_000_000, taskName: "t1"},
		{srcNode: "C", dstNode: "D", start: 0, dataSize: 100_000_000, taskName: "t2"},
	}, constBW(100e6))

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	// No contention: each finishes at 1.0 s.
	for i, r := range results {
		if !approxEqual(r.end, 1.0, testEps) {
			t.Errorf("result[%d] end=%.3f, want 1.000", i, r.end)
		}
	}
}

// ---------------------------------------------------------------------------
// heftAssignTasks integration tests
// ---------------------------------------------------------------------------

func makeNodes(names ...string) map[string]nodeInfo {
	m := make(map[string]nodeInfo, len(names))
	for _, n := range names {
		m[n] = nodeInfo{name: n, cpuMillis: 4000, memBytes: 4 << 30}
	}
	return m
}

func heftMakespan(r heftResult) float64 {
	ms := 0.0
	for _, e := range r.schedule {
		if e.EstEnd > ms {
			ms = e.EstEnd
		}
	}
	return ms
}

func TestHeft_FanOut_Contention(t *testing.T) {
	// DAG: A -> B, A -> C (fan-out from A).
	// Each on a dedicated node. A produces 100 MB. BW = 100 MB/s.
	//
	// HEFT rank order: A first, then B and C (equal rank).
	// First child scheduled gets full BW (1.0 s transfer).
	// Second child's transfer contends at A's egress:
	//   [1,2) shared at 50 MB/s -> 50 MB. [2,2.5) alone -> 50 MB at 100 MB/s.
	// Without contention: makespan = 3.0. With contention: 3.5.
	tasks := []taskSpec{
		{Name: "A", Runtime: 1.0, DataSize: "100MB", Constraints: []string{"n1"}},
		{Name: "B", Runtime: 1.0, Dependencies: []string{"A"}, Constraints: []string{"n2"}},
		{Name: "C", Runtime: 1.0, Dependencies: []string{"A"}, Constraints: []string{"n3"}},
	}
	result := heftAssignTasks(tasks, makeNodes("n1", "n2", "n3"), nil, nil, constBW(100e6))
	ms := heftMakespan(result)

	if !approxEqual(ms, 3.5, testEps) {
		t.Errorf("fan-out makespan=%.3f, want 3.500", ms)
	}
}

func TestHeft_FanIn_Contention(t *testing.T) {
	// DAG: B -> D, C -> D (fan-in to D). B and C independent.
	// B on n2, C on n3, D on n4. B and C each output 100 MB. BW = 100 MB/s.
	//
	// B and C run in parallel [0,1]. Both transfer to D simultaneously.
	// Ingress contention at n4: each gets 50 MB/s -> 2.0 s each.
	// depsReady = 3.0. D runs [3,4].
	// Without contention: makespan = 3.0. With contention: 4.0.
	tasks := []taskSpec{
		{Name: "B", Runtime: 1.0, DataSize: "100MB", Constraints: []string{"n2"}},
		{Name: "C", Runtime: 1.0, DataSize: "100MB", Constraints: []string{"n3"}},
		{Name: "D", Runtime: 1.0, Dependencies: []string{"B", "C"}, Constraints: []string{"n4"}},
	}
	result := heftAssignTasks(tasks, makeNodes("n2", "n3", "n4"), nil, nil, constBW(100e6))
	ms := heftMakespan(result)

	if !approxEqual(ms, 4.0, testEps) {
		t.Errorf("fan-in makespan=%.3f, want 4.000", ms)
	}
}

func TestHeft_NoContention_Linear(t *testing.T) {
	// Linear DAG: A -> B -> C. No fan-out/in, so no contention.
	// Each task 1.0 s, 100 MB output, BW = 100 MB/s.
	// A [0,1], transfer 1 s, B [2,3], transfer 1 s, C [4,5].
	tasks := []taskSpec{
		{Name: "A", Runtime: 1.0, DataSize: "100MB", Constraints: []string{"n1"}},
		{Name: "B", Runtime: 1.0, DataSize: "100MB", Dependencies: []string{"A"}, Constraints: []string{"n2"}},
		{Name: "C", Runtime: 1.0, Dependencies: []string{"B"}, Constraints: []string{"n3"}},
	}
	result := heftAssignTasks(tasks, makeNodes("n1", "n2", "n3"), nil, nil, constBW(100e6))
	ms := heftMakespan(result)

	// Linear chain: no contention possible. makespan = 5.0.
	if !approxEqual(ms, 5.0, testEps) {
		t.Errorf("linear makespan=%.3f, want 5.000", ms)
	}
}

func TestHeft_SameNode_NoTransfer(t *testing.T) {
	// All tasks constrained to same node: no transfers at all.
	// A [0,1], B [1,2], C [2,3]. Sequential on one node.
	tasks := []taskSpec{
		{Name: "A", Runtime: 1.0, DataSize: "100MB", Constraints: []string{"n1"}},
		{Name: "B", Runtime: 1.0, Dependencies: []string{"A"}, Constraints: []string{"n1"}},
		{Name: "C", Runtime: 1.0, Dependencies: []string{"B"}, Constraints: []string{"n1"}},
	}
	result := heftAssignTasks(tasks, makeNodes("n1"), nil, nil, constBW(100e6))
	ms := heftMakespan(result)

	if !approxEqual(ms, 3.0, testEps) {
		t.Errorf("same-node makespan=%.3f, want 3.000", ms)
	}
}
