# HEFT v2: Resource-Aware Parallel Scheduling

## Problem

Current HEFT assumes tasks on the same node execute sequentially — each task waits for the previous one to finish before starting. But k3s actually runs pods in parallel as long as the node has sufficient CPU/memory. This means:

1. **Makespan is over-predicted**: HEFT thinks task B waits for task A on the same node, but in reality they run simultaneously if independent
2. **Placement decisions are suboptimal**: HEFT avoids packing tasks on the same node (thinks it causes queuing), when actually parallel execution on a powerful node may be faster than spreading across slow links
3. **Gantt chart is wrong**: shows tasks stacked sequentially when they actually overlap

## Current HEFT behavior

```
Node anrg-3:  [task-A (3s)]──[task-B (5s)]──[task-C (2s)]
              0s            3s             8s            10s
```

HEFT computes: `EST(B, anrg-3) = max(nodeAvail[anrg-3], deps_ready) = max(3, ...) = 3`

## What actually happens in k3s

```
Node anrg-3:  [task-A (3s)]
              [task-B (5s)]        ← starts at same time if no dependency
              [task-C (2s)]
              0s         3s  5s
```

If A, B, C are independent and the node has enough CPU/memory, k3s schedules all three simultaneously. Real makespan = max(3, 5, 2) = 5s, not 10s.

## Proposed HEFT v2 algorithm

### New inputs needed
- **Per-node available resources**: CPU (millicores) and memory (bytes) from k8s Node API
- **Per-task resource requests**: CPU and memory from task spec (already in CRD)

### Key change: `nodeAvail` tracks resources, not time

**Current**: `nodeAvail[node]` = time when the node becomes free (scalar)

**New**: `nodeAvail[node]` = list of (start, end, cpu, mem) intervals of committed resources

### Scheduling logic

For each task T being placed on candidate node N:

```
1. Get task's resource request: (T.cpu, T.mem)
2. Get node's total allocatable: (N.cpu, N.mem)  ← from k8s API
3. For each time point, compute available resources = total - sum(committed)
4. EST(T, N) = earliest time where:
   a. All dependencies of T have completed (+ transfer time if cross-node)
   b. Node N has enough free resources (T.cpu ≤ avail_cpu AND T.mem ≤ avail_mem)
5. If T has NO dependency on other tasks on N → can start in parallel
6. EFT(T, N) = EST(T, N) + runtime(T)
7. Commit T's resources on N for interval [EST, EFT]
```

### Example

Node anrg-3 has 4000m CPU. Three independent tasks each need 1000m:

```
Current HEFT:
  task-A: EST=0, EFT=3  → nodeAvail=3
  task-B: EST=3, EFT=8  → nodeAvail=8  (WRONG: waits for A)
  task-C: EST=8, EFT=10 → nodeAvail=10

HEFT v2:
  task-A: EST=0, EFT=3, commits 1000m/4000m
  task-B: EST=0, EFT=5, commits 1000m/4000m (parallel! 2000m/4000m used)
  task-C: EST=0, EFT=2, commits 1000m/4000m (parallel! 3000m/4000m used)
  Makespan = 5s (not 10s)
```

### When tasks CAN'T be parallel

- Task B depends on task A → B must wait for A regardless of resources
- Node has 2000m CPU, A needs 1500m, B needs 1000m → B waits until A finishes (1500+1000 > 2000)

## Implementation plan

### 1. Query node resources: `cmd/odag-controller/main.go`
- Use k8s Node API: `node.Status.Allocatable` gives total CPU/mem
- Use k8s Pods API: sum resource requests of running pods to get current usage
- Or simpler: `kubectl top nodes` equivalent via metrics API
- Store as `nodeResources map[string]Resources{CPU int64, Mem int64}`

### 2. Update HEFT: `cmd/odag-controller/heft.go`
- Replace `nodeAvail map[string]float64` with resource timeline
- New struct: `type nodeTimeline struct { slots []timeSlot }`
- `type timeSlot struct { start, end float64; cpuUsed, memUsed int64 }`
- `findEarliestSlot(node, taskCPU, taskMem, depsReady) → float64`

### 3. Update predicted schedule: `cmd/odag-controller/schedule.go`
- `computePredictedSchedule()` should use same parallel logic
- Output overlapping time ranges for tasks on same node

### 4. Update UI Gantt chart
- Tasks on same node shown as overlapping bars (stacked vertically within the node row)
- Dashed horizontal lines between node rows for visual separation
- Wider x-axis to accommodate parallel tasks

## Data sources for node resources

```go
// From k8s API:
node.Status.Allocatable["cpu"]    // e.g. "4000m" or "4"
node.Status.Allocatable["memory"] // e.g. "16Gi"

// Already in task spec:
task.Resources.CPU    // e.g. "500m"
task.Resources.Memory // e.g. "256Mi"
```

The odag-controller already calls `getNodeInfoMap()` which lists nodes. We just need to also read `.Status.Allocatable` from each node.

## Risks / considerations

- **Resource contention**: Even if CPU is available, tasks may compete for memory bandwidth, cache, etc. Parallel execution on same node may be slower per-task than sequential. HEFT v2 assumes no interference — could add a "slowdown factor" for co-located tasks.
- **Pod startup overhead**: k3s takes 1-3s to pull image and start container. HEFT v2 should account for this fixed overhead.
- **Dynamic resource usage**: Other pods (system, data-agent) consume resources. Should subtract system overhead from allocatable.
