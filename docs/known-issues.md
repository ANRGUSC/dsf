# Known Issues & Future Work

## Profiler instability with parallel tasks on same node

**Issue**: When multiple independent tasks run concurrently on the same node, they compete for CPU/memory/IO. The profiler records each task's wall-clock runtime, but this runtime is inflated by resource contention from co-located tasks. On the next run, HEFT uses these inflated runtimes and may make different placement decisions, leading to different contention patterns and different runtimes — creating instability in the EMA profiles.

**Example**: task-a alone on anrg-3 takes 5s. But when task-a, task-b, task-c, task-d all run on anrg-3 simultaneously, task-a takes 8s due to CPU contention. The profiler records 8s. Next run, HEFT spreads tasks across nodes (because 8s looks expensive), task-a runs alone and takes 5s. Profiler EMA adjusts toward 5s. Cycle repeats.

**Possible fixes**:
- Record CPU time (not wall-clock) via `/proc/[pid]/stat` or container metrics
- Discount observed runtime by a contention factor based on co-located task count
- Use the resource-aware HEFT v2 timeline to predict contention and adjust expected runtime accordingly
- Separate "solo runtime" from "contended runtime" in profiler DB

**Priority**: Medium — affects HEFT accuracy for dense DAGs on small clusters. Less of an issue when tasks have dedicated nodes or when the cluster has many nodes relative to DAG width.

---

## Stale local controller processes

**Issue**: If a developer runs `./odag-controller` locally for testing and forgets to kill it, it races with the in-cluster controller on the same CRDs. Both watch ODAG events, both deploy pods, both write status updates. This causes predicted schedules to be overwritten, duplicate pod creation attempts, and confusing mismatches between predicted and actual placements.

**Mitigation**: Before running experiments, always check: `pgrep -af odag-controller` and kill any local processes. Consider adding leader election to the controller so only one instance is active.

---

## NFS scalability eval not yet working

**Issue**: The `shared_volume` transport bypasses the data-agent, so the odag-controller never sees DataReady state and downstream tasks never get scheduled. The NFS overlay approach (mounting NFS over `/data/dsf-outputs`) is implemented but not yet tested end-to-end.

**Status**: Setup/teardown scripts ready (`eval/scalability/setup-nfs-overlay.sh`, `teardown-nfs-overlay.sh`). Need to verify the NFS overlay + data-agent combination works, then run the ODAG scalability eval.

---

## CDAG profiling not implemented

**Issue**: CDAGs are continuous — there's no completion event to trigger profiling. The locality scheduler uses static `dataRate` hints from the template spec. There's no online learning of actual data rates.

**Possible fix**: Periodically sample ZMQ socket throughput or add a metrics endpoint to the SDK that reports bytes/sec per peer. Feed this back to the cdag-controller for adaptive re-scheduling.

---

## Gantt chart doesn't show data transfer time

**Issue**: The current Gantt chart shows task execution bars but not the data transfer intervals between tasks. For network-aware scheduling, visualizing when data transfers happen and how long they take would help users understand the scheduling decisions.

**Possible fix**: Add thin colored bars between task bars showing the transfer duration (dataSize / bandwidth) for cross-node transfers. Same-node transfers would show as zero-width.
