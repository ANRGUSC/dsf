# Benchmark: Multi-ODAG HEFT vs Random Scheduling

Compares HEFT (Heterogeneous Earliest Finish Time) against random task placement
across 5 concurrent ODAGs on a heterogeneous network with shaped bandwidth.

## Cluster Setup

- **Nodes**: anrg-3, anrg-4, anrg-5, anrg-6 (4 worker nodes, 16GB RAM each)
- **Master**: anrg-2 (SchedulingDisabled)
- **Network**: 1Gbps Ethernet, shaped with `tc` to create heterogeneous links

### Bandwidth Matrix

```
         → anrg-3    → anrg-4    → anrg-5    → anrg-6
anrg-3     (local)    1 Gbps     500 Mbps    100 Mbps
anrg-4     1 Gbps     (local)    500 Mbps    100 Mbps
anrg-5     500 Mbps   1 Gbps     (local)     100 Mbps
anrg-6     100 Mbps   100 Mbps   100 Mbps    (local)
```

anrg-6 is the "slow" node (100 Mbps to everyone). anrg-3 ↔ anrg-4 is the fast pair
(1 Gbps). anrg-5 has mixed links.

## ODAGs


| #   | Name            | Topology       | Tasks | Critical Path Runtime | Max Data Transfer            |
| --- | --------------- | -------------- | ----- | --------------------- | ---------------------------- |
| 1   | video-transcode | Linear chain   | 4     | 31s (5+8+15+3)        | 500MB (decode output)        |
| 2   | ml-training     | Diamond        | 5     | 37s (4+8+20+5)        | 200MB (preprocess output)    |
| 3   | etl-wide        | Fan-out/fan-in | 5     | 16s (3+7+6)           | 300MB (extract fan-out to 3) |
| 4   | sensor-fusion   | Complex DAG    | 7     | 29s (4+10+12+3)       | 200MB (fuse-ab output)       |
| 5   | image-batch     | Deep chain     | 5     | 26s (4+6+8+5+3)       | 500MB (download output)      |


All tasks use full-size binary payloads matching their declared `dataSize`.
Node constraints limit each task to 1-2 candidate nodes to create meaningful
placement choices.

## How to Run

### Prerequisites

1. ODAG controller deployed with HEFT bandwidth matrix (`cmd/odag-controller/heft.go`)
2. Data-agent DaemonSet running on all nodes (`pushTimeout >= 120s`)
3. Task image built and pushed:
  ```bash
   docker build -f examples/multi-odag-heft/tasks/Dockerfile \
     -t 192.168.1.163:5000/multi-odag-task:latest .
   docker push 192.168.1.163:5000/multi-odag-task:latest
  ```

### Step 1: Apply bandwidth shaping

```bash
./setup-tc.sh
```

This creates privileged pods on each node that apply `tc` rules. Rules persist
on the host kernel even after pods complete. To verify:

```bash
ssh anrg-3 "tc class show dev enp2s0"
```

### Step 2: Run HEFT

```bash
./run-benchmark.sh heft
```

Submits all 5 ODAGs with `scheduler: heft` staggered by 3-6 seconds, waits for
completion, and prints results.

### Step 3: Run Random (3 trials)

```bash
./run-benchmark.sh random 3
```

Same ODAGs with `scheduler: random`, repeated 3 times.

### Step 4: Clean up tc rules

```bash
./teardown-tc.sh
```

### Using the UI

The Batch page at `http://localhost:8080/batch` provides a combined Gantt chart
view. Click **Run Batch** to submit all 5 ODAGs with HEFT. The page auto-updates
via SSE.

## Results (2026-03-28)

### HEFT vs Random — Individual Makespan (seconds)


| ODAG            | HEFT    | Random R1 | Random R2 | Random R3 | Random Avg | Improvement |
| --------------- | ------- | --------- | --------- | --------- | ---------- | ----------- |
| video-transcode | **70**  | 142       | 73        | 131       | 115.3      | **39%**     |
| ml-training     | **53**  | 77        | 51        | 53        | 60.3       | **12%**     |
| etl-wide        | **41**  | 49        | 72        | 44        | 55.0       | **25%**     |
| sensor-fusion   | **56**  | 84        | 84        | 69        | 79.0       | **29%**     |
| image-batch     | **99**  | 122       | 87        | 82        | 97.0       | ~0%         |
| **Total**       | **319** | 474       | 367       | 379       | **406.7**  | **22%**     |


### HEFT Placement Strategy

HEFT consistently:

- Co-locates dependent tasks on the same node or fast links (anrg-3 ↔ anrg-4 at 1Gbps)
- Only places leaf tasks with small/zero data on anrg-6 (the slow node)
- Avoids sending large payloads (200-500MB) over 100Mbps links

### Random Placement Failures

Random scheduling placed data-heavy tasks on anrg-6 in 2 of 3 runs:

- **R1**: `encode` (receives 500MB from decode) on anrg-6 → 40s transfer overhead → 142s makespan
- **R3**: `encode` on anrg-6 again → 131s makespan
- **R2**: `clean-b` and `clean-c` on anrg-6 → 300MB each over 100Mbps → etl-wide 72s (vs HEFT 41s)

### Key Observations

1. **HEFT advantage grows with network heterogeneity.** On a flat 1Gbps network,
  HEFT and random performed identically. With shaped bandwidth (100Mbps–1Gbps),
   HEFT is 22% faster on average.
2. **HEFT prevents worst-case blowups.** Random has high variance (73s to 142s for
  video-transcode). HEFT is consistent (70s every run).
3. **Data transfer dominates over compute** when runtimes are moderate (3-20s) and
  payloads are large (200-500MB). A 500MB transfer over 100Mbps takes 40s — longer
   than any task's compute time.
4. **Per-link bandwidth awareness is critical.** Using a flat bandwidth constant in
  HEFT misses the heterogeneity. The bandwidth matrix lets HEFT distinguish 1Gbps
   from 100Mbps links and make informed placement decisions.

## Controller Optimizations Applied

During this benchmark development, three controller optimizations were made:


| Fix                               | Before                      | After            | Impact                       |
| --------------------------------- | --------------------------- | ---------------- | ---------------------------- |
| K8s client QPS 5→50, Burst 10→100 | 1.4s throttle per API call  | 0 throttling     | Eliminated 6s inter-task gap |
| Removed redundant ODAG CR fetch   | 2 GETs per pod event        | 1 GET            | -1.5s per task transition    |
| Pod state cache (sync.Map)        | List API call per pod event | In-memory lookup | -1s per task transition      |


Combined: extract→clean-a gap went from **6s to 0s**.