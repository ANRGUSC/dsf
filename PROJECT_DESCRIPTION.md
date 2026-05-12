# DSF — Project Description for Expert Review

> A briefing document intended to give a domain expert enough context to
> co-author a paper. It covers (1) what we built and why, (2) the
> architecture, (3) the scheduling contributions, (4) experimental
> setup, and (5) the results we currently have. Pointers to relevant
> files and figures are included at every step.

**Working title (current):**
*DSF: A Network-Aware DAG Scheduling Framework for Heterogeneous Edge Clusters*

**Repo root:** `/home/anrg/dsf` (Git branch `develop-new`).
**Cluster:** 8 schedulable nodes (`anrg-1`, `anrg-3..9`) on k3s, plus
master `anrg-2` (SchedulingDisabled). Local container registry at
`192.168.1.163:5000`.

---

## 1. Purpose & Problem Statement

### Why this project exists

Modern containerised workflow systems (Argo Workflows, Apache Airflow,
Tekton, Prefect, Nextflow, Pegasus) were designed for **cloud**
environments with:

- Abundant, flat, high-bandwidth networks.
- Centralized shared storage (S3, NFS, GridFTP) that every node can
  reach cheaply.
- Homogeneous nodes whose only differentiating axis is "more or fewer
  of the same kind of CPU."

At the **edge** — IoBT clusters, on-premises ML, smart facilities,
research deployments — none of these assumptions hold. Nodes are
heterogeneous (some carry sensors, some carry GPUs, some are
storage-poor), links between node pairs differ by an order of
magnitude in bandwidth, and shared storage is either absent, expensive,
or itself the bottleneck. Cloud-derived workflow systems on edge
clusters therefore exhibit three structural inefficiencies:

1. **Two-hop penalty.** Every inter-task transfer goes
   `task → shared store → task`, even when the producing and consuming
   tasks land on the same node.
2. **Task/IO coupling.** Producer pods stay alive (holding CPU and
   memory) until their upload to the store completes — wasted resources
   when compute is already done.
3. **Network is invisible.** The scheduler does not know which links
   are fast and which are slow, so placement is bandwidth-blind.
   "Data is ready" is treated as a single global event, when in reality
   data is ready on a *specific node* at a *specific time*.

### The DSF angle

DSF (Distributed Scheduling Framework) is a Kubernetes-native
(k3s-targeted) DAG orchestration system that treats the **network as a
first-class scheduling input** and replaces shared-storage data movement
with **direct point-to-point transfer**. It supports both:

- **ODAG** — One-shot DAG (kind `ODAG`): finite pipeline, tasks run
  exactly once, the DAG succeeds or fails.
- **CDAG** — Continuous DAG (kind `CDAG`): streaming pipeline, tasks
  run forever and are restarted on failure.

Both models use the same SDK (`init → recv → send → close`), the same
CRD layout, the same controllers' design, the same CLI verbs (`dsf
odag …`, `dsf cdag …`), and the same UI. The user switches modes by
changing the YAML `kind`, not by rewriting task code.

Concise problem statement (paper framing):

> Task completion and data availability are two distinct events
> separated by a network transfer. Treating them as one — as all
> existing workflow systems do — wastes resources, ignores topology,
> and prevents compute-transfer overlap.

This is the MPI non-blocking-send insight applied at the container
orchestration layer, where the constants (slower startup, larger
payloads, heterogeneous links, container isolation) are different but
the principle is identical.

---

## 2. What We Built

The full system lives in `/home/anrg/dsf`. The pieces that exist today
and are running in-cluster:

| Component | Path | Role |
|---|---|---|
| ODAG CRD | `api/v1/odag-crd.yml` | `dsf.io/v1`, kind `ODAG` |
| CDAG CRD | `api/v1/cdag-crd.yml` | `dsf.io/v1`, kind `CDAG` |
| ODAGTemplate CRD | (in same files) | Reusable spec; controller materializes runs from it |
| CDAGTemplate CRD | (in same files) | Same for CDAGs |
| ODAG controller | `cmd/odag-controller/` (Go) | Watches ODAG CRs, places + creates pods + services, watches pods, writes status, runs HEFT |
| CDAG controller | `cmd/cdag-controller/` (Go) | Watches CDAG CRs, runs a 30 s reconcile loop, recreates failed pods, writes status |
| UI server | `cmd/ui-server/` (Go) | HTTP API + SQLite history + Server-Sent Events |
| `dsf` CLI | `cmd/cli/` (Go, cobra) | `dsf {odag,cdag} {submit,list,status,delete,logs,run}` |
| Python SDK | `sdk/python/dsf_sdk/` | `DSFTask` user API + ZMQ transports (PUSH/PULL, PUB/SUB) + router |
| HEFT scheduler | `pkg/scheduler/heft.go` + controller-side code | Network-aware HEFT with online profiler |
| Data-agent | `data-agent` binary (built artifact) | Node-local sidecar for ODAG large-data transfers (file-based) |
| React frontend | `ui/` | List + detail pages, React Flow graph, Recharts history, Gantt views |
| Profiler DB | SQLite (`dsf-profiler-test.db`) | Per-(task, node) runtime samples, per-link bandwidth samples |
| History DB | SQLite (`dsf-history.db`) | ODAG run history, used by UI |
| Examples / benchmarks | `examples/`, `eval/network-aware/` | iobt, hetero-compute, wide-pipeline-flex, dag-pipeline, pipeline-ctg, multi-odag-heft, etc. |

### Architecture summary

```
User
 │  dsf {odag,cdag} submit -f x.yml      (or kubectl apply)
 ▼
dsf-system namespace (control plane)
 ┌──────────────────────┐  ┌─────────────────────┐  ┌──────────────────────┐
 │   odag-controller    │  │   cdag-controller   │  │      ui-server       │
 │  network-aware HEFT  │  │  reconcile every    │  │  K8s watch + SQLite  │
 │  + EMA profiler      │  │  30s, restart pods  │  │  REST + SSE + React  │
 └─────────┬────────────┘  └─────────┬───────────┘  └──────────────────────┘
           │                         │
default namespace (task pods, one Service per task for stable DNS)
   ┌──────────┐ DSF_PEER_TRANSFORM=zmq://transform-svc:5555  ┌──────────┐
   │ generate ├────────── ZMQ PUSH/PULL (ODAG) ─────────────►│transform │
   │  (PUSH)  │                                              │  (PULL)  │
   └──────────┘                                              └──┬───────┘
                                                                │ PUSH
                                                          ┌─────▼────┐
                                                          │  output  │
                                                          └──────────┘

(For CDAGs the SDK uses ZMQ PUB/SUB instead of PUSH/PULL.)
(For large ODAG payloads, the data-agent DaemonSet does HTTP file
 transfer node-to-node and signals data readiness; the scheduler can
 then trigger downstream tasks when *their* node has the data.)
```

### Key design decisions

| Decision | Rationale |
|---|---|
| Separate CRDs and controllers for ODAG and CDAG | Their execution models are fundamentally different — one-shot completion vs. forever-running. Sharing controller code creates more accidents than it saves. |
| One ClusterIP Service per task (DNS at `{odag}-{task}.{ns}.svc.cluster.local:5555`) | Stable, controller-free service discovery — peers find each other via env vars (`DSF_PEER_<TASKNAME>`). |
| ZMQ for streaming, file+HTTP (data-agent) for large ODAG payloads | ZMQ is brokerless, low-overhead, ideal for small-to-medium messages. File transfer scales better for hundreds of MB and decouples the producer pod's lifetime from the transfer's. |
| Online EMA profiler (per task × per node runtime, per link bandwidth) | Removes the need for hand-tuned cost estimates; HEFT becomes self-improving over repeated runs. |
| `spreadEpsilon` tie-break (the ε-HEFT contribution) | Addresses a specific pathology we observed: classical HEFT with profiler-fed runtimes concentrates parallel layers on a single node when EFTs are numerically equal but practically indistinguishable. |
| All-pods-start-simultaneously (ODAG) | Downstream pods block on `recv()` until upstream pushes. This is what makes PUSH/PULL work without a broker and without a layer-by-layer scheduler. |
| Constraint-aware placement (`constraints.nodeNames`) | First-class data-gravity expression — pin sensor capture to the sensor node, inference to GPU nodes, etc. |
| Retention policies (`retention.data.policy: immediate / delayed / keepLatest`) | Keep disks clean during long sweeps; matters on resource-constrained edge nodes. |
| SSE for UI live updates | No polling; UI reflects pod state within a second of any cluster event. |

Details on each component live in `/home/anrg/dsf/docs/architecture.md`.

---

## 3. Core Contributions (paper-level)

### Contribution 1 — Network-aware HEFT with online profiling

We use HEFT (Topcuoglu et al., 2002) as the scheduling primitive but
adapt it for an edge Kubernetes setting:

- **Edge bandwidth matrix.** A per-(node, node) bandwidth table is
  maintained by an external measurement source (currently set via a
  ConfigMap; in earlier phase-1 work we did live measurement with
  `pathload`, see `archive/pathload/`). Communication costs in HEFT
  use `dataSize / bandwidth(src, dst)`.
- **Online EMA runtime profiler.** Every task completion writes a
  `(task, node, runtime)` sample. The profiler stores an EMA estimate
  (`emaAlpha = 0.7`, `minSamples = 2`) per (task, node). On the next
  run, HEFT pulls runtimes from the profiler instead of the spec hints.
- **Resolver chain.** Runtime / dataSize values are resolved by:
  profiler → spec hints → template defaults → hardcoded fallback. Cold
  starts (no profiler data) fall back to spec hints — so the first
  run is no worse than a static-cost HEFT.

The result is **HEFT that converges cold-to-optimal in 3–4 runs** on
the real cluster without any manual cost tuning, and continues to
adapt if the cluster or workload changes.

### Contribution 2 — Unified batch + streaming under one platform

Most systems do batch (Argo, Airflow) or streaming (Kafka Streams,
Flink), not both. DSF unifies them:

- **Same SDK pattern** for both: `task = DSFTask(); data = task.recv("up"); task.send("down", out); task.close()`.
- **Same CRD layout, CLI verbs, UI**: only the `kind` and the
  transport pattern differ.
- **Templates and runs.** Users submit `ODAGTemplate` or
  `CDAGTemplate`; the controller materializes runs from them with
  auto-incrementing run numbers and per-template retention.

### Contribution 3 — Brokerless, P2P data plane

- **ODAG large payloads** go through a per-node *data-agent*
  DaemonSet using HTTP push, not through a shared store. The
  producer pod exits as soon as it hands data off to its
  node-local agent; the agent does the cross-node transfer while
  the cluster is free to schedule other work.
- **CDAG streaming** is direct ZMQ PUB/SUB pod-to-pod via stable
  ClusterIP Services. No Kafka broker, no MQTT, no ZooKeeper.

The architectural payoff is that **task completion and data
availability are decoupled.** Same-node successors start immediately;
cross-node successors wait only for the actual physical transfer
(asynchronously, while the upstream pod has already exited).

### Contribution 4 — Kubernetes-native edge orchestration

CRDs (`ODAG`, `CDAG`, `ODAGTemplate`, `CDAGTemplate`), RBAC, owner
references for automatic GC, retention policies for resource-bounded
nodes, and a UI that shows graph + Gantt + history + live status
without polling. All on k3s, no external services beyond a container
registry.

### Contribution 5 — ε-tolerant HEFT tie-breaking (`spreadEpsilon`)

This is the contribution that emerged from observing real cluster
runs.

**The pathology.** Classical HEFT picks the candidate node with the
strictly minimum EFT. When the EMA profiler converges to
near-identical runtime estimates for independent parallel tasks on
candidate nodes (which happens often — same image, similar work,
similar hardware), strict EFT comparison degenerates to
iteration-order tie-breaking. We saw all four `infer-i` tasks in the
IoBT template land on the **same** compute node across 10 consecutive
runs, despite three candidates being available with profiler-identical
runtimes. The predicted makespan was unchanged, but the realised
makespan was brittle to any runtime jitter on the concentrated node,
created NIC contention on a single node, and violated the
reviewer-intuitive expectation that a network-aware scheduler
spreads parallel work.

**The mechanism.** We added a `spreadEpsilon` field (seconds) to
`ODAGTemplate.spec.schedulerConfig`. When choosing among candidates:

1. Compute EFT for every candidate (unchanged from classical HEFT).
2. Let `minEFT = min(EFTs)`.
3. Among candidates with `EFT ≤ minEFT + spreadEpsilon`, pick the
   node with the **fewest committed tasks** so far.

`spreadEpsilon = 0` is still strictly better than classical HEFT: exact
ties are now broken by load rather than by iteration order, so the
scheduler spreads work whenever EFTs are genuinely equal.
`spreadEpsilon > 0` absorbs profiler noise — any candidate within
tolerance of optimal is treated as indistinguishable.

**Why this is a paper-worthy contribution, not just a knob.**

- **Principled framing.** The profiler's outputs are *point estimates*
  with hidden uncertainty. Strict `<` comparison ignores that
  uncertainty. ε-tolerant tie-breaking is the simplest form of
  uncertainty-aware list scheduling; richer variants (variance-aware
  PEFT, risk-adjusted `EFT + k·σ`) are natural follow-ons.
- **Targets a measurable, reproducible failure mode.** Concentration
  under runtime jitter is verifiable in simulation: inject N% jitter
  into a parallel layer and measure makespan distributions. Classical
  HEFT shows a heavy right tail; ε-HEFT tightens it.
- **Generalizes to CDAGs.** The same EMA-induced concentration arises
  in continuous streaming when per-replica latency estimates converge.
  The ε mechanism transfers without architectural change.

In our experimental results (Section 5), ε-HEFT is *makespan-neutral
or slightly better* than strict HEFT, and visibly redistributes the
`infer-i` placements across all three compute nodes instead of
concentrating them. The placement-entropy improvement is the main
empirical evidence for the spread benefit.

---

## 4. Experimental Setup

The experiments live in `eval/network-aware/`. Everything is
self-contained in that directory: templates, task source, Dockerfiles,
orchestration scripts, plotting scripts, and archived results.

### 4.1 Cluster and shaping

- 8 schedulable workers (`anrg-1`, `anrg-3`..`anrg-9`), master `anrg-2`.
- Linux `tc` (htb + netem) applied per node-pair via a setup script
  (`setup-tc-matrix.sh`). The bandwidth matrix is also exposed to the
  scheduler via a ConfigMap (`bandwidth-configmap.yml`) so that HEFT
  uses the same numbers the kernel enforces.

**Bandwidth matrix v2 (current, used for the headline results):**

```
         a-1   a-3   a-4   a-5   a-6   a-7   a-8   a-9
a-1       —    F     F     F     M     M     M     M
a-3       F    —     F     F     S     M     M     M
a-4       F    F     —     F     M     S     M     M
a-5       F    F     F     —     M     M     S     M
a-6       M    S     M     M     —     F     F     M
a-7       M    M     S     M     F     —     F     M
a-8       M    M     M     S     F     F     —     M
a-9       M    M     M     M     M     M     M     —
```

- **F** = 1 Gbps (same-tier: edge↔edge, compute↔compute)
- **M** = 100 Mbps (cross-tier generic)
- **S** = 50 Mbps (engineered bottlenecks: anrg-3↔6, anrg-4↔7, anrg-5↔8)

20× asymmetry between fastest and slowest pair; cross-tier links are
10× slower than same-tier. This is enough asymmetry that random
placement is reliably bad, while HEFT can find same-tier shortcuts or
the least-slow cross-tier option.

(Matrix v1 used M=300 Mbps, S=100 Mbps — 10× asymmetry. v1 results are
preserved under `results/archive-matrix-v1/` and are referenced in the
discussion for sensitivity analysis.)

### 4.2 Benchmarks

Three ODAGs, each chosen to stress a different regime:

| Benchmark | Tasks | Role | Data profile |
|---|---|---|---|
| **iobt** | 14 (5 layers) | Realistic IoBT ISR snapshot. `capture→preprocess→infer→fuse→report`, fan-out 4 then fan-in. Capture and preprocess pinned to sensor nodes; infer free across three compute nodes. | 80–150 MB sensor bursts; ~1 MB detections; ~1 MB report. **Transfer-bound critical path.** |
| **hetero-compute** | 5 | Minimal microbenchmark with explicit per-node `runtimeProfile` hints. Every task is multi-candidate. | 5–100 MB mixed. **Placement-dominated** — every decision matters. |
| **wide-pipeline-flex** | 10 | Structural stress test: fan-out × 2, fan-in × 2, two ingress contention points. | 10–100 MB per edge. **Tests fan-out/fan-in coordination.** |

Each benchmark has three scheduler variants:

- `template-random.yml` — random placement (no scheduling intelligence).
- `template-heft.yml` — network-aware HEFT, `spreadEpsilon = 0`.
- `template-heft-eps.yml` — HEFT, `spreadEpsilon = 1.0` (the Contribution-5 variant).

For the **iobt ε ablation** we additionally ship `template-heft-eps05.yml`
(ε=0.5) and `template-heft-eps20.yml` (ε=2.0).

All variants share the same profiling config (`emaAlpha=0.7`,
`minSamples=2`, `runtimeSource=profiler`, `bandwidthSource=external`,
`retention.data.policy=immediate`) so EMA behavior is directly
comparable.

### 4.3 Sweep procedure

20 runs per configuration. Total: 11 × 20 = 220 runs (iobt has 5
configs because of the ε ablation; the other two benchmarks have 3
configs each).

For each (benchmark, config) cell:

1. Apply the template.
2. Reset the profiler DB (so we observe cold-to-warm convergence in
   every cell).
3. Run 20 ODAG instances back-to-back.
4. Dump per-run status JSON, per-config summary CSV, profiler DB
   snapshot, and a sweep driver log.

All warm statistics in this document use **runs 5–20 (N=16)** to
exclude the cold-start phase.

Scripts:

- `setup-tc-matrix.sh` / `teardown-tc-matrix.sh` — tc shaping
- `cleanup-cluster.sh` — wipe all ODAG/CDAG resources, task pods, per-run data
- `reset-profiler.sh` — clear profiler state
- `sweep-scheduler.sh <benchmark> [N]` — end-to-end sweep
- `plot-results.py` — generate figures from `results/`

---

## 5. Results

The headline figures are in
`eval/network-aware/results/archive-matrix-v2/figures/`:

- `makespan-distribution.png` — box plots of warm makespans per
  config per benchmark
- `makespan-convergence.png` — makespan vs. run index, showing cold-to-warm convergence
- `prediction-scatter.png` — actual vs. HEFT-predicted makespan
- `iobt-infer-placement.png` — share of `infer-i` placements across the three compute nodes
- `bandwidth-matrix.png` — visualisation of the tc matrix

### 5.1 Warm-mean makespan (runs 5–20, N=16, matrix v2)

| Benchmark | Config | Mean (s) | Std (s) | p95 (s) |
|---|---|---:|---:|---:|
| iobt | random | 51.06 | 6.16 | 61.25 |
| iobt | heft (ε=0) | 45.69 | 1.26 | 48.00 |
| iobt | heft-eps05 (ε=0.5) | 45.44 | 1.22 | 47.25 |
| iobt | heft-eps (ε=1.0) | 45.81 | 1.55 | 48.25 |
| iobt | heft-eps20 (ε=2.0) | 45.81 | 1.51 | 48.25 |
| hetero-compute | random | 59.06 | 6.99 | 70.00 |
| hetero-compute | heft (ε=0) | 42.56 | 0.50 | 43.00 |
| hetero-compute | heft-eps (ε=1.0) | 42.75 | 0.75 | 43.50 |
| wide-pipeline-flex | random | 45.69 | 4.65 | 56.25 |
| wide-pipeline-flex | heft (ε=0) | 43.25 | 1.60 | 45.25 |
| wide-pipeline-flex | heft-eps (ε=1.0) | **42.56** | **0.93** | **44.00** |

### 5.2 HEFT vs. random (headline)

| Benchmark | Mean reduction | p95 reduction | Std ratio (random ÷ HEFT) |
|---|---:|---:|---:|
| iobt | **10.5%** | **21.6%** | **4.9× tighter** under HEFT |
| hetero-compute | **28.0%** | **38.6%** | **14× tighter** |
| wide-pipeline-flex | 5.3% | 19.6% | 2.9× tighter |

**Reading these numbers.**

- The mean-reduction headline is **largest on hetero-compute** (28%),
  the benchmark where every task is multi-candidate and placement
  fully determines makespan. This is the cleanest demonstration of
  the network-awareness payoff.
- **iobt** shows a smaller mean reduction (10.5%) because much of its
  critical path is the 150 MB capture→preprocess edge, which is
  hard-pinned to one node and therefore independent of scheduling.
  HEFT still wins **p95 by 21.6%** and tightens std by ~5× — the
  consistency story matters even when mean savings are modest.
- **wide-pipeline-flex** has a small mean improvement (5.3%) because
  its 100 MB source fan-out is bandwidth-bound regardless of placement.
  But again, p95 improves 19.6% and std tightens 2.9×.

So the network-aware contribution shows up in two channels:
**average-case throughput** (mean makespan) when placement controls
the critical path, and **tail-latency consistency** (p95 and std)
always.

### 5.3 ε-HEFT behavior (Contribution 5)

On all three benchmarks, `heft-eps` is within ≤0.7 s of strict HEFT
on the mean, with equal or tighter std:

- **iobt** — ε=1.0 mean 45.81 vs ε=0 mean 45.69: statistically
  indistinguishable. The ε-sweep (0, 0.5, 1.0, 2.0) gives 45.69 →
  45.44 → 45.81 → 45.81 — essentially flat, confirming that ε does
  not cost makespan on this benchmark.
- **hetero-compute** — ε=1.0 mean 42.75 vs ε=0 mean 42.56: +0.19 s
  (within noise).
- **wide-pipeline-flex** — ε=1.0 mean 42.56 vs ε=0 mean 43.25:
  **ε *wins* by 0.7 s** on mean and **halves std (0.93 vs 1.60)**.

The placement-spread benefit shows in
`figures/iobt-infer-placement.png`. With strict HEFT, the four
`infer-i` tasks concentrate; with ε-HEFT they redistribute across all
three compute nodes. This is the qualitative reviewer-intuitive
property the knob was designed to deliver, and it costs nothing in
makespan.

### 5.4 Sensitivity to matrix severity (v1 vs v2)

Matrix v1 (10× asymmetry, M=300/S=100) shows smaller gaps:

| | v1 random | v2 random | v1 heft | v2 heft | v1 gap | v2 gap |
|---|---:|---:|---:|---:|---:|---:|
| iobt | 40.55 | 51.06 | 37.60 | 45.69 | 7.3% | 10.5% |
| hetero-compute | 48.50 | 59.06 | 43.65 | 42.56 | 10.0% | **28.0%** |
| wide-pipeline-flex | 38.75 | 45.69 | 36.95 | 43.25 | 4.6% | 5.3% |

Matrix severity expands the gap most on the benchmark
(`hetero-compute`) where every task is multi-candidate. Makespan-limited
benchmarks (iobt's 150 MB critical path; wpf's 100 MB source fan-out)
see smaller changes because the critical edges are bandwidth-bound
regardless of placement. This is the right qualitative behaviour for a
network-aware scheduler.

### 5.5 Prediction accuracy

`prediction-scatter.png` plots actual makespan against HEFT's predicted
makespan for the HEFT configs. On `hetero-compute` and
`wide-pipeline-flex` the predictions track actuals closely once the
profiler has converged. On `iobt` predictions are slightly biased low
(HEFT under-estimates contention on fan-in at `fuse-tracks`), but the
*ranking* of placements is correct, which is what HEFT relies on.

### 5.6 Earlier-phase results (kept for reference, deprecated by v2)

The `docs/paper-notes.md` document contains an older set of
single-benchmark results (15 runs on a 4-node subset) that show:

- 27% mean / 32% post-convergence makespan reduction on a smaller IoBT
  variant.
- 22% mean / 39% p95 latency reduction on a streaming CDAG variant
  (camera fusion pipeline).

The v2 matrix results above supersede these but are consistent with
them on the qualitative trends.

---

## 6. What's Solid and What Still Needs Work

### Solid (ready to defend in a paper)

- Full system implementation: CRDs, two controllers, UI, CLI, Python
  SDK, profiler, HEFT, ε-HEFT, retention, transport router.
- Working on a real 8-node cluster with reproducible tc-shaped network
  topology.
- Three benchmarks chosen to stress different regimes
  (transfer-bound, placement-dominated, structural).
- 220 sweep runs with cold-start-excluded statistics, archived under
  `results/archive-matrix-v2/`.
- Mean / p95 / std all consistent with the network-aware story across
  benchmarks; ε-HEFT is makespan-neutral or slightly better than strict
  HEFT and visibly spreads parallel work.

### To strengthen before submission

- **CDAG quantitative results on the v2 matrix.** We have older CDAG
  numbers (22% avg / 39% p95 latency improvement on a 4-node camera
  pipeline) but they were collected before the current matrix and
  templates. Re-running on the v2 matrix would complete the symmetry of
  the paper.
- **Brokered baseline.** The motivation contrasts DSF's brokerless P2P
  data plane with Kafka/MQTT/NFS. We don't yet have a direct head-to-head
  measurement on the same cluster. Plan: ZMQ vs MQTT (mosquitto) on a
  fan-out topology, N ∈ {2,4,6,8}, 100 KB messages at 5 msg/s — predicted
  shape: ZMQ throughput scales linearly, MQTT plateaus as the broker
  saturates.
- **Variance-aware HEFT extension.** ε-HEFT is the simplest form of
  uncertainty-aware list scheduling. A PEFT-style variance-aware
  follow-on (`EFT + k·σ`) would deepen the contribution; we have the
  per-(task, node) sample variance in the profiler already.
- **Fault model.** The data-agent retries with bounded backoff but
  there is no formal failure-mode analysis. Worth at least an
  honest limitations paragraph; ideally a fault-injection experiment.
- **HEFT v2 (resource-aware co-location).** Notes in
  `docs/heft-v2-notes.md` — current HEFT assumes sequential execution
  on a node, but k3s parallelises co-located pods. Switching the
  `nodeAvail` scalar to a (CPU, memory) timeline would improve
  prediction accuracy and let HEFT pack parallel-friendly tasks
  intentionally. Defer past first submission.

### Known weaker points (will be reviewer targets)

- **HEFT is well-known.** Our HEFT-specific novelty is the *online EMA
  profiler that feeds it* and the *ε tie-break*, not the algorithm
  itself. The contribution framing should lead with the profiler/ε
  combination, not with HEFT.
- **iobt mean reduction is only ~10%.** It is critical to explain
  *why* (capture→preprocess is pinned and bandwidth-bound) and to point
  reviewers at the p95 / std numbers and at hetero-compute (28%) as
  the placement-dominated counter-example.
- **Mean makespan is similar across ε values on iobt.** This is the
  *desired* property — ε is a Pareto knob for spread vs. strict
  optimality — but we need to make sure reviewers don't read it as
  "ε does nothing."

---

## 7. Where to Look in the Repo

| Question | File / directory |
|---|---|
| High-level README | `README.md` |
| Detailed architecture | `docs/architecture.md` |
| Paper notes / contributions | `docs/paper-notes.md` |
| Paper motivation (NSDI framing) | `docs/paper-motivation.md` |
| 20 scenarios where DSF beats existing systems | `docs/paper-scenarios.md` |
| HEFT v2 sketch (deferred work) | `docs/heft-v2-notes.md` |
| IoBT benchmark rationale | `docs/iobt-design-rationale.md` |
| ODAG controller (Go) | `cmd/odag-controller/` |
| CDAG controller (Go) | `cmd/cdag-controller/` |
| UI server (Go) | `cmd/ui-server/` |
| Python SDK | `sdk/python/dsf_sdk/` |
| HEFT (Go reference) | `pkg/scheduler/heft.go` |
| HEFT (Python reference, for future subprocess scheduler) | `sdk/python/dsf_sdk/schedulers/heft.py` |
| Eval scripts and templates | `eval/network-aware/` |
| Archived results, matrix v2 (headline) | `eval/network-aware/results/archive-matrix-v2/` |
| Archived results, matrix v1 (sensitivity) | `eval/network-aware/results/archive-matrix-v1/` |
| Phase-1 / phase-2 (kept for reference) | `archive/` |

---

## 8. Suggested Paper Arc (one slide)

1. **Motivation.** Workflow systems were designed for cloud
   environments; the edge is heterogeneous, link-asymmetric, and
   storage-poor. Existing systems can't see the network and pay a
   two-hop penalty even for co-located producers/consumers.
2. **DSF system.** Kubernetes-native (k3s) DAG framework: ODAG (batch)
   + CDAG (streaming) under one CRD/SDK/UI; brokerless P2P data plane
   (ZMQ + data-agent); per-node retention.
3. **Scheduling contribution.** Network-aware HEFT with an online EMA
   profiler that converges in 3–4 runs, plus an ε-tolerant tie-break
   that absorbs profiler uncertainty and spreads parallel work without
   hurting makespan.
4. **Evaluation.** 8-node tc-shaped cluster (20× bandwidth asymmetry).
   Three benchmarks (iobt, hetero-compute, wide-pipeline-flex), 20
   runs × 11 configs. HEFT vs. random: up to 28% mean / 38.6% p95
   reduction; std tightens by 2.9–14×. ε-HEFT: makespan-neutral,
   spreads parallel layers across all candidate nodes.
5. **Limitations and next steps.** Brokered-baseline comparison and a
   variance-aware HEFT extension are the obvious follow-ons.

---

*Last refreshed: 2026-05-11.*
