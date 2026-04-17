# IoBT Mission Snapshot — Design Rationale

Paper-ready notes on the `examples/iobt-mission-snapshot-odag` benchmark: DAG
structure, the "no compression" modeling assumption, and the rationale for
separating the capture and preprocess tasks even when they are co-located.

---

## 1. DAG structure (recap)

```
capture-1 -> preprocess-1 -> infer-1 \
capture-2 -> preprocess-2 -> infer-2  \
capture-3 -> preprocess-3 -> infer-3   --> fuse-tracks --> generate-report
capture-4 -> preprocess-4 -> infer-4  /
```

Five stages, 14 tasks:

| Stage       | Tasks              | Work                                                    | Data out            | Placement                              |
|-------------|--------------------|---------------------------------------------------------|---------------------|----------------------------------------|
| Capture     | capture-1..4       | Deterministic MD5-seeded raw blob; sensor-delay sleep   | 100/120/80/150 MB   | Pinned to sensor nodes (anrg-1,3,4,5)  |
| Preprocess  | preprocess-1..4    | SHA-256 over 64 KB chunks (simulated feature extraction)| 100/120/80/150 MB   | Co-located with matching capture       |
| Infer       | infer-1..4         | CPU busy-loop; emits 5–15 fake detections (JSON)        | ~1 MB               | Compute tier (anrg-6,7,8,9)            |
| Fuse        | fuse-tracks        | Fan-in `recv_all()`; group+avg by class, top-5          | ~1 MB               | anrg-7, anrg-8                         |
| Report      | generate-report    | Formatted mission report with provenance                | 0 (stdout)          | anrg-9 (gateway)                       |

Mixed hard/soft placement: capture, preprocess, fuse, and report are pinned;
infer has four candidate nodes, so real scheduling decisions are made by HEFT
on the inference layer.

---

## 2. "No compression" modeling assumption

For the benchmark we deliberately remove data compression on the
capture→preprocess→infer path:

- Preprocess output size equals capture output size (1:1 per branch).
- Template `dataSize` values set accordingly (100/120/80/150 MB).
- `tasks/preprocess/task.py` pads the output to the full `DSF_DATA_SIZE` from
  the received raw bytes (no synthesized filler).

**Why this assumption matters.** Without compression, the preprocess→infer edge
becomes the dominant cross-node transfer (80–150 MB × 4 = up to 450 MB of
aggregate inter-node traffic per run). This shifts the benchmark's critical
path from compute to network, which is where DSF's network-aware HEFT
scheduling is intended to win. A compressing preprocess would hide that cost
and make the benchmark compute-bound, understating the value of transfer-aware
placement.

This is a modeling choice specific to the benchmark, not a claim that real IoBT
deployments never compress. Real systems compress when bandwidth is the
constraint; here we are *measuring* bandwidth effects and so we stress them.

---

## 3. Why capture and preprocess are separate tasks despite co-location

A reasonable reader will ask: if `capture-i` and `preprocess-i` are pinned to
the same node, why are they two tasks instead of one fused task? Five reasons,
in roughly decreasing order of importance for the paper:

### 3.1 They model different architectural roles, not different nodes
- **Capture** is the sensor driver: hardware I/O, frame buffers, timestamping,
  device-specific quirks.
- **Preprocess** is a signal / feature pipeline: chunking, denoising, feature
  extraction, format conversion.

In real IoBT systems these are maintained by different teams, updated on
different cadences, and sometimes written in different languages. Fusing them
couples hardware access to analytics code. Keeping them separate means the
preprocess stage can be swapped (e.g., to compare feature extractors) without
touching the sensor driver image.

### 3.2 Co-location is a scheduling *decision*, not a *structural* constraint
The DAG topology does not assume co-location; the `constraints.nodeNames`
field expresses it as a placement policy. This separation makes it possible to:
- Fail over preprocess to a neighbor node when the sensor host dies mid-mission.
- Lift preprocess onto a sensor node with an on-board accelerator while keeping
  a fallback path to a nearby compute node for dumber sensor nodes.
- A/B test co-located vs. remote preprocess to quantify the locality benefit
  empirically — a key experiment for the paper.

If capture and preprocess were fused into one task, none of these experiments
are expressible without forking the code.

### 3.3 The framework's unit of observability is the task
DSF reports per-task runtime, data size, node placement, retries, logs, and
Gantt bars. Fusing capture and preprocess erases the measurement boundary
between "sensor capture time" and "feature extraction time" — the exact
breakdown a benchmark needs to surface. The simulated SHA-256 work in
preprocess models a real cost (on-sensor DSP or feature extraction), and
resolving that cost separately is how we defend the claim that DSF gives
operators fine-grained visibility.

### 3.4 HEFT needs the data-movement edge to rank correctly
Even when capture and preprocess are co-located, the `capture → preprocess`
edge still carries a `dataSize` (100–150 MB) that HEFT uses in its upward
rank computation. Fusing the two tasks hides that data movement from the
scheduler. HEFT would then plan only around the compressed-looking
preprocess→infer edge and could mis-rank branches whose capture→preprocess
cost differs materially from their preprocess→infer cost. The two-task form
exposes both edges to the planner.

### 3.5 Failure and retry semantics
If preprocess crashes (e.g., OOM on an unexpectedly large frame), we want to
retry preprocess only — not re-capture. A sensor capture may have consumed a
one-shot burst that cannot be reproduced. Two tasks give two independent retry
boundaries; one fused task forces re-capture on any analytics failure.

---

## 4. Summary for the paper

The IoBT Mission Snapshot benchmark is structured to exercise all three cost
regimes that DSF claims to handle well: compute-bound inference, transfer-bound
preprocess→infer edges, and coordination-bound fan-in at `fuse-tracks`. The
deliberate choice to split capture and preprocess — even under co-location —
is not incidental; it lets us model real architectural boundaries, measure
per-stage costs independently, and expose both data-movement edges to the
scheduler. The "no compression" assumption on preprocess output is a modeling
choice that targets the benchmark at the network-aware scheduling regime,
where DSF's contribution is most visible.
