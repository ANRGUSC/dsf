# DSF — Paper Motivation

Target venue: NSDI (or similar systems venue: OSDI, EuroSys, Middleware)

---

## The Core Observation

The strongest NSDI motivation starts not with features, but with a **fundamental mismatch** that exists in every current workflow system:

> **Existing containerized DAG frameworks treat the network as plumbing — a shared medium to reach shared storage. DSF treats the network as a first-class resource that should inform when tasks start, where they run, and how they communicate.**

This is not a philosophical point. It has concrete, measurable consequences.

In every major workflow system today (Argo, Airflow, Tekton, Prefect), the data movement pattern for task A → task B is:

```
A computes → A uploads to shared store (S3, NFS, DB) → B downloads from shared store → B computes
```

This introduces three structural inefficiencies that DSF directly attacks:

1. **Two-hop penalty**: Data travels A → store → B even if A and B are on the same physical node. The network topology is invisible to the framework.
2. **Task-I/O coupling**: Task A's pod stays alive (holding CPU and memory) until the upload completes. Compute is done, but resources are held. On resource-constrained nodes this is directly wasteful.
3. **Global vs. local data readiness**: "Data is ready" means "data is in the store" — but the right question is "data is ready *on the node where B will run*." These are different events, separated by a network transfer. No existing system distinguishes them.

---

## Use Case 1: Heterogeneous Edge Clusters

**The primary scenario and the one where the network pathology is worst.**

### Setup

A facility (industrial plant, research lab, hospital, smart building) deploys a k3s cluster of 8–20 nodes. Nodes are heterogeneous: some are ARM boards with sensors attached, some are x86 servers, one has a GPU, one has a high-speed NIC. Cloud connectivity is absent, expensive, or has latency requirements that rule it out. There is no shared storage that scales.

### The pipeline

Sensor capture → preprocessing → ML inference → alerting/storage. This is a DAG. Each stage needs to run on the right node type. Data between stages can be tens to hundreds of MB per batch.

### Why existing tools fail here

- **Argo** requires S3-compatible artifact storage — you'd need to run MinIO, which consumes memory on already-constrained nodes and adds two network hops per inter-task data transfer.
- **Airflow** requires a central database and a broker — more infrastructure overhead, designed for cloud deployment.
- **Neither** supports "pin this task to the GPU node, pin that task to the sensor node" as a first-class DAG spec primitive.
- Both were designed for cloud environments with elastic resources and a flat, high-bandwidth network — assumptions that do not hold at the edge.

### What DSF offers

- `constraints.nodeNames` pins inference to the GPU node and capture to the sensor node, directly in the YAML spec.
- Data moves directly: sensor node → inference node via data-agent HTTP, no intermediate store.
- The inference pod exits as soon as compute finishes; the data-agent delivers results to the alerting node while the GPU is already freed for the next batch.
- No broker, no shared storage, no extra deployments beyond k3s itself.

### The NSDI hook

Edge computing with real network topology constraints is increasingly common — IoT, 5G MEC, on-premises ML. The gap in tooling is measurable: broker overhead, extra network hops, and infrastructure resource consumption are all quantifiable on real hardware.

---

## Use Case 2: Scientific Multi-Stage Data Pipelines

### Setup

A research cluster (university HPC or national lab) runs bioinformatics, climate simulation post-processing, or physics analysis pipelines. Stages: ingest → quality control → alignment/simulation → statistical analysis → visualization. Intermediate outputs are large — genomics alignment can produce 10–50 GB per sample; climate simulation checkpoints can reach TBs.

### The current problem

These pipelines typically run with Nextflow + NFS or Snakemake + NFS. NFS is the shared storage, and the link to the NFS server becomes the bottleneck. Every task reads its input from NFS (one hop) and writes its output to NFS (one hop). If the analysis task runs on the same node as the alignment task, the data still traverses the network to NFS and back. Node co-location is invisible to the scheduler.

### What DSF offers

- **DataReady scheduling** means the analysis task starts only when alignment's output is physically on the analysis task's node — which, if they are co-located, means zero transfer latency.
- **HEFT placement** can minimize transfer cost: "alignment produces 20 GB → assign analysis to the same node, or to the node with the highest bandwidth link to alignment's node."
- **Non-blocking send** means alignment's CPU and memory are freed the instant it writes its output locally and hands off to the data-agent. The data-agent does the cross-node transfer independently. The cluster's utilization profile improves.

### The NSDI hook

A clean measurement opportunity: quantify how much per-node DataReady scheduling reduces makespan vs. pod-completion-triggered scheduling, as a function of data size and placement strategy. The experiment is straightforward to run on the anrg cluster.

---

## Use Case 3: Network Measurement Experiments

**The origin use case, directly from the archive/phase-1 pathload work — and compelling because it is self-referential.**

### Setup

Coordinated network measurement experiments across a cluster. Each experiment is a pipeline:
- Sender task: generates probe traffic on a specific node pair
- Receiver task: collects measurements on the peer node
- Analyzer task: aggregates and computes metrics (available bandwidth, path delay, pathload)

### Why this is a DAG problem

The experiment has inherent structure — you cannot analyze before you measure, you cannot measure before the receiver is ready. It is a one-shot DAG with strict node affinity (sender and receiver must be on specific nodes) and ordering constraints.

### What was missing

A way to express "run this task on node A, this task on node B, wire them with this communication pattern" without managing pod creation and inter-pod communication manually — and to do this systematically for many node pairs.

### What DSF enables

- Submit an ODAG with `constraints.nodeNames` pinning sender and receiver to the correct nodes.
- The SDK wires the communication transparently (ZMQ or file transport).
- One-shot completion; results collected by the analyzer task.
- The same pipeline repeated across node pairs without manual pod management.

### The NSDI hook

A system that can orchestrate network measurement DAGs on the very infrastructure it is designed for closes the loop neatly: DSF's design is informed by the measurement work in archive/, and that measurement work is itself a motivating use case. DSF running on DSF's own cluster, measuring itself. Reviewers will notice the coherence.

---

## Use Case 4: The Data Gravity Problem

**A conceptual contribution that generalizes across all the above use cases.**

### The observation

In many pipelines, data is generated at a specific node and is large. Moving it is expensive. The right strategy is: bring compute to data for the first stage, then move only the smaller processed output downstream. This is called **data gravity** — data pulls compute toward itself.

### The gap in existing systems

No Kubernetes-native workflow system gives you a first-class way to express "stage 1 must run where the data already is; schedule stage 2 based on where stage 1's output ends up." HEFT reasons about this in theory but is not implemented at the container orchestration level in any existing K8s-native framework.

### DSF's answer

- `constraints.nodeNames` = explicit data gravity expression ("this task must run on the node where the raw data lives").
- `dep_node()` in the SDK = a task can introspect where its dependency ran, enabling task code to log or adapt based on whether a transfer was local or cross-node.
- HEFT integration = automatic data-gravity-aware scheduling using `dataSize` and `runtime` hints from the spec, without user intervention.

### Why this matters for the paper

Data gravity is a recognized concept in distributed systems and cloud architecture, but it has not been operationalized at the DAG scheduling layer in a Kubernetes-native framework. DSF is the first system where the scheduler and the data plane jointly reason about it.

---

## Use Case 5: Brokerless Streaming Pipelines (CDAGs)

**Filling the gap between heavyweight stream processors and raw Kubernetes Deployments.**

### The scenario

A continuous data processing pipeline — log analysis, metrics aggregation, video frame processing, sensor fusion. It should run forever, on Kubernetes, with direct task-to-task communication.

### Current options and their costs

| Option | Problem |
|---|---|
| Flink / Spark Streaming | Heavyweight runtimes, their own cluster managers, not container-native |
| Kafka + consumer pods | Requires a Kafka cluster (multiple brokers, ZooKeeper/KRaft), adds broker-mediated latency, high operational complexity |
| Argo CronWorkflow | Not streaming — repeated batch, not continuous |
| Custom Deployments | Manual ZMQ/socket wiring, no lifecycle management, no observability |

On a 4-node edge cluster, a 3-broker Kafka setup consumes a significant fraction of available memory and bandwidth — resources that the pipeline tasks themselves need.

### What DSF's CDAG offers

- YAML-described streaming graph deployed as Kubernetes pods.
- Direct ZMQ PUB/SUB between tasks via stable Kubernetes ClusterIP Services — no broker hop.
- Reconcile loop every 30s: failed pods are detected and recreated, respecting placement constraints.
- The same 4-line SDK pattern (`recv → process → send → close`) works identically for both ODAGs and CDAGs; users switch modes by changing the YAML `kind`, not their task code.

### The NSDI hook

A sharp comparison: DSF CDAG vs. Kafka-backed pipeline on the same cluster. Measure broker overhead (CPU/memory consumed by broker infrastructure), per-message latency (direct ZMQ vs. broker-mediated), and throughput under resource pressure. The edge cluster makes the differences visible in a way that cloud experiments would not.

---

## The Unifying Insight for the Paper

The five use cases above all reduce to the same problem: **workflow systems were designed for cloud environments with abundant, flat networks and shared storage. DSF is designed for environments where the network is heterogeneous, resources are constrained, and shared infrastructure is a cost, not a given.**

The specific architectural insight that DSF contributes:

> **Task completion and data availability are two distinct events separated by a network transfer. Treating them as one — as all existing workflow systems do — wastes resources, ignores topology, and prevents compute-transfer overlap.**
>
> DSF makes this distinction explicit: a task signals data handoff immediately and exits; a node-local data-agent completes the transfer independently; the scheduler triggers the downstream task when *its specific node* has the data, not when the upstream pod exits.

This is analogous to the non-blocking send insight in MPI — parallel systems research learned decades ago that blocking a process during data transfer is wasteful. DSF applies the same insight at the container orchestration level, where the context is different (slower startup costs, larger data payloads, heterogeneous networks, container isolation) but the principle is identical. That is a framing NSDI will recognize.

---

## Three Levels of Decoupling (Paper Framing)

The contribution can be organized as three levels of decoupling, each building on the previous:

**Level 1 — Compute-Transfer Decoupling**
Task pods exit after compute; data transfer proceeds independently via node-local agents. The task lifetime equals only the compute phase. Cluster resources are freed immediately.

**Level 2 — Data-Locality-Aware Scheduling**
Downstream tasks are triggered by data arrival on *their* node, not by upstream pod completion. Same-node successors start immediately (zero transfer overhead); cross-node successors wait only for the actual network transfer, not for the upstream pod to exit.

**Level 3 — Unified Batch/Streaming Abstraction**
The same SDK and pattern (`init → recv → send → close`) supports both one-shot and continuous DAGs. Transport selection (file+HTTP for ODAGs, ZMQ PUB/SUB for CDAGs) is handled by the framework. Users switch modes by changing the YAML `kind`.

---

## What Is Not Yet a Strong Motivation (be honest in the paper)

- **HEFT not integrated into live controllers**: don't lead with "intelligent scheduling" until Experiments 4–6 are run with real HEFT placement.
- **No broker comparison measured**: the CDAG vs. Kafka claim needs numbers before it becomes a paper-level result.
- **The streaming story is secondary**: the core novelty is the decoupled data plane (Levels 1 and 2 above). Level 3 strengthens the paper but should not be the lead.
- **No fault model**: the data-agent has 5 retries with 500ms backoff, but there is no formal analysis of failure modes. This is a reviewer target — acknowledge it in the limitations section.
