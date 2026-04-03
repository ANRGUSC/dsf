# DSF — 20 Concrete Scenarios Where Existing Frameworks Fall Short

Each scenario describes a real deployment, the DAG structure, exactly why existing tools fail, and what DSF enables. References are to top-venue papers.

Scenarios 1–10 are **ODAGs** (one-shot, run to completion). Scenarios 11–20 are **CDAGs** (continuous, always-on streaming pipelines).

---

## Scenario 1: Whole-Genome Sequencing Pipeline on a University Research Cluster

**Setting.** A genomics lab runs a 10-node cluster (heterogeneous: some nodes have NVMe SSDs, one has a GPU for GPU-accelerated alignment). Raw reads arrive from a sequencer attached to a specific node. Intermediate files (aligned BAM, sorted BAM, variant VCF) are 20–200 GB per sample.

**Pipeline.**
```
basecalling (sequencer node) → quality control → alignment (BWA-MEM) → sorting → variant calling (GATK HaplotypeCaller) → annotation
```

**Why existing tools fail.**
- **Nextflow + NFS**: the standard tool for this pipeline (Di Tommaso et al., *Nature Biotechnology*, 2017). Every inter-stage file goes to NFS regardless of whether the next stage runs on the same node. The NFS link saturates on multi-sample runs. Nextflow has no concept of "launch this task on the node where the input data already lives." Nextflow's `executor = 'local'` or SLURM modes don't support Kubernetes-level node affinity with data-aware scheduling.
- **Pegasus** (Deelman et al., *Future Generation Computer Systems*, 2015): explicitly designed around shared storage (GridFTP, S3). Every inter-task data transfer goes through a staging site. Two hops per edge in the DAG, regardless of co-location.

**What DSF enables.**
- `constraints.nodeNames: [sequencer-node]` pins basecalling to the sequencer-attached node — no raw read data moves off that node.
- HEFT placement uses `dataSize` (200 GB BAM) to schedule alignment and sorting on the same node as basecalling where possible, eliminating the transfer entirely.
- DataReady scheduling: variant calling starts exactly when the sorted BAM arrives on its assigned node, not after GATK's pod starts and blocks waiting for the file.

**References.**
- Di Tommaso, P. et al. "Nextflow enables reproducible computational workflows." *Nature Biotechnology* 35, 316–319 (2017).
- Deelman, E. et al. "Pegasus, a workflow management system for science automation." *Future Generation Computer Systems* 46, 17–35 (2015).

---

## Scenario 2: Live Edge Video Analytics on a Smart City Camera Network

**Setting.** A city deploys 20 cameras, each attached to an ARM edge node. A separate node has a GPU for object detection. A fifth node aggregates alerts. The cluster is on-premises with 1 GbE links; no cloud connectivity. Frames arrive at 30 fps per camera.

**Pipeline.**
```
frame capture (camera node) → object detection (GPU node) → tracking (CPU node) → alert aggregation
```

**Why existing tools fail.**
- **VideoStorm** (Hung et al., *NSDI*, 2017): designed for a centralized cluster with a single powerful back-end. Its resource manager assumes all cameras stream to a central server — the model is ingest-then-process, not process-at-source. It cannot express "run capture on camera node X, detection on GPU node Y."
- **Chameleon** (Jiang et al., *SIGCOMM*, 2018): adapts video knobs (resolution, frame rate, model) to improve accuracy under resource constraints, but operates on a central back-end. No per-node pipeline structure.
- **Reducto** (Li et al., *SIGCOMM*, 2020): filters frames on-camera before sending to the back-end (closer to what DSF enables at the data layer), but Reducto is a filtering heuristic, not a framework for deploying and orchestrating the full processing pipeline across heterogeneous nodes.
- **Argo Workflows**: no constraint mechanism pins capture to the specific camera node. All inter-task data goes to a shared artifact store — 30 fps frames for 20 cameras through MinIO is not feasible on a constrained 1 GbE cluster.

**What DSF enables.**
- One CDAG per camera with `constraints.nodeNames: [camera-node-i]` for capture and `constraints.nodeNames: [gpu-node]` for detection.
- Direct ZMQ PUB/SUB: frames go camera-node → GPU node via ClusterIP DNS, no broker.
- CDAG's reconcile loop restarts failed pods; RestartPolicy: Always handles transient failures.

**References.**
- Hung, C.-C. et al. "VideoStorm: Live Video Analytics at Scale with Approximation and Delay-Tolerance." *NSDI* (2017).
- Jiang, J. et al. "Chameleon: Scalable Adaptation of Video Analytics." *SIGCOMM* (2018).
- Li, H. et al. "Reducto: On-Camera Filtering for Resource-Efficient Real-Time Video Analytics." *SIGCOMM* (2020).

---

## Scenario 3: Coordinated Available-Bandwidth Measurement Campaigns

**Setting.** A network researcher wants to measure available bandwidth between all pairs of 9 nodes in a cluster (36 directed pairs). Each measurement is a one-shot pipeline: probe sender on node A, probe receiver on node B, analyzer collecting and computing the result. The researcher repeats this for different times of day, cross traffic levels, and routing configurations.

**Pipeline (per node pair).**
```
receiver setup (node B) → sender probe (node A) → collector/analyzer (any node)
```

**Why existing tools fail.**
- **Manual scripts (SSH + screen/tmux)**: the standard approach for pathload-style experiments (Jain & Dovrolis, *SIGCOMM*, 2002). Does not compose: running 36 pairs systematically requires fragile bash orchestration. No retry, no status, no structured data collection.
- **Airflow / Prefect**: designed for data pipelines, not node-targeted experiments. No mechanism to say "this task must run on node A, this task on node B." TaskGroups in Airflow have no node affinity.
- **Ansible**: can run tasks on specific hosts but has no DAG execution model. There is no concept of "task B starts only after task A's output is ready on task B's node." Ansible runs playbooks, not data-flow pipelines.
- **Pingmesh** (Xu et al., *SIGCOMM*, 2015): measures ICMP latency at scale inside a data center, with agents pre-deployed everywhere. It is a measurement system, not a programmable DAG framework for arbitrary measurement pipelines. You cannot compose Pingmesh agents into a custom multi-step experiment.

**What DSF enables.**
- Each measurement pair is an ODAG: receiver task with `constraints.nodeNames: [node-B]`, sender task with `constraints.nodeNames: [node-A]`, analyzer with no constraint.
- 36 ODAGs submitted in parallel. Status, logs, and results collected via the CLI.
- DataReady ensures the analyzer starts only when the sender has finished and its output has arrived on the analyzer's node.
- One YAML template, parameterized per pair.

**References.**
- Jain, M. and Dovrolis, C. "End-to-end available bandwidth: measurement methodology, dynamics, and relation with TCP throughput." *SIGCOMM* (2002).
- Xu, M. et al. "Pingmesh: A Large-Scale System for Data Center Network Latency Measurement and Analysis." *SIGCOMM* (2015).

---

## Scenario 4: Multi-Stage ML Inference Pipeline on a GPU-Heterogeneous Cluster

**Setting.** A hospital runs inference on medical imaging data (CT scans, ~500 MB per study). The pipeline is: DICOM preprocessing (any CPU node) → segmentation model (GPU node, only 1 in the cluster) → downstream classifier (CPU node) → report generation. Data cannot leave the premises (HIPAA). No cloud.

**Pipeline.**
```
DICOM ingest (data-local node) → preprocessing → segmentation (GPU node) → classifier → report
```

**Why existing tools fail.**
- **Clipper** (Crankshaw et al., *NSDI*, 2017): a model serving system that handles single-model, single-stage inference with batching and latency targets. It is not a multi-stage pipeline framework. There is no concept of "chain preprocessing → model → postprocessing" with different node assignments per stage.
- **Clockwork** (Gujarati et al., *OSDI*, 2020): focuses on predictable latency for DNN serving from a single cluster of GPU workers. Same limitation: single-stage serving, not a composable DAG with mixed CPU/GPU stages.
- **Ray** (Moritz et al., *OSDI*, 2018): general distributed compute framework. Its object store passes data between tasks but routes through a central object store on a coordinator node — data from the DICOM node goes to the object store, then the GPU node fetches from it. On a 10 GbE hospital cluster with 500 MB studies, this is two full network transfers per case. Ray's scheduler does not support "pin this task to the only GPU node" as a first-class DAG spec primitive; you use `ray.remote(resources={"GPU": 1})`, which relies on Ray's resource model, not hardware node pinning.
- **Argo**: artifacts go to S3 (MinIO). Two hops per edge, and MinIO must be deployed and maintained on-premises.

**What DSF enables.**
- `constraints.nodeNames: [gpu-node]` pins segmentation to the one GPU node.
- `constraints.nodeNames: [dicom-server]` pins ingest to the node where DICOM files are stored — no raw imaging data traverses the network.
- DataReady scheduling: the GPU node's segmentation task starts immediately when the preprocessed volume arrives there, without waiting for the preprocessing pod to exit.
- Non-blocking send: the preprocessing pod exits as soon as it writes the volume locally and hands off to the data-agent. The GPU is not blocked waiting for a pod slot to open up.

**References.**
- Crankshaw, D. et al. "Clipper: A Low-Latency Online Prediction Serving System." *NSDI* (2017).
- Gujarati, A. et al. "Serving DNNs like Clockwork: Performance Predictability from the Bottom Up." *OSDI* (2020).
- Moritz, P. et al. "Ray: A Distributed Framework for Emerging AI Applications." *OSDI* (2018).

---

## Scenario 5: Split Inference on a Heterogeneous Edge Cluster (Beyond Binary Cloud Split)

**Setting.** A manufacturing plant runs a quality control pipeline on a small cluster: 4 camera nodes (ARM, low compute), 2 feature extraction nodes (x86 CPU), 1 GPU inference node, 1 alerting/logging node. The goal is to process images through a deep model without sending raw data off-premises.

**Pipeline.**
```
camera capture (ARM node) → resize + normalize (CPU node) → DNN inference (GPU node) → defect classifier (CPU node) → alert
```

**Why existing tools fail.**
- **Neurosurgeon** (Kang et al., *ASPLOS*, 2017): the canonical work on splitting DNN inference between device and cloud. Neurosurgeon partitions a single DNN at one layer boundary — everything before the split runs on the device, everything after runs in the cloud. It is a binary split and requires a cloud back-end. It cannot express a multi-stage pipeline with 4 different node types in a single DAG, nor does it work on an on-premises-only cluster.
- **Argo / Airflow**: can express the DAG but have no mechanism to pin different stages to different hardware types beyond Kubernetes node selectors. More critically, inter-stage data (the feature tensor, possibly 10–50 MB per image) goes through shared artifact storage, adding two network hops per stage transition on a constrained plant network.

**What DSF enables.**
- The full 4-stage pipeline is one ODAG with per-task `constraints.nodeNames`.
- Data flows directly: ARM node → CPU node (one hop, resize output ~1 MB) → GPU node (one hop, feature tensor ~20 MB) → CPU classifier → alert. No intermediate store.
- HEFT scheduling can assign the CPU preprocessing to the CPU node closest (highest bandwidth) to the GPU node, minimizing the dominant transfer.
- Same SDK pattern in every task regardless of the node type it runs on.

**References.**
- Kang, Y. et al. "Neurosurgeon: Collaborative Intelligence Between the Cloud and Mobile Edge." *ASPLOS* (2017).
- Topcuoglu, H. et al. "Performance-Effective and Low-Complexity Task Scheduling for Heterogeneous Computing." *IEEE TPDS* 13(3), 260–274 (2002). (HEFT, the scheduling algorithm DSF uses)

---

## Scenario 6: Post-Processing Pipeline for Large-Scale Scientific Simulations (No Shared Filesystem)

**Setting.** A computational fluid dynamics group has a 12-node cluster. Simulations run on nodes 1–8 (MPI, writes checkpoint files locally). Post-processing (interpolation, field extraction, visualization) must run where the checkpoint files are — moving them is prohibitively expensive (each checkpoint is 50–300 GB). Post-processing has 3 stages, each dependent on the previous stage's output.

**Pipeline (per simulation node).**
```
checkpoint reader (simulation node) → field extractor → interpolation → visualization renderer (GPU node)
```

**Why existing tools fail.**
- **Parsl** (Babuji et al., *HPDC*, 2019): a Python parallel programming library for HPC. Parsl moves data between tasks via shared filesystem (Globus, NFS, POSIX). If the shared filesystem is unavailable or too slow, Parsl stalls. Parsl's `DataFuture` model assumes data is always addressable via a path that all workers can access. It has no model for "this task must run on the node where the file lives."
- **Pegasus** (Deelman et al., *FGCS*, 2015): explicitly stages all inter-task data through a transfer site. For 300 GB checkpoints, Pegasus would stage the file off the simulation node to a staging area, then back to the post-processing node — doubling the data movement.
- **Argo**: same artifact-storage problem. 300 GB to MinIO then back = 600 GB of network traffic per checkpoint.

**What DSF enables.**
- `constraints.nodeNames: [sim-node-i]` pins the checkpoint reader to the node where the file exists.
- DataReady scheduling: the field extractor starts on the same node (zero transfer if co-located) or waits only for the actual cross-node transfer if the next stage must run elsewhere.
- Non-blocking send: the checkpoint reader exits immediately after writing its output locally and handing off to the data-agent. The simulation node's memory is freed without waiting for the post-processing pipeline to complete.

**References.**
- Babuji, Y. et al. "Parsl: Pervasive Parallel Programming in Python." *HPDC* (2019).
- Deelman, E. et al. "Pegasus, a workflow management system for science automation." *Future Generation Computer Systems* 46, 17–35 (2015).
- Topcuoglu, H. et al. "Performance-Effective and Low-Complexity Task Scheduling for Heterogeneous Computing." *IEEE TPDS* 13(3) (2002).

---

## Scenario 7: Serverless-Style Parallel DAG Computation on a Private Cluster (No FaaS)

**Setting.** A quantitative finance team runs a Monte Carlo simulation DAG nightly: a parameter sweep with fan-out (hundreds of simulation tasks) followed by result aggregation. The computation must stay on-premises (regulatory). The team has a 16-node bare-metal k3s cluster but no cloud account and no FaaS platform.

**Pipeline.**
```
parameter generator → [sim-1, sim-2, ..., sim-N] (fan-out) → aggregator → report
```

**Why existing tools fail.**
- **gg** (Fouladi et al., *USENIX ATC*, 2019): the canonical system for running DAG-structured jobs by farming out tasks to cloud Lambda functions. gg's entire execution model is built on a cloud function provider (AWS Lambda, Google Cloud Functions). It cannot run on a private cluster. There is no "gg on k3s" mode.
- **Locus** (Pu et al., *NSDI*, 2019): addresses shuffling in serverless MapReduce but again requires a cloud FaaS substrate. The locality-enhancement it provides is exactly what DSF provides natively (run tasks where their data is), but for a cloud context.
- **Argo**: can express the fan-out DAG but all inter-task data goes to S3/MinIO. For a high-fan-out DAG where each simulation writes 100 MB of results, that is 100 × 100 MB = 10 GB going through MinIO, then 10 GB back out to the aggregator. On a private cluster with a 10 GbE switch, this is the bottleneck.

**What DSF enables.**
- Fan-out is expressed in the ODAG spec: aggregator lists all sim tasks as dependencies.
- Each sim task writes its result locally; the data-agent pushes it to the aggregator's node.
- DataReady at the aggregator: it starts as soon as all sim outputs have arrived on its node — not after all sim pods have exited.
- No FaaS platform needed. k3s + DSF is the full stack.

**References.**
- Fouladi, S. et al. "From Laptop to Lambda: Outsourcing Everyday Jobs to Thousands of Transient Functional Workers." *USENIX ATC* (2019).
- Pu, Q. et al. "Shuffling, Fast and Slow: Scalable Analytics on Serverless Infrastructure." *NSDI* (2019).

---

## Scenario 8: Adaptive Wide-Area Streaming Analytics with Edge Pre-Filtering

**Setting.** A logistics company monitors truck telemetry from 50 depots. Each depot has a local edge node (ARM, 4G uplink). Telemetry streams include GPS, engine sensors, and camera snapshots. A central 8-node cluster at HQ processes the data. The WAN links are bandwidth-constrained and asymmetric (4G: ~20 Mbps down, 5 Mbps up).

**Pipeline.**
```
telemetry ingest (edge node, per depot) → local pre-filter/compression → WAN transfer → HQ aggregator → anomaly detector → dashboard
```

**Why existing tools fail.**
- **AWStream** (Zhang et al., *SIGCOMM*, 2018): addresses exactly the bandwidth-adaptation problem for wide-area analytics. AWStream profiles application accuracy vs. bandwidth, degrades stream quality under congestion, and recovers when bandwidth improves. However, AWStream is an analytics adaptation system, not a pipeline orchestration framework. It does not deploy or manage the container-based pipeline stages across the edge and HQ cluster. You still need to separately deploy and manage the pre-filter, aggregator, and detector — AWStream provides no help with that.
- **Apache Flink**: can express the streaming DAG but requires a Flink cluster at both edge and HQ, or a unified cluster with WAN-spanning. Flink's checkpointing and state management go through a centralized state backend (RocksDB + S3). On a 5 Mbps 4G uplink, the state backend traffic alone can saturate the link.
- **Kafka**: requires a Kafka cluster. On a 4-core ARM edge node with 8 GB RAM, a Kafka broker consumes a significant portion of available resources. More importantly, every telemetry message traverses edge → Kafka broker (potentially at HQ) → consumer, adding broker-mediated latency on an already-constrained WAN link.

**What DSF enables.**
- The pipeline is one CDAG per depot. The pre-filter task has `constraints.nodeNames: [depot-i-node]`; the aggregator and detector run on HQ nodes.
- Direct ZMQ PUB/SUB: filtered telemetry goes depot-node → HQ aggregator node, no broker hop.
- The pre-filter task code can use `task.expected_data_size` and `task.dep_node()` to adapt compression aggressiveness based on the uplink budget.
- CDAG's reconcile loop handles 4G link dropouts: failed pods are recreated without manual intervention.

**References.**
- Zhang, B. et al. "AWStream: Adaptive Wide-Area Streaming Analytics." *SIGCOMM* (2018).
- Carbone, P. et al. "Apache Flink: Stream and Batch Processing in a Single Engine." *IEEE Data Engineering Bulletin* 36(4) (2015).

---

## Scenario 9: Programmable Network Telemetry Pipeline

**Setting.** A network operator deploys a telemetry collection and analysis pipeline across a cluster of 6 server nodes, each with a 100 GbE NIC and a direct connection to a top-of-rack switch. The goal is to collect per-flow telemetry, identify heavy hitters, detect anomalies, and generate alerts — a 3-stage pipeline that must run continuously.

**Pipeline.**
```
telemetry collector (per NIC-facing node) → heavy-hitter detector (aggregator node) → anomaly detector (ML node) → alerter
```

**Why existing tools fail.**
- **Sonata** (Gupta et al., *SIGCOMM*, 2018): a query-driven telemetry system that partitions queries between the data plane (P4 switches) and a stream processor (Apache Spark). Sonata handles the data-plane → stream-processor handoff efficiently. But Sonata is a query system, not a pipeline deployment framework. The stream processor it uses (Spark) treats all data as going through a central shuffle. Sonata does not address where to run the anomaly detector, how to pin the collector to the NIC-facing node, or how to pass intermediate results (heavy-hitter summaries) directly from the aggregator to the ML node without going through HDFS.
- **Kafka + consumer pods**: collector pods publish to a Kafka topic; detector and anomaly pods consume from it. On 100 GbE nodes generating high-rate telemetry, the broker becomes the bottleneck. Each message traverses: NIC node → Kafka broker node → detector node — two hops, one of which goes through a potentially congested broker.
- **Custom Kubernetes Deployments**: teams deploy collector DaemonSets and detector Deployments manually. There is no lifecycle management, no structured data flow, no DAG-aware restart policy, and no observability of the pipeline as a unit.

**What DSF enables.**
- The collector task has `constraints.nodeNames: [nic-node-i]` — it runs only on nodes directly connected to telemetry sources.
- Direct ZMQ PUB/SUB: high-rate telemetry summaries go collector → aggregator → ML node without a broker hop.
- CDAG models the continuous nature: all pods run forever; the reconcile loop restarts any failed component.
- The full pipeline is one YAML manifest, submitted once, observable as a single unit in the UI.

**References.**
- Gupta, A. et al. "Sonata: Query-Driven Streaming Network Telemetry." *SIGCOMM* (2018).
- Xu, M. et al. "Pingmesh: A Large-Scale System for Data Center Network Latency Measurement and Analysis." *SIGCOMM* (2015). (Illustrates the complexity of coordinated telemetry collection at scale without a structured pipeline framework.)

---

## Scenario 10: Multi-Hop Distributed Tracing and Root-Cause Analysis Pipeline

**Setting.** A microservices platform runs 30 services across 8 cluster nodes. A latency regression is observed. The team wants to run a one-shot diagnostic DAG: collect distributed traces from all 8 nodes simultaneously, merge and correlate them, run a root-cause analysis algorithm, and produce a report. This must run against the live cluster without disrupting production.

**Pipeline.**
```
trace collector (per node, 8 tasks in parallel) → merger (any node) → critical-path analyzer → report generator
```

**Why existing tools fail.**
- **Existing tracing systems (Jaeger, Zipkin, Dapper-style)**: these systems collect traces continuously into a central backend (Cassandra, Elasticsearch). They are always-on infrastructure, not on-demand diagnostic DAGs. You cannot say "collect traces for exactly 60 seconds, merge them, run this specific analysis algorithm, produce a report, then stop." Running an ad-hoc analysis against their central store requires querying through their API, which adds latency and cannot be expressed as a DAG with data flowing between stages.
- **Airflow**: no mechanism to pin the trace collector task to a specific node. Even if you used a PythonOperator on each host, Airflow's workers can run on any node; you cannot guarantee `collector-node-3` runs on node 3. Airflow's XCom (inter-task data) goes through the database — for 8 nodes worth of traces (potentially hundreds of MB total), the XCom DB is not appropriate.
- **Argo Workflows**: supports node affinity, but the trace data from 8 collectors must all go to an artifact store before the merger can start. The merger cannot start until all 8 uploads complete — no DataReady-per-child parallelism. On an 8-node cluster with a slow artifact store, this adds a synchronization barrier that DSF eliminates.
- **Manual scripts**: the current approach. SSH into each node, run the collector, scp results to a central node, run the merger. Sequential, fragile, no retry, no structured status.

**What DSF enables.**
- 8 collector tasks, each with `constraints.nodeNames: [node-i]` — guaranteed to run on the right node.
- All 8 collectors run simultaneously. Each writes its trace data locally and hands off to the data-agent.
- The merger task has `dependencies: [collector-1, ..., collector-8]`. DataReady triggers: the merger starts as soon as all 8 collector outputs have arrived on its node — without waiting for all 8 collector pods to exit. Collectors that finish early (lighter nodes) don't block collectors still running on busy nodes.
- The full diagnostic pipeline is one ODAG. It runs, completes, and is garbage-collected. No persistent infrastructure.

**References.**
- Sigelman, B. et al. "Dapper, a Large-Scale Distributed Systems Tracing Infrastructure." *Google Technical Report* (2010). (Canonical reference for distributed tracing; not a venue paper but widely cited as the origin of the field.)
- Zhao, X. et al. "Non-Intrusive Performance Profiling for Entire Software Stacks Based on the Flow Reconstruction Principle." *OSDI* (2016). (Root-cause analysis via tracing in production systems.)

---

## CDAG Scenarios (Continuous / Always-On Streaming Pipelines)

---

## Scenario 11: Continuous Network Intrusion Detection at Gateway Nodes

**Setting.** A university or enterprise network has 4 gateway nodes, each handling traffic for a different subnet. An ML-based IDS must run continuously: capture packets at each gateway, extract flow features, classify traffic, and forward alerts to a central SIEM. Packet capture must happen at the gateway node — routing raw packets to a central node first would saturate the uplinks.

**Pipeline.**
```
packet capture (per gateway node) → flow feature extractor → ML classifier (GPU node) → alert aggregator → SIEM forwarder
```

**Why existing tools fail.**
- **Kafka + Flink**: the canonical production approach. Kafka brokers receive flow records from each gateway; Flink consumers run the classifier. On a campus network at 10 Gbps, the broker is a bottleneck — every flow record traverses: gateway → Kafka broker → Flink worker, two hops, with broker disk I/O in between. The Kafka broker requires dedicated memory and CPU on the cluster, competing with the classifier for the GPU node's resources.
- **Custom DaemonSet + Deployment**: packet capture runs as a DaemonSet on gateway nodes; a separate Deployment runs the classifier. There is no structured data flow between them — teams wire this together with Kafka topics (back to the broker problem) or shared NFS (too slow for high-rate traffic). No lifecycle management ties the two components together as a single observable unit.
- **Snort / Suricata in standalone mode**: not Kubernetes-native. Running these on k3s requires custom wrappers; there is no concept of a multi-stage pipeline where feature extraction runs on one node type and ML classification runs on a GPU node.

**What DSF enables.**
- `constraints.nodeNames: [gateway-node-i]` pins packet capture to the right gateway — no raw packet data crosses the network for collection.
- ZMQ PUB/SUB: flow feature records go gateway → GPU classifier → alert aggregator directly, no broker hop.
- The CDAG reconcile loop restarts any failed pod within 30 seconds with no manual intervention. A gateway node reboot does not require an operator to restart the capture pod.
- All 4 per-gateway pipelines and the shared classifier are one YAML manifest, one observable unit.

**References.**
- Sommer, R. and Paxson, V. "Outside the Closed World: On Using Machine Learning for Network Intrusion Detection." *IEEE Symposium on Security and Privacy* (S&P) (2010). (Canonical paper on ML-based IDS; directly motivates the continuous, always-on pipeline model.)
- Mirsky, Y. et al. "Kitsune: An Ensemble of Autoencoders for Online Network Intrusion Detection." *NDSS* (2018). (Demonstrates that IDS must process traffic continuously at the network edge, not batch-upload to a central server.)

---

## Scenario 12: Always-On Live Video Transcoding for Adaptive Bitrate Streaming

**Setting.** A media company operates a private transcoding cluster (8 nodes, 2 with GPUs) for live sports broadcasts. Incoming RTMP streams from encoders arrive at designated ingest nodes. Each stream must be continuously decoded, transcoded to 4 bitrate ladders in parallel (GPU-accelerated), re-packaged, and pushed to a CDN origin. The pipeline runs for the duration of a broadcast (2–6 hours) and must survive individual pod failures without dropping the stream.

**Pipeline.**
```
RTMP ingest (per-stream ingest node) → decoder → GPU transcoder (4 parallel bitrates) → packager → CDN pusher
```

**Why existing tools fail.**
- **ExCamera** (Fouladi et al., *NSDI*, 2017): achieves low-latency video encoding by parallelizing across thousands of AWS Lambda functions. ExCamera is architected around cloud functions — it cannot run on a private cluster with 2 GPU nodes. Its execution model is also one-shot (encode a video file), not continuous (transcode an infinite live stream).
- **FFmpeg as a Kubernetes Job**: a common approach. One FFmpeg process per stream on one node. No GPU-CPU split across nodes — FFmpeg runs monolithically. If the node fails, the stream drops. There is no multi-stage pipeline with different node affinity per stage.
- **Apache Flink**: can model the stream processing DAG but has no native concept of pinning a stage to a GPU node. Flink's network stack routes data through its managed network buffers and memory pools — not direct socket-to-socket. Adding a GPU transcoding operator in Flink requires a custom source/sink with JNI, significant engineering effort with no benefit over DSF's container-per-stage model.

**What DSF enables.**
- The ingest task has `constraints.nodeNames: [ingest-node-i]` — the RTMP stream arrives on a specific node and stays there.
- The transcoder task has `constraints.nodeNames: [gpu-node-1, gpu-node-2]` — the controller picks one of the two GPU nodes.
- ZMQ PUB/SUB: decoded frames go ingest node → GPU transcoder node, one hop, no broker.
- CDAG RestartPolicy: Always — if the transcoder pod crashes mid-broadcast, the reconcile loop recreates it on a GPU node within 30 seconds, using the same ZMQ service endpoint. The ingest pod does not restart.

**References.**
- Fouladi, S. et al. "Encoding, Fast and Slow: Low-Latency Video Processing Using Thousands of Tiny Threads of Execution." *NSDI* (2017). (ExCamera: parallelized video encoding via Lambda — the cloud-centric counterpart to what DSF enables on-premises.)
- Hung, C.-C. et al. "VideoStorm: Live Video Analytics at Scale with Approximation and Delay-Tolerance." *NSDI* (2017). (Demonstrates the gap between what video pipelines need — node-level resource control, continuous operation — and what existing schedulers provide.)

---

## Scenario 13: Continuous Online Learning Pipeline (Feature Extraction → Incremental Training → Serving)

**Setting.** An e-commerce platform runs a recommender system on a 12-node on-premises cluster. User interaction events arrive continuously from a front-end log collector. The pipeline: parse and featurize events, update an embedding model incrementally (GPU node), validate the updated model, and route predictions through the serving layer. The model must be updated every few minutes with new data. No cloud; GDPR requires all user data to stay on-premises.

**Pipeline.**
```
event collector (log-facing node) → feature extractor → incremental trainer (GPU node) → model validator → prediction server → response logger
```

**Why existing tools fail.**
- **Clipper** (Crankshaw et al., *NSDI*, 2017): a model serving system focused on low-latency prediction with batching and model versioning. Clipper handles the serving stage only — it does not model the upstream feature extraction and training pipeline. Connecting a feature extractor to Clipper requires a separate message bus (Kafka), adding a broker hop between feature extraction and the prediction server.
- **TFX** (Baylor et al., *KDD*, 2017): Google's production ML pipeline framework. TFX is a batch-oriented system: it runs a training job on a snapshot of data, evaluates, and pushes the model. It does not model continuous incremental training from a live stream. TFX also requires Google's infrastructure (Beam runners, TFServing on cloud) or a significant on-premises adaptation effort.
- **Kubeflow Pipelines**: designed for batch ML training jobs, not streaming. A Kubeflow pipeline run is triggered; it does not loop continuously. Running Kubeflow in a "continuous" mode requires external orchestration (a CronJob triggering pipeline runs), introducing gaps between runs and no DAG-level coordination between the feature extractor and the trainer.

**What DSF enables.**
- The CDAG runs indefinitely: the event collector continuously publishes to the feature extractor, which publishes to the trainer, which publishes updated model weights to the validator and serving node.
- `constraints.nodeNames: [gpu-node]` pins the trainer to the GPU node without requiring Kubeflow or any ML platform infrastructure.
- Model weights between trainer and serving node are small (MB-range) — ZMQ PUB/SUB delivers them in milliseconds with no broker hop.
- The reconcile loop restarts a crashed trainer pod without restarting the event collector or the serving layer — partial failure recovery at the task level.

**References.**
- Crankshaw, D. et al. "Clipper: A Low-Latency Online Prediction Serving System." *NSDI* (2017).
- Baylor, D. et al. "TFX: A TensorFlow-Based Production-Scale Machine Learning Platform." *KDD* (2017). (Explicitly batch-oriented; motivates the gap for continuously updating models from live streams.)

---

## Scenario 14: Shared Edge Cluster for Multi-Robot Perception and SLAM

**Setting.** A warehouse deploys 8 autonomous mobile robots (AMRs). Each robot has a LiDAR and a camera but limited on-board compute (ARM SoC). A shared edge cluster of 6 nodes (4 CPU, 2 GPU) offloads perception. Each robot streams sensor data to its assigned edge node; the pipeline runs the full perception stack continuously: sensor fusion, object detection, SLAM map update, and command generation.

**Pipeline (per robot).**
```
sensor receiver (robot-assigned edge node) → sensor fusion → object detector (GPU node) → SLAM updater (map-owning node) → command publisher (back to robot)
```

**Why existing tools fail.**
- **ROS 2**: the standard framework for robot software. ROS 2 is designed for a single robot's compute graph, not for offloading to a shared cluster. Running ROS 2 nodes across a cluster requires DDS configuration across machines, lacks Kubernetes lifecycle management, and has no concept of scheduling ROS nodes onto specific Kubernetes nodes based on resource constraints.
- **Kafka + consumer Deployments**: wiring sensor data from 8 robots through Kafka topics adds broker-mediated latency. SLAM requires low-latency feedback (the map update must return a navigation command quickly enough for the robot to react). Each message traverses: edge node → Kafka broker → GPU worker → broker again → command subscriber — 4 hops, each adding latency.
- **Custom Kubernetes Deployments**: teams deploy a Deployment per pipeline stage. No guarantee that the sensor receiver for robot-3 lands on the node physically closest to robot-3's WiFi AP. No DAG-level lifecycle management; a crashed SLAM updater takes down no other stage, but there is no automatic restart coordination.

**What DSF enables.**
- `constraints.nodeNames: [edge-node-i]` pins each robot's sensor receiver to the designated edge node (physically closest to the robot's AP, minimizing WiFi latency).
- `constraints.nodeNames: [gpu-node-1, gpu-node-2]` routes object detection to a GPU node; the CDAG controller load-balances across the two.
- Direct ZMQ: point cloud data (sensor fusion output, ~5 MB/s per robot) goes directly from the edge node to the GPU node, not through a broker.
- CDAG's reconcile loop restarts a failed SLAM updater and reconnects the ZMQ topology automatically within one reconcile interval.

**References.**
- Boroujerdian, B. et al. "MAVBench: Micro Aerial Vehicle Benchmarking." *IEEE/ACM International Symposium on Microarchitecture* (MICRO) (2018). (Identifies perception, planning, and actuation as a latency-sensitive pipeline with distinct compute requirements per stage — the same structure DSF CDAGs model.)
- Liu, L. et al. "Edge Assisted Real-time Object Detection for Mobile Augmented Reality." *MobiCom* (2019). (Demonstrates that offloading perception to an edge server requires tight coupling between the ingest node and the compute node — exactly what DSF's node-pinning addresses.)

---

## Scenario 15: Distributed Log Preprocessing and Anomaly Detection (Always-On)

**Setting.** A production cluster of 10 nodes runs microservices generating 500 MB/hour of structured logs per node (5 GB/hour total). A streaming log analysis pipeline must collect logs at each node, parse and filter them locally (reducing volume by ~90%), aggregate the filtered events, and run anomaly detection continuously. Shipping raw logs to a central store first wastes 90% of network bandwidth.

**Pipeline.**
```
log collector (per node) → local parser + filter → cross-node aggregator → anomaly detector → alert dispatcher
```

**Why existing tools fail.**
- **ELK Stack (Logstash + Elasticsearch)**: the standard production approach. Logstash ships full raw logs to Elasticsearch before any filtering. On a 10-node cluster generating 5 GB/hour, this means 5 GB/hour of log traffic on the internal network — just for collection, before any analysis. Elasticsearch is a centralized store; the anomaly detection query runs against it, adding query latency on top of ingestion latency.
- **Fluentd + Kafka**: Fluentd agents run on each node and ship to Kafka. Better than ELK for throughput but the broker is still a mandatory hop. Kafka requires a separate cluster or at minimum 3 broker pods, consuming memory on the already-loaded cluster. The anomaly detection consumer has no locality guarantee — it can run on any node, regardless of where the aggregator ran.
- **Apache Flink**: can run the parse-filter-aggregate-detect pipeline but its network stack passes all data through Flink's managed memory and network buffers. Data from node-local collectors goes through Flink's task manager network stack, not direct ZMQ sockets. More critically, Flink requires a JobManager and TaskManager cluster running at all times, consuming resources even when log volume is low.

**What DSF enables.**
- Log collector + parser/filter tasks have `constraints.nodeNames: [node-i]` — local filtering runs on-node, reducing cross-node traffic from 5 GB/hour to ~500 MB/hour.
- The aggregator and detector run on dedicated analysis nodes; ZMQ PUB/SUB routes filtered events directly from each collector to the aggregator.
- No broker, no Flink cluster. Just k3s + the CDAG controller + the data-agent DaemonSet.
- CDAG's RestartPolicy: Always keeps the pipeline alive across node failures without manual restart.

**References.**
- Murray, D.G. et al. "Naiad: A Timely Dataflow System." *SOSP* (2013). (Naiad is the most general treatment of continuous dataflow computation; it motivates why streaming pipelines need direct data routing between stages rather than shared storage hops — the same principle DSF implements at the container level.)
- Kulkarni, S. et al. "Twitter Heron: Stream Processing at Scale." *SIGMOD* (2015). (Heron's architecture critique of Storm is directly relevant: centralized shuffle is a bottleneck. Heron routes data directly between stages — the same insight DSF applies at the container/ZMQ level, without requiring a Heron cluster.)

---

## Scenario 16: 5G MEC Augmented Reality Offload Pipeline

**Setting.** A 5G deployment at a stadium uses 6 Mobile Edge Computing (MEC) nodes, each colocated with a base station (gNB). AR headsets connect to the nearest gNB. The AR rendering pipeline: receive frame from headset → run neural rendering / object overlay (GPU MEC node) → compress and return rendered frame. Round-trip latency must stay under 20 ms. All compute must happen at the MEC tier — cloud round-trip adds 40–80 ms.

**Pipeline.**
```
frame receiver (gNB-local MEC node) → neural renderer (GPU MEC node) → encoder → frame sender (back to headset)
```

**Why existing tools fail.**
- **Cloud-based AR rendering**: the obvious but infeasible baseline. AWS/GCP round-trip latency from a stadium is 40–80 ms, exceeding the 20 ms budget for comfortable AR. This is the entire motivation for MEC — but existing MEC frameworks (ETSI MEC APIs, AWS Wavelength, Azure Edge Zones) are cloud-vendor-specific and do not provide a Kubernetes-native DAG pipeline for private 5G deployments.
- **Kafka + consumer pods**: a Kafka broker at the MEC tier still adds ~1–5 ms of broker-mediated latency per message hop, on top of the encoding/decoding latency. At 60 fps, 5 ms per hop consumes 25% of the latency budget on broker overhead alone.
- **Kubernetes Deployments without node affinity**: the frame receiver might land on MEC node 3 while the neural renderer lands on MEC node 5. The frame data crosses the MEC backhaul network unnecessarily — backhaul between MEC nodes adds 1–3 ms.

**What DSF enables.**
- `constraints.nodeNames: [mec-node-i]` pins the frame receiver to the MEC node collocated with the gNB serving each headset. Frame data never leaves the correct MEC node for the receive stage.
- `constraints.nodeNames: [gpu-mec-node-i]` pins rendering to the GPU-equipped MEC node closest to the receiver.
- ZMQ PUB/SUB delivers compressed frames directly between stages without broker latency.
- When a headset roams to a different gNB, the CDAG spec can be resubmitted with updated `constraints.nodeNames`; the reconcile loop migrates the receiver task.

**References.**
- Shi, W. et al. "Edge Computing: Vision and Challenges." *IEEE Internet of Things Journal* 3(5), 637–646 (2016). (Foundational paper establishing the MEC latency argument — cloud round-trips are incompatible with real-time applications. Widely cited as the motivation for edge-local pipeline processing.)
- Liu, L. et al. "Edge Assisted Real-time Object Detection for Mobile Augmented Reality." *MobiCom* (2019). (Demonstrates 20 ms latency targets for AR offload to edge and the requirement for tight edge-node-to-GPU-node locality in the processing pipeline.)

---

## Scenario 17: Streaming Financial Market Data Pipeline (On-Premises, Latency-Sensitive)

**Setting.** A quantitative trading firm runs a proprietary cluster (16 nodes, 2 with FPGA/SmartNIC for kernel-bypass networking). Market data from multiple exchanges arrives at designated feed handlers on the FPGA nodes. The pipeline: receive and normalize market data → compute real-time signals (order book imbalance, momentum) → run risk checks → generate orders. This runs continuously during trading hours with microsecond-to-millisecond latency requirements. All compute is on-premises; no cloud.

**Pipeline.**
```
feed handler (FPGA/SmartNIC node) → normalizer → signal computer (CPU-intensive node) → risk engine → order generator
```

**Why existing tools fail.**
- **Apache Kafka**: introduces broker-mediated latency measured in milliseconds. At market data rates (millions of messages/second for Level 2 book data), Kafka's disk write path and broker serialization are incompatible with sub-millisecond signal computation latency requirements. Papers on low-latency systems (Belay et al., *OSDI*, 2014) show that even OS network stacks add microseconds that matter in this domain.
- **Naiad** (Murray et al., *SOSP*, 2013): Naiad's timely dataflow model can express the pipeline and handles streaming data efficiently. However, Naiad is a .NET framework — it runs as a single distributed program, not as isolated containers with independent fault domains. A bug in the signal computer crashes the entire Naiad process, taking down the feed handler and risk engine. Naiad also has no concept of pinning a stage to a specific node type (FPGA vs. CPU).
- **Flink / Spark Streaming**: both route data through managed network buffers and a centralized coordinator. On a 16-node cluster with FPGA nodes, Flink has no mechanism to route market data directly from the FPGA's kernel-bypass NIC to the signal computer without going through Flink's network stack overhead.

**What DSF enables.**
- `constraints.nodeNames: [fpga-node-1, fpga-node-2]` pins the feed handler to the FPGA-equipped nodes where market data arrives via kernel-bypass NIC.
- ZMQ PUSH/PULL (for CDAG: PUB/SUB) connects the feed handler directly to the signal computer on a designated CPU node — one hop, no broker, no disk write.
- Each stage is an isolated container: a risk engine crash does not affect the feed handler. The reconcile loop restarts the risk engine within seconds with no operator intervention.
- CDAG models the always-on nature of a trading session; the ODAG model handles the end-of-day reconciliation batch.

**References.**
- Murray, D.G. et al. "Naiad: A Timely Dataflow System." *SOSP* (2013). (The most rigorous treatment of continuous low-latency streaming pipelines; Naiad's limitations — single-runtime, no container isolation, no node pinning — are exactly the gaps DSF fills.)
- Belay, A. et al. "IX: A Protected Dataplane Operating System for High Throughput and Low Latency." *OSDI* (2014). (Establishes the latency budget available for networking in latency-sensitive systems; motivates why each broker hop in Kafka or Flink is a meaningful cost.)

---

## Scenario 18: Continuous Environmental Sensor Calibration and Aggregation Network

**Setting.** An air quality research group deploys 12 low-cost sensor nodes across a city (mounted on lampposts and buildings). Each node has CO₂, NO₂, PM2.5, and temperature sensors. Raw sensor readings have per-unit drift and must be calibrated continuously against a reference model. The pipeline: read raw sensors → apply calibration correction → spatial aggregation → anomaly spike detection → public data API. This runs 24/7 for months at a time.

**Pipeline.**
```
sensor reader (per sensor node) → calibration corrector → spatial aggregator (per district) → anomaly detector → API publisher
```

**Why existing tools fail.**
- **MQTT + cloud broker (AWS IoT, Azure IoT Hub)**: the standard IoT approach. Every sensor reading goes to a cloud broker — 12 nodes × 1 reading/second × 24/7 = significant cloud data egress costs. More importantly, the calibration correction model is specific to each sensor unit; running it in the cloud requires sending uncalibrated data to the cloud and back, or maintaining per-sensor state in a cloud function — complex and expensive. The cloud round-trip also adds 50–200 ms latency to every reading.
- **InfluxDB + Grafana**: the common time-series stack. This is a storage and visualization system, not a pipeline processing framework. The calibration correction and spatial aggregation must be implemented separately (Python scripts, cron jobs), with no structured data flow or automatic restart.
- **Kafka on constrained hardware**: each sensor node is a Raspberry Pi-class device (4 GB RAM, 4-core ARM). A Kafka broker alone requires 1–2 GB JVM heap. Running a broker on the sensor node is not feasible; running a central broker requires all raw sensor data to leave each node, losing the opportunity to do local calibration (which reduces cross-node traffic).

**What DSF enables.**
- `constraints.nodeNames: [sensor-node-i]` pins the sensor reader and calibration corrector to each physical device — calibration runs locally, sending corrected (and compressed) readings cross-node.
- One CDAG covers all 12 sensors: 12 reader/corrector pairs → 4 district aggregators → 1 anomaly detector → 1 API publisher. One manifest, one observable unit.
- RestartPolicy: Always handles node reboots (power outages, firmware updates) without operator intervention. The reconcile loop detects the missing pod and recreates it on the correct node.
- ZMQ PUB/SUB: calibrated readings go sensor-node → district aggregator directly; no cloud round-trip, no broker.

**References.**
- Balaji, B. et al. "Sentinel: Occupancy Based HVAC Actuation using Existing WiFi Infrastructure within Commercial Buildings." *SenSys* (2013). (Representative of always-on sensor pipelines on resource-constrained nodes where a broker-per-node approach is infeasible; ACM SenSys is the top venue for sensor systems.)
- Gupta, A. et al. "Sonata: Query-Driven Streaming Network Telemetry." *SIGCOMM* (2018). (The data-plane pre-filtering insight in Sonata — reduce data volume before it crosses a network boundary — is the same insight DSF applies with local calibration + filtering before spatial aggregation.)

---

## Scenario 19: Continuous Radio Telescope Data Reduction Pipeline

**Setting.** A radio telescope array (e.g., a LOFAR station or SKA-Low prototype) generates continuous visibility data from a correlator. The data rate is 10–40 Gbps from the correlator node. The pipeline: receive visibilities → RFI flagging (CPU-intensive, must run immediately to reduce data volume) → calibration (GPU-accelerated) → imaging (high-memory node) → source finding → archive. This runs 24/7 during observation campaigns. Intermediate data volumes are enormous; no shared filesystem can absorb the raw correlator output.

**Pipeline.**
```
visibility receiver (correlator node) → RFI flagger (CPU nodes, parallel) → calibrator (GPU node) → imager (high-memory node) → source finder → archive writer
```

**Why existing tools fail.**
- **Nextflow + NFS**: the standard for offline radio astronomy pipelines (e.g., LOFAR's standard imaging pipeline uses Nextflow). For offline processing this works — data is already on disk. For a real-time continuous pipeline, writing raw visibilities to NFS at 10–40 Gbps before processing is not feasible; NFS cannot absorb that write rate.
- **CASA (Common Astronomy Software Applications)**: the standard radio astronomy toolkit. CASA is a monolithic application, not a distributed pipeline framework. Running CASA stages across multiple cluster nodes requires manual scripting, not a structured DAG with lifecycle management.
- **Apache Kafka**: a 10–40 Gbps data stream is far beyond what a Kafka broker can absorb (typical throughput: 1–2 Gbps for a well-tuned broker). Kafka was not designed for high-rate scientific data streams.

**What DSF enables.**
- `constraints.nodeNames: [correlator-node]` pins the visibility receiver to the correlator output node — data does not move until it has been RFI-flagged and its volume reduced.
- `constraints.nodeNames: [high-memory-node]` pins the imager to the node with sufficient RAM for the imaging FFT (often 256–512 GB for full-Stokes wide-field imaging).
- ZMQ PUB/SUB: calibrated visibilities go directly from the GPU calibrator node to the imager — one hop, no intermediate store.
- CDAG models the continuous observation: the pipeline runs for the duration of the observing run. When the telescope slews to a new target, only the calibrator configuration changes; the pipeline topology stays alive.

**References.**
- Di Tommaso, P. et al. "Nextflow enables reproducible computational workflows." *Nature Biotechnology* 35, 316–319 (2017). (Nextflow is the current standard for offline radio astronomy pipelines; its shared-filesystem assumption is the gap DSF addresses for online/real-time processing.)
- Topcuoglu, H. et al. "Performance-Effective and Low-Complexity Task Scheduling for Heterogeneous Computing." *IEEE TPDS* 13(3) (2002). (HEFT is directly relevant: radio telescope pipelines have well-characterized task runtimes and data volumes, making them ideal candidates for HEFT-based scheduling — exactly the profiling input DSF's scheduler interface accepts.)

---

## Scenario 20: Continuous Distributed Model Serving with A/B Traffic Splitting

**Setting.** A company runs a 10-node on-premises inference cluster serving a recommendation model. Two model versions are always live (production and canary). Incoming requests are split: 90% to the production model, 10% to the canary. Both models run simultaneously on GPU nodes. A metrics collector aggregates latency and accuracy signals from both paths and feeds a routing controller that adjusts the traffic split in real time.

**Pipeline.**
```
request receiver → traffic splitter → [production model (GPU), canary model (GPU)] → response merger → response sender
                                          ↓                         ↓
                                    metrics collector → routing controller (feedback loop)
```

**Why existing tools fail.**
- **Clipper** (Crankshaw et al., *NSDI*, 2017): supports model versioning and A/B testing but routes all traffic through Clipper's own front-end, which is a single point of failure and a potential latency bottleneck. Clipper does not model the metrics collection and routing controller as first-class pipeline stages — those are external systems the operator must wire up separately.
- **Seldon Core / KServe**: Kubernetes-native model serving with canary deployment support. Seldon handles the traffic split via Istio or Ambassador ingress, not a DAG pipeline. The metrics feedback loop (collecting accuracy signals from both models and adjusting the split) must be implemented as a separate controller outside Seldon. There is no mechanism to express "metrics collector must run on the same node as the model it's collecting from" — which matters when collecting GPU memory profiles or latency histograms.
- **Argo Rollouts**: handles canary deployments for Kubernetes workloads based on Prometheus metrics. Argo Rollouts is a deployment strategy tool, not a streaming pipeline — it does not model real-time request routing, per-request response merging, or a feedback loop operating at the request level.

**What DSF enables.**
- The CDAG expresses the full pipeline including the feedback loop: splitter → models → merger → metrics → controller → back to splitter via env var update.
- `constraints.nodeNames: [gpu-node-1]` pins production model; `constraints.nodeNames: [gpu-node-2]` pins canary model — each model gets its own GPU node, no resource contention.
- ZMQ PUB/SUB: request batches go splitter → each model directly; latency metrics go from each model → metrics collector directly. No service mesh, no sidecar proxies, no extra hops.
- CDAG's reconcile loop restarts a crashed canary model within 30 seconds, automatically returning it to its assigned GPU node. The production path is unaffected.

**References.**
- Crankshaw, D. et al. "Clipper: A Low-Latency Online Prediction Serving System." *NSDI* (2017). (Clipper motivates the continuous model serving problem; its single-front-end architecture is the gap DSF's DAG model fills for multi-model pipelines with feedback loops.)
- Gujarati, A. et al. "Serving DNNs like Clockwork: Performance Predictability from the Bottom Up." *OSDI* (2020). (Clockwork's key insight — predictable model serving requires tight control over where and when each model runs — is exactly what DSF's `constraints.nodeNames` provides at the DAG level.)

---

## Summary Table

| # | Domain | DAG kind | Primary failure in existing tools |
|---|---|---|---|
| 1 | Genomics / bioinformatics | ODAG | Shared filesystem (NFS) bottleneck; no data-local scheduling |
| 2 | Edge video analytics | ODAG | Centralized back-end assumption; no per-camera node pinning |
| 3 | Network measurement campaigns | ODAG | No DAG framework supports node-targeted experiments |
| 4 | Medical ML inference | ODAG | Single-stage serving systems; artifact-store two-hop penalty |
| 5 | Multi-stage split inference | ODAG | Binary cloud split (Neurosurgeon); no multi-stage on-prem DAG |
| 6 | Simulation post-processing (HPC) | ODAG | Shared filesystem assumed; 300 GB checkpoints cannot be staged |
| 7 | Monte Carlo fan-out (private cluster) | ODAG | FaaS requires cloud; Argo bottlenecks on artifact store |
| 8 | Wide-area telemetry (depot + HQ) | ODAG | AWStream adapts quality but does not deploy the pipeline |
| 9 | Programmable network telemetry | ODAG | Kafka is the bottleneck; Sonata is a query system, not a deployer |
| 10 | Distributed tracing + root-cause analysis | ODAG | No tool supports node-pinned parallel collection + DAG merge |
| 11 | Network intrusion detection | CDAG | Kafka broker bottleneck at 10 Gbps; DaemonSet has no DAG lifecycle |
| 12 | Live video transcoding | CDAG | ExCamera requires cloud Lambda; FFmpeg is monolithic, no fault tolerance |
| 13 | Online ML (feature → train → serve) | CDAG | Clipper is single-stage; TFX is batch-only; Kubeflow is triggered, not continuous |
| 14 | Multi-robot edge perception + SLAM | CDAG | ROS 2 is not cluster-native; Kafka adds broker latency incompatible with SLAM feedback |
| 15 | Distributed log preprocessing | CDAG | ELK ships raw logs (wastes 90% bandwidth); Flink requires a separate cluster |
| 16 | 5G MEC augmented reality offload | CDAG | Cloud round-trip exceeds latency budget; Kafka adds broker hop on constrained MEC |
| 17 | Financial market data pipeline | CDAG | Kafka adds ms latency; Naiad is single-runtime, no container isolation or node pinning |
| 18 | Environmental sensor calibration | CDAG | MQTT sends uncalibrated data to cloud; Kafka JVM too heavy for ARM sensor nodes |
| 19 | Radio telescope data reduction | CDAG | NFS cannot absorb 10–40 Gbps; Kafka was not designed for scientific data rates |
| 20 | Continuous A/B model serving | CDAG | Clipper is single-front-end; Seldon routes via Istio, not a pipeline; Argo Rollouts is deployment-only |
