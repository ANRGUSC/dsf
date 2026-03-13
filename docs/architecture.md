# DSF Architecture Reference

This document describes the internal design of DSF in detail. For a quick-start guide see [README.md](../README.md).

---

## Table of Contents

1. [High-level overview](#1-high-level-overview)
2. [CRDs and the API surface](#2-crds-and-the-api-surface)
3. [ODAG controller](#3-odag-controller)
4. [CDAG controller](#4-cdag-controller)
5. [Scheduling](#5-scheduling)
6. [Python SDK and transport layer](#6-python-sdk-and-transport-layer)
7. [UI server](#7-ui-server)
8. [React frontend](#8-react-frontend)
9. [Data flow end-to-end](#9-data-flow-end-to-end)
10. [Key design decisions and trade-offs](#10-key-design-decisions-and-trade-offs)

---

## 1. High-level overview

DSF is a thin scheduling layer on top of Kubernetes. It adds two Custom Resource Definitions:

| Kind | Plural | Purpose |
|---|---|---|
| `ODAG` | `odags` | One-shot pipeline; every task runs exactly once; succeeds or fails |
| `CDAG` | `cdags` | Streaming pipeline; tasks run forever; failed pods are restarted |

Users write task code as container images using the DSF Python SDK. They describe the task graph in a YAML manifest and submit it via the `dsf` CLI or `kubectl apply`. Two Go controllers translate the CRs into Kubernetes Pods and ClusterIP Services.

---

## 2. CRDs and the API surface

Both CRDs live in group `dsf.io/v1`.

### `ODAG` spec fields

| Field | Type | Description |
|---|---|---|
| `spec.scheduler` | string | Accepted but currently unused — placement is constraint-aware random. |
| `spec.retryPolicy.maxRetries` | int | Retries per task on failure. |
| `spec.tasks[].name` | string | Unique task name within the ODAG. |
| `spec.tasks[].image` | string | Container image. |
| `spec.tasks[].command` | []string | Entry-point override. |
| `spec.tasks[].dependencies` | []string | Names of tasks that must complete before this one starts. |
| `spec.tasks[].resources.cpu` | string | CPU request (K8s quantity). |
| `spec.tasks[].resources.memory` | string | Memory request (K8s quantity). |
| `spec.tasks[].dataSize` | string | Estimated output size — reserved for future HEFT integration. |
| `spec.tasks[].runtime` | int | Estimated runtime in seconds — reserved for future HEFT integration. |
| `spec.tasks[].constraints.nodeNames` | []string | Allowed node names. Controller picks a random node from this list intersected with schedulable cluster nodes. Omit to allow any node. |

### `ODAG` status fields written by the controller

| Field | Description |
|---|---|
| `status.phase` | `Pending` → `Scheduling` → `Running` → `Succeeded` / `Failed` |
| `status.startTime` | RFC 3339 timestamp when first pod started. |
| `status.completionTime` | RFC 3339 timestamp when last pod finished. |
| `status.makespan` | Wall-clock duration in seconds (`completionTime − startTime`). |
| `status.tasks[].name` | Task name. |
| `status.tasks[].phase` | Per-task phase. |
| `status.tasks[].node` | Node the task ran on (from `pod.Spec.NodeName`). |
| `status.tasks[].podName` | Pod name. |
| `status.tasks[].startTime` | Pod start time. |
| `status.tasks[].completionTime` | Container finish time. |

### `CDAG` spec fields

Same as ODAG except:
- `spec.restartPolicy` — `Always`, `OnFailure`, or `Never` (propagated to Pods).
- `spec.tasks[].replicas` — number of pod replicas per task.
- No `retryPolicy` (the reconcile loop handles restarts).
- `spec.tasks[].constraints.nodeNames` — same semantics as ODAG.

### `CDAG` status fields written by the controller

| Field | Description |
|---|---|
| `status.phase` | `Pending` → `Running` / `Degraded` / `Failed` |
| `status.tasks[].name` | Task name. |
| `status.tasks[].node` | Node the task's pod is running on (from `pod.Spec.NodeName`). |
| `status.tasks[].desiredReplicas` | Desired number of replicas. |
| `status.tasks[].readyReplicas` | Currently running pod count. |
| `status.tasks[].podNames` | Names of live pods for this task. |

---

## 3. ODAG controller

**Entry point:** `cmd/odag-controller/main.go`

### Watch loop

The controller opens a Kubernetes **dynamic watch** on `odags.dsf.io` across all namespaces. For each `ADDED` event it launches a goroutine that orchestrates the ODAG lifecycle. A `processedODAGs sync.Map` prevents duplicate processing.

### Scheduling / placement phase

1. Calls `getNodes()` to list all schedulable cluster nodes (filters out nodes with `NoSchedule` taints).
2. Calls `assignTasks()` — the ODAG-specific placement function — which picks a random node from each task's `constraints.nodeNames` intersected with the cluster nodes. If a task has no constraints, any node is eligible.
3. Updates `status.phase = Scheduling`.

### Pod + Service creation

All pods are created **simultaneously** (not layer-by-layer). For each task:

1. Creates a **ClusterIP Service** named `{odag-name}-{task-name}` in the same namespace.
   - Label selector: `dsf-odag={odag-name}, dsf-task={task-name}`.
   - Port 5555 (ZMQ).
2. Creates a **Pod** with:
   - `NodeAffinity` (RequiredDuringScheduling) constraining the pod to the assigned node.
   - `imagePullPolicy: Always`.
   - Env vars: `DSF_PEER_<TASK_UPPER>=zmq://{svc}.{namespace}.svc.cluster.local:5555` for every peer task.
   - `DSF_TRANSPORT_PATTERN=pushpull`, `DSF_RECV_PORT=5555`.
   - Owner reference pointing to the ODAG CR (enables garbage collection on delete).
3. Sets `status.phase = Running`.

### Pod watch loop

A second watch (`pods` with label `dsf-odag={odag-name}`) monitors all task pods and fires `reconcileODAG` on every event. This function:

1. Reads each pod's `status.phase` and `status.containerStatuses`.
2. Writes the per-task status array (including `node` from `pod.Spec.NodeName`) via a `MergePatch` on the `/status` subresource.
3. If all pods `Succeeded`: computes makespan, writes `status.phase = Succeeded`.
4. If any pod `Failed` and retries are exhausted: writes `status.phase = Failed`.

### Retry logic

On pod failure the controller checks `pod.Status.ContainerStatuses[].RestartCount` against `spec.retryPolicy.maxRetries`. If retries remain it deletes the failed pod and recreates it. If retries are exhausted the ODAG fails.

---

## 4. CDAG controller

**Entry point:** `cmd/cdag-controller/main.go`

### Watch loop

Same dynamic watch pattern as the ODAG controller, on `cdags.dsf.io`. A `deployedCDAGs sync.Map` prevents `deployCDAG` from running twice for the same CR.

### Initial placement

1. Calls `getNodes()` to list schedulable cluster nodes.
2. Calls `assignTasks()` — the CDAG-specific placement function, separate from the ODAG one. Picks a random allowed node per task using `pickNode(constraints, nodes)`.
3. Writes initial `status.tasks[]` with the planned node assignments (replicas not yet ready).

### Service + Pod creation

For each task:
1. Creates a **ClusterIP Service** (same pattern as ODAG, label `dsf-cdag={cdag-name}`).
2. Creates Pods with:
   - `restartPolicy` from `spec.restartPolicy` (default `Always`).
   - `DSF_TRANSPORT_PATTERN=pubsub`, `DSF_PUB_PORT=5555`.
   - Peer endpoints injected as env vars.
   - `NodeAffinity` to the assigned node.
   - Owner reference to the CDAG CR.

### Reconcile loop

Every **30 seconds**, a background goroutine runs `reconcileCDAG()` for every known CDAG:

1. Lists all pods matching `dsf-cdag={name}, dsf-task={task}` for each task.
2. Counts running (`PodRunning`) and alive (`PodRunning | PodPending`) pods.
3. Records `pod.Spec.NodeName` from the first running pod as the task's assigned node.
4. If `alive < desiredReplicas`: deletes any `Failed` pods by name and recreates them with a fresh `pickNode()` call.
5. Calls `updateCDAGTaskStatuses()` to write the full `status.tasks[]` (name, node, desiredReplicas, readyReplicas, podNames).
6. Sets `status.phase = Running` if all tasks have full replicas, `Degraded` otherwise.

---

## 5. Scheduling

### Current approach: constraint-aware random placement

Both controllers implement their own independent `assignTasks()` function. This was a deliberate decision — ODAG and CDAG have fundamentally different execution models, and their scheduling logic will diverge as more sophisticated algorithms are added.

**`assignTasks` (ODAG / CDAG):**
```
For each task:
  allowed = constraints.nodeNames ∩ schedulableClusterNodes
  if allowed is empty: allowed = schedulableClusterNodes
  assign task → random node from allowed
```

### HEFT (reference implementation)

`pkg/scheduler/heft.go` contains a full Go implementation of the **Heterogeneous Earliest Finish Time** algorithm. It is not currently called by either controller but is kept for reference and future integration. The Python equivalent lives in `sdk/python/dsf_sdk/schedulers/heft.py`.

HEFT minimises estimated makespan by computing upward ranks and greedily assigning tasks to the node that gives the earliest finish time, accounting for per-node compute speed and inter-task communication costs.

---

## 6. Python SDK and transport layer

**Package:** `sdk/python/dsf_sdk/`

### User API (`api.py`)

```python
class DSFTask:
    def send(self, peer: str, data: Any) -> None
    def recv(self, peer: str | None = None) -> Any
    def close(self) -> None
```

`send`/`recv` accept any JSON-serialisable value. The SDK serialises with `json.dumps` / `json.loads`.

### Transport selection (`transport/router.py`)

The transport is selected at startup from `DSF_TRANSPORT_PATTERN`:

| Value | Transport class | Use case |
|---|---|---|
| `pushpull` (default) | `ZmqPushPullTransport` | One-shot ODAGs |
| `pubsub` | `ZmqPubSubTransport` | Continuous CDAGs |

### PUSH/PULL transport (`transport/zeromq.py`)

```
Receiver: binds PULL socket at *:DSF_RECV_PORT (default 5555)
Sender:   connects PUSH socket to peer's ClusterIP Service endpoint
```

Key reliability details:
- **`LINGER=5000`**: ZMQ waits up to 5 s for the I/O thread to flush before `socket.close()` returns.
- **0.2 s sleep after connect**: Prevents the "slow-joiner" problem where the first message is dropped.
- **0.3 s sleep after send**: Gives the I/O thread time to flush to TCP before the process exits.

### PUB/SUB transport (`transport/zeromq.py`)

```
Publisher:  binds PUB socket at *:DSF_PUB_PORT (default 5555)
Subscriber: connects SUB socket to peer's ClusterIP Service endpoint
```

Messages are sent as two-frame multipart: `[topic_bytes, payload_bytes]`. The subscriber subscribes to all topics (`SUBSCRIBE=""`). A 0.5 s sleep after PUB bind gives subscribers time to connect.

### Environment variables injected by controllers

| Variable | Value | Set by |
|---|---|---|
| `DSF_TRANSPORT_PATTERN` | `pushpull` or `pubsub` | Both controllers |
| `DSF_RECV_PORT` | `5555` | odag-controller |
| `DSF_PUB_PORT` | `5555` | cdag-controller |
| `DSF_TASK_NAME` | task name | cdag-controller |
| `DSF_PEER_<TASKNAME_UPPER>` | `zmq://{svc}.{ns}.svc.cluster.local:5555` | Both controllers |

---

## 7. UI server

**Entry point:** `cmd/ui-server/main.go`

### In-memory cache

The server maintains two maps: `odags` and `cdags`, both keyed by `"namespace/name"`. These are populated and kept fresh by a **dynamic watch loop** (one goroutine per resource type). The watch loop reconnects on error with a 5 s backoff.

### REST API

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/odags` | List all ODAG summaries |
| `GET` | `/api/odags/{ns}/{name}` | Full ODAG detail (spec + status.tasks) |
| `GET` | `/api/odags/{ns}/{name}/history` | Run history from SQLite |
| `GET` | `/api/cdags` | List all CDAG summaries |
| `GET` | `/api/cdags/{ns}/{name}` | Full CDAG detail (spec + status.tasks with node) |
| `GET` | `/api/events` | Server-Sent Events stream |

All endpoints are read-only (`GET`). CORS is enabled for `*` to allow the Vite dev server to proxy.

### SQLite history

On every `ADDED`/`MODIFIED` event where an ODAG reaches `Succeeded` or `Failed`, the server calls `recordHistory()` which inserts a row into `odag_runs` using `INSERT OR IGNORE` (idempotent, keyed by `uid + resourceVersion`).

```sql
CREATE TABLE IF NOT EXISTS odag_runs (
    id              TEXT PRIMARY KEY,   -- uid + resourceVersion
    name            TEXT NOT NULL,
    namespace       TEXT NOT NULL,
    phase           TEXT NOT NULL,
    makespan        REAL,
    start_time      TEXT,
    completion_time TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
)
```

The database file is mounted from a hostPath volume (`/var/lib/dsf/dsf-history.db`) so history persists across pod restarts.

### Server-Sent Events

The SSE handler (`/api/events`) registers a buffered `chan []byte` per connected client. The `broadcast()` function sends a JSON event to all registered channels on every resource change:

```json
{"resource": "odags", "eventType": "MODIFIED", "name": "dag-pipeline", "namespace": "default"}
{"resource": "cdags", "eventType": "MODIFIED", "name": "pipeline-ctg", "namespace": "default"}
```

The frontend's `useSSE` hook listens on this stream and invalidates the relevant React Query caches, giving sub-second UI updates without polling.

### `nestedFloat` helper

CRD fields with `type: number` may arrive as `int64` from the K8s API server. `unstructured.NestedFloat64` does a strict `float64` assertion and silently fails on `int64`. The `nestedFloat()` helper handles `float64`, `int64`, and `json.Number` to work correctly in all cases.

---

## 8. React frontend

**Entry point:** `ui/src/`
**Stack:** React 18 + Vite + TypeScript + TanStack Query + React Flow + Recharts + Tailwind CSS

### Pages

| Page | Component | Data source |
|---|---|---|
| ODAG list | `pages/ODAGList.tsx` | `GET /api/odags` |
| ODAG detail | `pages/ODAGDetail.tsx` | `GET /api/odags/{ns}/{name}` + history |
| CDAG list | `pages/CDAGList.tsx` | `GET /api/cdags` |
| CDAG detail | `pages/CDAGDetail.tsx` | `GET /api/cdags/{ns}/{name}` |

### DAGGraph component (`components/DAGGraph.tsx`)

Renders the ODAG task graph using React Flow with a custom `TaskNode` node type.

**Layout:** Tasks are grouped into layers by topological depth (roots at layer 0). Within each layer, nodes are vertically centred. x = `layer × (NODE_W + COL_GAP)`.

**Node features:**
- Background and border colour match task phase (gray=Pending, amber=Running, green=Succeeded, red=Failed).
- Always-visible text: assigned node (`anrg-5`), allowed nodes (`↦ anrg-1, anrg-3`).
- Hover tooltip: Phase, Node, Start, End, Runtime, Image.

**Edges:** Colour matches the upstream task's phase. Edges animate when the upstream task is Running.

### CDAGGraph component (`components/CDAGGraph.tsx`)

Renders the CDAG task graph with the same layout algorithm and React Flow setup.

**Key differences from DAGGraph:**
- Node shows `{readyReplicas}/{desiredReplicas} ready` instead of task phase.
- Phase is derived: `Pending` (0 ready), `Degraded` (partial), `Running` (all ready).
- Purple colour palette for Degraded state.
- Hover tooltip: Status, Replicas, Node, Image, Allowed nodes.

### SSE hook (`hooks/useSSE.ts`)

Connects to `/api/events` via `EventSource`. On each message:
- `resource === "odags"` → invalidates `['odags']`, `['odag', ns, name]`, `['odag-history', ns, name]`
- `resource === "cdags"` → invalidates `['cdags']`, `['cdag', ns, name]`

The `EventSource` API auto-reconnects on connection loss.

### Development proxy

`ui/vite.config.ts` proxies `/api` to `http://localhost:8080` so the Vite dev server and the Go server can run side-by-side during development.

---

## 9. Data flow end-to-end

### One-shot ODAG

```
User: kubectl apply -f odag.yml  (kind: ODAG)
  └─> K8s API Server stores ODAG CR

odag-controller (watch ADDED):
  └─> assignTasks(): task -> node assignments (constraint-aware random)
  └─> Create ClusterIP Services (one per task, label dsf-odag=<name>)
  └─> Create Pods (all simultaneously, NodeAffinity to assigned node)
  └─> Write initial status.tasks[] with node assignments
  └─> status.phase = Running

Pods (all start simultaneously):
  generate: binds PULL at :5555, sends to transform via PUSH
  transform: binds PULL at :5555, blocks until generate pushes, sends to output
  output:   binds PULL at :5555, blocks until transform pushes, writes result

odag-controller (pod watch loop):
  └─> On each pod event: update status.tasks[] (phase, node, startTime, etc.)
  └─> When all Succeeded: compute makespan, status.phase = Succeeded

ui-server (resource watch):
  └─> Receives MODIFIED event, updates in-memory odags cache
  └─> Calls recordHistory() -> INSERT into odag_runs
  └─> Broadcasts SSE: {"resource":"odags","eventType":"MODIFIED",...}

Frontend:
  └─> useSSE invalidates ['odags'] and ['odag', ns, name]
  └─> React Query refetches /api/odags/{ns}/{name}
  └─> DAGGraph re-renders with updated task phases and node labels
```

### Continuous CDAG

```
User: kubectl apply -f examples/pipeline-ctg/cdag.yml  (kind: CDAG)
  └─> K8s API Server stores CDAG CR

cdag-controller (watch ADDED):
  └─> assignTasks(): task -> node assignments (constraint-aware random)
  └─> Create ClusterIP Services (label dsf-cdag=<name>)
  └─> Create Pods with restartPolicy: Always, DSF_TRANSPORT_PATTERN=pubsub
  └─> Write initial status.tasks[] with planned node assignments

Pods (run forever):
  producer:  publishes messages to PUB socket on :5555
  processor: subscribes to producer, processes, republishes
  sink:      subscribes to processor, writes output

cdag-controller (reconcile loop, every 30s):
  └─> List pods by dsf-cdag=<name>,dsf-task=<task> for each task
  └─> Count alive pods, read pod.Spec.NodeName
  └─> Recreate any missing/failed pods (pickNode respects constraints)
  └─> updateCDAGTaskStatuses(): write status.tasks[] with node info
  └─> status.phase = Running | Degraded

ui-server + frontend: same SSE-driven live update path as ODAG
  └─> CDAGGraph re-renders with updated replica counts and node labels
```

---

## 10. Key design decisions and trade-offs

### All pods start simultaneously (ODAGs)

**Decision:** Create all pods at once; downstream tasks block on `recv()`.

**Why:** ZMQ PUSH/PULL does not require a broker. If all pods are up and the receiver has bound its PULL socket before the sender connects, the message is reliably delivered. Layer-by-layer creation would complicate the controller and add latency.

**Trade-off:** Every task image must be pulled to its assigned node before any work starts. `imagePullPolicy: Always` adds a registry round-trip on every run.

### Separate scheduling functions per controller

**Decision:** `assignTasks()` in `odag-controller/main.go` and `assignTasks()` in `cdag-controller/main.go` are independent functions, not shared.

**Why:** ODAG and CDAG have different execution models and different information available at scheduling time (ODAGs have `dataSize`/`runtime` hints; CDAGs have `replicas`). Keeping them separate allows them to evolve independently — the ODAG scheduler can incorporate HEFT without affecting the CDAG scheduler.

**Trade-off:** Some duplication in the initial random-placement logic. Acceptable at this stage.

### Constraint-aware random placement (not HEFT)

**Decision:** Both controllers use constraint-aware random node selection rather than HEFT.

**Why:** HEFT requires accurate `runtime` and `dataSize` hints per task. Without reliable measurements these defaults produce no better results than random assignment, while adding complexity. Random placement within constraints is predictable and correct.

**Trade-off:** No makespan optimisation. HEFT will be re-introduced once task profiling is available.

### ClusterIP Service per task (stable DNS for ZMQ)

**Decision:** One Service per task with a predictable DNS name.

**Why:** Pod IP addresses are ephemeral. A Service gives a stable DNS name that can be injected as an env var at pod creation time. The controller knows all task names before any pod is scheduled, so all `DSF_PEER_*` env vars can be injected upfront.

**Trade-off:** Services are not garbage-collected automatically; owner references on the Service objects handle deletion when the ODAG/CDAG CR is deleted.

### `imagePullPolicy: Always` for task pods

**Decision:** Task pods always pull from the registry, even if the image is cached.

**Why:** Images are tagged `:latest`. Using `IfNotPresent` caused worker nodes to serve stale cached images after a rebuild.

**Trade-off:** Extra registry round-trip per pod startup. For production use, version-tagged images with `IfNotPresent` would be preferred.

### ZMQ timing (LINGER + sleeps)

**Decision:** `LINGER=5000`, 0.2 s sleep after connect, 0.3 s sleep after send.

**Why:** ZMQ's I/O is asynchronous. `socket.send()` enqueues the message; the I/O thread flushes to TCP in the background. If the Python process exits before the flush completes, the message is silently dropped.

**Trade-off:** Adds ~0.5 s latency per send for one-shot tasks. For CDAGs using PUB/SUB, publishers stay alive so this is not an issue.

### SQLite for ODAG history

**Decision:** A single SQLite file mounted at `/var/lib/dsf/dsf-history.db`.

**Why:** History is append-only, the write rate is low (one row per ODAG completion), and there is only one writer (the ui-server). SQLite is zero-dependency and sufficient.

**Trade-off:** Not suitable for multi-replica ui-server deployments. For HA, replace with an external database.

### SSE over WebSocket for live updates

**Decision:** Server-Sent Events (`EventSource`) instead of WebSocket.

**Why:** SSE is unidirectional (server→client), simpler to implement in a plain `net/http` handler, and automatically reconnects via the browser's `EventSource` API.

**Trade-off:** SSE requires HTTP/1.1 keep-alive; connections through some reverse proxies may time out without a heartbeat.
