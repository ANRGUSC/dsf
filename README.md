# DSF — Distributed Scheduling Framework

DSF runs containerised task graphs on a k3s cluster.
Two execution models are supported:

| Model | Kind | Use case |
|---|---|---|
| **One-shot DAG** | `ODAG` | Finite pipelines — tasks run once, in dependency order, and the DAG succeeds or fails |
| **Continuous DAG** | `CDAG` | Streaming pipelines — tasks run forever, restarted automatically on failure |

Tasks are ordinary container images. They communicate via **ZeroMQ** (PUSH/PULL for one-shot, PUB/SUB for continuous) using the DSF Python SDK. No shared filesystem or message broker is required.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Repository layout](#repository-layout)
3. [Prerequisites](#prerequisites)
4. [Quick start](#quick-start)
5. [Writing tasks](#writing-tasks)
6. [ODAG reference](#odag-reference)
7. [CDAG reference](#cdag-reference)
8. [CLI reference](#cli-reference)
9. [UI](#ui)
10. [Build & deploy reference](#build--deploy-reference)
11. [Cluster setup](#cluster-setup)
12. [Troubleshooting](#troubleshooting)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  User                                                               │
│   dsf odag submit -f odag.yml     dsf cdag submit -f cdag.yml      │
└────────────────────┬────────────────────────────┬───────────────────┘
                     │                            │
             ODAG CR │                    CDAG CR │
                     ▼                            ▼
┌────────────────────────────────────────────────────────────────────┐
│  dsf-system namespace (runs on master node)                        │
│                                                                    │
│  ┌─────────────────────┐    ┌─────────────────────┐               │
│  │  odag-controller    │    │  cdag-controller    │                │
│  │                     │    │                     │                │
│  │  Constraint-aware   │    │  Constraint-aware   │                │
│  │  random placement   │    │  random placement   │                │
│  │  Creates pods+svcs  │    │  Creates pods+svcs  │                │
│  │  Watches pods       │    │  Reconcile loop     │                │
│  │  Updates status     │    │  Recreates failed   │                │
│  └──────────┬──────────┘    └──────────┬──────────┘               │
│             │                          │                           │
│  ┌──────────▼──────────────────────────▼──────────┐               │
│  │              ui-server  :8080                  │               │
│  │  K8s watch cache  │  SQLite history  │  SSE    │               │
│  │  REST API /api/*  │  React frontend            │               │
│  └────────────────────────────────────────────────┘               │
└────────────────────────────────────────────────────────────────────┘
                     │                            │
          Pods on worker nodes          ClusterIP Services (ZMQ DNS)
                     │                            │
┌────────────────────▼────────────────────────────▼───────────────────┐
│  default namespace (task pods)                                       │
│                                                                      │
│  ┌─────────┐  DSF_PEER_TRANSFORM=zmq://…  ┌───────────┐             │
│  │generate │ ────────────────────────────► │ transform │             │
│  │  PUSH   │                               │   PULL    │             │
│  └─────────┘                               └─────┬─────┘             │
│                                                  │ PUSH              │
│                                            ┌─────▼─────┐             │
│                                            │  output   │             │
│                                            │   PULL    │             │
│                                            └───────────┘             │
└──────────────────────────────────────────────────────────────────────┘
```

### Key design decisions

- **All pods start simultaneously (ODAGs).** Downstream tasks block on `recv()` until upstream pushes. This makes PUSH/PULL work without a broker.
- **One ClusterIP Service per task.** DNS name `{name}-{task}.{namespace}.svc.cluster.local:5555` is stable and injected into every pod as `DSF_PEER_<TASKNAME>`.
- **Constraint-aware random placement.** Each task specifies a list of allowed nodes via `constraints.nodeNames`. The controller picks a random node from the intersection of the constraint list and the schedulable cluster nodes. The ODAG and CDAG schedulers are independent — their placement logic will diverge as smarter algorithms are added.
- **CDAG reconcile loop** runs every 30 s; recreates any missing or failed pods automatically.

---

## Repository layout

```
dsf/
├── api/
│   └── v1/
│       ├── odag-crd.yml          # ODAG CRD (dsf.io/v1, kind: ODAG)
│       └── cdag-crd.yml          # CDAG CRD (dsf.io/v1, kind: CDAG)
│
├── cmd/
│   ├── odag-controller/
│   │   ├── main.go               # One-shot DAG controller
│   │   └── Dockerfile
│   ├── cdag-controller/
│   │   ├── main.go               # Continuous DAG controller
│   │   └── Dockerfile
│   ├── ui-server/
│   │   ├── main.go               # HTTP server + K8s watch + SQLite
│   │   └── Dockerfile            # Multi-stage: embeds ui/dist
│   └── cli/
│       └── main.go               # dsf CLI (cobra)
│
├── pkg/
│   └── scheduler/
│       └── heft.go               # HEFT algorithm (reference, not used by controllers)
│
├── sdk/
│   └── python/
│       └── dsf_sdk/
│           ├── api.py            # DSFTask — user-facing send/recv
│           ├── transport/
│           │   ├── zeromq.py     # ZmqPushPullTransport, ZmqPubSubTransport
│           │   └── router.py     # Selects transport from env vars
│           └── schedulers/
│               └── heft.py       # Python HEFT (for future custom scheduler subprocesses)
│
├── ui/                           # React + Vite frontend
│   └── src/
│       ├── pages/                # ODAGList, ODAGDetail, CDAGList, CDAGDetail
│       ├── components/           # DAGGraph, CDAGGraph (React Flow), StatusBadge
│       ├── hooks/                # useSSE — SSE-driven query invalidation
│       └── api/client.ts         # Typed fetch wrappers
│
├── deployments/
│   ├── namespace.yml
│   ├── odag-controller/          # RBAC + Deployment
│   ├── cdag-controller/          # RBAC + Deployment
│   └── ui-server/                # RBAC + Deployment + NodePort Service
│
├── examples/
│   ├── dag-pipeline/             # One-shot ODAG: generate → transform → output
│   └── pipeline-ctg/             # Continuous CDAG: producer → processor → sink
│
├── docs/
│   ├── PLAN.md                   # Architecture brainstorm (living doc)
│   └── architecture.md           # Detailed architecture reference
│
├── archive/                      # phase-1, phase-2 (reference only)
├── Makefile                      # All build / push / deploy commands
└── go.mod
```

---

## Prerequisites

| Tool | Purpose |
|---|---|
| `kubectl` | Cluster access |
| `docker` | Building images |
| `go` ≥ 1.23 | Building Go binaries |
| `node` ≥ 20, `npm` | Building the React UI |
| `sshpass` | Pushing registry config to worker nodes (setup only) |

The cluster must be running **k3s**. All commands assume `~/.kube/config` is configured.

---

## Quick start

### 1. Install CRDs and deploy the control plane

```bash
# Install CRDs and namespace
make install

# Build all images and push to the local registry
make push-all

# Deploy controllers and UI to the cluster
make deploy
```

The UI is now available at `http://<master-ip>:30080`.

### 2. Run the one-shot example

```bash
make example-odag
```

Watch it run:

```bash
dsf odag list
dsf odag status dag-pipeline
dsf odag logs dag-pipeline generate
```

### 3. Run the continuous example

```bash
make example-cdag
```

Watch it run:

```bash
dsf cdag list
dsf cdag status pipeline-ctg
dsf cdag logs pipeline-ctg sink
```

### 4. Clean up

```bash
dsf odag delete dag-pipeline
dsf cdag delete pipeline-ctg
```

---

## Writing tasks

Tasks are Python scripts that use the `dsf_sdk` package. The SDK is copied into the image at build time — no pip install from PyPI needed.

```python
from dsf_sdk import DSFTask

task = DSFTask()   # reads DSF_* env vars injected by the controller

# Receive from an upstream task (blocks until data arrives)
data = task.recv("upstream-task-name")

# Do work
result = process(data)

# Send to a downstream task
task.send("downstream-task-name", result)
task.close()
```

`send` / `recv` accept any JSON-serialisable value. The transport (ZMQ PUSH/PULL or PUB/SUB) is selected automatically by `DSF_TRANSPORT_PATTERN`.

### Dockerfile template

```dockerfile
# Build from the repo root:
#   docker build -f examples/my-dag/tasks/my-task/Dockerfile -t <registry>/my-task:latest .
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir pyzmq
COPY sdk/python/dsf_sdk ./dsf_sdk
COPY examples/my-dag/tasks/my-task/task.py .
CMD ["python", "task.py"]
```

---

## ODAG reference

```yaml
apiVersion: dsf.io/v1
kind: ODAG
metadata:
  name: my-dag
  namespace: default
spec:
  scheduler: heft          # field accepted but placement uses constraint-aware random
  retryPolicy:
    maxRetries: 2
  tasks:
    - name: generate
      image: 192.168.1.163:5000/my-generate:latest
      command: ["python", "task.py"]
      dependencies: []
      resources:
        cpu: "200m"
        memory: "128Mi"
      dataSize: "1MB"      # kept for future HEFT integration
      runtime: 10          # kept for future HEFT integration
      constraints:
        nodeNames:          # task will only be placed on one of these nodes
          - anrg-1
          - anrg-3
          - anrg-5

    - name: transform
      image: 192.168.1.163:5000/my-transform:latest
      command: ["python", "task.py"]
      dependencies: ["generate"]
      resources:
        cpu: "500m"
        memory: "256Mi"
      constraints:
        nodeNames:
          - anrg-4
          - anrg-6
          - anrg-8
```

### Status fields

| Field | Description |
|---|---|
| `status.phase` | `Pending` → `Scheduling` → `Running` → `Succeeded` / `Failed` |
| `status.makespan` | Actual wall-clock makespan in seconds (set on completion) |
| `status.completionTime` | RFC 3339 timestamp |
| `status.tasks[].phase` | Per-task phase |
| `status.tasks[].node` | Node the task ran on |
| `status.tasks[].startTime` | Pod start time |
| `status.tasks[].completionTime` | Container finish time |

---

## CDAG reference

```yaml
apiVersion: dsf.io/v1
kind: CDAG
metadata:
  name: my-cdag
  namespace: default
spec:
  restartPolicy: Always    # Always | OnFailure | Never
  tasks:
    - name: producer
      image: 192.168.1.163:5000/my-producer:latest
      command: ["python", "task.py"]
      replicas: 1
      dependencies: []
      resources:
        cpu: "200m"
        memory: "128Mi"
      constraints:
        nodeNames:
          - anrg-1
          - anrg-3

    - name: consumer
      image: 192.168.1.163:5000/my-consumer:latest
      command: ["python", "task.py"]
      replicas: 1
      dependencies: ["producer"]
      resources:
        cpu: "300m"
        memory: "128Mi"
      constraints:
        nodeNames:
          - anrg-4
          - anrg-6
```

CDAG tasks use ZMQ **PUB/SUB** (publisher binds, subscriber connects).
The reconcile loop recreates any missing pods every 30 seconds.

### Status fields

| Field | Description |
|---|---|
| `status.phase` | `Pending` → `Running` / `Degraded` / `Failed` |
| `status.tasks[].name` | Task name |
| `status.tasks[].node` | Node the task's pod is running on |
| `status.tasks[].desiredReplicas` | Desired replica count |
| `status.tasks[].readyReplicas` | Currently running replicas |
| `status.tasks[].podNames` | Names of live pods |

---

## CLI reference

```
dsf odag submit  -f <file>                  Submit an ODAG from a YAML file
dsf odag list    [-n <namespace>]           List all ODAGs
dsf odag status  <name> [-n <namespace>]    Show detailed status
dsf odag delete  <name> [-n <namespace>]    Delete ODAG + pods + services
dsf odag logs    <name> <task>              Stream logs from a task pod

dsf cdag submit  -f <file>                  Submit a CDAG from a YAML file
dsf cdag list    [-n <namespace>]           List all CDAGs
dsf cdag status  <name> [-n <namespace>]    Show detailed status
dsf cdag delete  <name> [-n <namespace>]    Delete CDAG + pods + services
dsf cdag logs    <name> <task>              Stream logs from a task pod

Global flags:
  --kubeconfig <path>    Path to kubeconfig (default: $KUBECONFIG or ~/.kube/config)
```

---

## UI

The web UI is served by the `ui-server` on NodePort **30080**.

| Page | URL | Description |
|---|---|---|
| ODAG list | `/` | All ODAGs with phase, makespan, age |
| ODAG detail | `/odags/{ns}/{name}` | Graph view, tasks table, run history chart |
| CDAG list | `/cdags` | All CDAGs with phase, age |
| CDAG detail | `/cdags/{ns}/{name}` | Graph view, live replica counts, node assignment |

Both the ODAG and CDAG detail pages show the **task graph** with nodes coloured by phase (gray=Pending, amber=Running, green=Succeeded/Ready, red=Failed). Hover over any node to see timing details (ODAG) or replica/node details (CDAG). Allowed nodes appear directly on the node card as `↦ anrg-1, anrg-3`.

Updates arrive via **Server-Sent Events** (`/api/events`) — no polling delay.

---

## Build & deploy reference

See the `Makefile` for the canonical commands. Key targets:

```bash
make build                  # Build all Go binaries into bin/
make ui-build               # Build the React UI into ui/dist/
make test                   # Run Go unit tests

make image-odag-controller  # Build odag-controller Docker image
make image-cdag-controller  # Build cdag-controller Docker image
make image-ui-server        # Build ui-server Docker image (includes UI)
make image-examples         # Build all example task images
make push-all               # Build and push everything to the local registry
make push-controllers       # Build and push only control-plane images

make install                # Apply CRDs + namespace + RBAC to cluster
make deploy                 # Apply Deployments + Service to cluster
make rollout                # Force-restart all control-plane deployments

make example-odag           # Submit the dag-pipeline ODAG example
make example-cdag           # Submit the pipeline-ctg CDAG example
make clean-examples         # Delete both examples from the cluster
make clean-deploy           # Delete all DSF control-plane resources
make clean-all              # Delete everything including CRDs and namespace
```

---

## Cluster setup

This section covers the one-time setup required for a fresh k3s cluster.

### 1. Start a local Docker registry on the master node

```bash
docker run -d -p 5000:5000 --restart=always --name registry registry:2
```

### 2. Configure Docker on the master to trust the registry

```bash
echo '{"insecure-registries": ["<master-ip>:5000"]}' | sudo tee /etc/docker/daemon.json
sudo systemctl restart docker
```

### 3. Configure k3s on every node to mirror the registry

Write the following to `/etc/rancher/k3s/registries.yaml` on **all nodes** (master and workers), then restart the k3s service:

```yaml
mirrors:
  "<master-ip>:5000":
    endpoint:
      - "http://<master-ip>:5000"
```

```bash
# On master
sudo systemctl restart k3s

# On each worker (example using sshpass)
for node in anrg-1 anrg-3 anrg-4 anrg-5 anrg-6 anrg-7 anrg-8 anrg-9; do
  sshpass -p <password> ssh anrg@$node \
    "echo '<yaml>' | sudo tee /etc/rancher/k3s/registries.yaml && \
     echo '<password>' | sudo -S systemctl restart k3s-agent"
done
```

### 4. Apply CRDs, RBAC, and deploy

```bash
make install
make push-all
make deploy
```

---

## Troubleshooting

### Controller pod is Pending

```bash
kubectl describe pod -n dsf-system -l app=odag-controller
kubectl describe pod -n dsf-system -l app=cdag-controller
```

Common cause: the master node has `SchedulingDisabled`. The deployments use a toleration for `node.kubernetes.io/unschedulable:NoSchedule` and a `nodeSelector` for the master hostname. Check that both match.

### Task pod image pull fails

```bash
kubectl describe pod <pod-name> -n default
```

- Verify the registry container is running: `docker ps | grep registry`
- Verify `/etc/rancher/k3s/registries.yaml` exists on the worker node and k3s-agent was restarted after it was written.

### ZMQ message not received (one-shot ODAG hangs)

The SDK sets `LINGER=5000ms` and sleeps 0.3 s after each send to flush the socket. If a downstream task never receives a message:

1. Check that both pods started: `kubectl get pods -l dsf-odag=<name>`
2. Check sender logs: `dsf odag logs <name> <sender-task>`
3. Verify the ClusterIP service exists: `kubectl get svc -l dsf-odag=<name>`

### CDAG task not restarting after failure

The cdag-controller reconciles every 30 s. Check its logs:

```bash
kubectl logs -n dsf-system deployment/cdag-controller --tail=40
```

### Constraint nodes not being respected

If a pod lands on a node not in `constraints.nodeNames`, delete the pod and the CDAG/ODAG CR cleanly:

```bash
kubectl delete cdag <name> -n default
kubectl get pods -n default          # wait until all pods are gone
kubectl apply -f <file>
```

Stale pods from a previous run (with different labels) can block fresh pod creation since controllers skip pod creation if a pod with the same name already exists.

### UI shows stale data

The SSE connection may have dropped. Refresh the page — the browser reconnects automatically via `EventSource` auto-retry.
