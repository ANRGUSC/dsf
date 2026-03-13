# DSF Framework: Plan (Living Document)

**Status:** Not final — for cleaning and restructuring. This document captures the brainstorm and target shape of DSF as a user-facing framework.

---

## Target product (vision)

1. **User defines a DAG** (tasks + dependencies; ODAG for one-shot, CDAG for continuous).
2. **User implements each task** (e.g. container images, one per task or per role).
3. **User gives the DAG to “the program”** (submit).
4. **The system runs it on the cluster** and provides:
   - High throughput (scheduling, placement)
   - Resilience (retries, recovery)
   - Execution guarantees (keep the graph running or eventually complete)
   - Optional: observability (status, logs, metrics)

So the framework owns: **deployment, lifecycle, retries, and status**; the user owns: **DAG structure and task code (images)**.

---

## 1. Retry vs report (decided: hybrid)

**Decision: hybrid.** System retries (recreate missing pods, optional retry limit) and always reports (status, events, CLI).

- **Retry at the system level (controller):** Treat desired state as N pods per task; reconcile and recreate missing pods on eviction/failure; optional retry policy and Degraded/Failed status.
- **Report always:** CR status (phase, per-task counts), Kubernetes events, and CLI (e.g. `dsf odag status <name>`).

---

## 2. Where DSF sits

- **Above Kubernetes:** Controller + CRDs + CLI. Uses K8s API for Pods/Services; does not replace kubelet or scheduler.
- **Responsibilities:** Controller reconciles CR → pods/services, retry, status, events; CRD = desired state; CLI = submit, status, list, delete, logs.

---

## 3. k3s integration (Option C — decided)

**Decision:** Separate **dsf** CLI (Option C). Commands: `dsf odag submit -f <file>`, `dsf odag status <name>`, `dsf odag list`, `dsf odag delete <name>`, `dsf odag logs <name> <task>` — and the same set under `dsf cdag`. Uses KUBECONFIG; no k3s fork.

---

## 4. What to add for “framework” behavior

1. **Reconciliation loop** in the controller (desired vs actual pods; recreate missing).
2. **CR status** (phase, tasks[], desired/ready replicas, last error).
3. **Events** on CR or namespace (fail, recreate, retry limit).
4. **DSF CLI** (submit, status, list, delete, logs).
5. **Optional:** Higher-level DAG spec + translator to CDAG YAML.

---

## 5. Task communication API (transport abstraction)

**Goal:** User code uses a simple API (e.g. `dsf.send("task", payload)`, `dsf.recv("task")`); framework chooses transport (ZMQ, shm, socket, resilient) based on placement.

- **DSF SDK** in task images; controller injects peer config via env (e.g. `DSF_PEER_<name>=<transport>://<endpoint>`).
- **Transport:** Same node → shm or Unix socket; remote stable → ZMQ; remote unstable → resilient (e.g. HTTP + retries).
- **Contract:** Controller sets env; SDK reads it. Compatible with existing ZMQ env; add shm/socket later for co-location.

---

## 6. One-shot vs continuous DAGs

**Both supported.** ODAG (run once; one CR + odag-controller); CDAG (long-lived; one CR + cdag-controller, reconcile, optional replication). Same CLI and task SDK for both.

---

## 7. Final project structure (Option C)

Actual layout (as implemented):

```
dsf/
├── README.md
├── api/
│   ├── v1/
│   │   ├── odag-crd.yml              # ODAG CRD (dsf.io/v1, kind: ODAG)
│   │   └── cdag-crd.yml              # CDAG CRD (dsf.io/v1, kind: CDAG)
│   └── scheduler/
│       └── schema.json               # Scheduler I/O contract
├── cmd/
│   ├── odag-controller/              # One-shot DAG controller
│   │   ├── main.go
│   │   └── Dockerfile
│   ├── cdag-controller/              # Continuous DAG controller
│   │   ├── main.go
│   │   └── Dockerfile
│   ├── ui-server/                    # HTTP API + K8s watch + SQLite + SSE
│   │   ├── main.go
│   │   └── Dockerfile
│   └── cli/                          # dsf CLI (cobra)
│       └── main.go
├── pkg/
│   └── scheduler/
│       └── heft.go                   # HEFT algorithm (Go reference impl)
├── deployments/
│   ├── namespace.yml
│   ├── odag-controller/              # RBAC + Deployment
│   ├── cdag-controller/              # RBAC + Deployment
│   └── ui-server/                    # RBAC + Deployment + NodePort Service
├── sdk/python/
│   └── dsf_sdk/
│       ├── api.py                    # DSFTask — user-facing send/recv
│       ├── schedulers/               # HEFT (Python, for custom schedulers)
│       └── transport/                # zeromq push/pull and pub/sub, router
├── ui/                               # React + Vite frontend
│   └── src/
│       ├── pages/                    # ODAGList, ODAGDetail, CDAGList, CDAGDetail
│       ├── components/               # DAGGraph, CDAGGraph, StatusBadge
│       ├── hooks/                    # useSSE
│       └── api/client.ts             # Typed fetch wrappers
├── examples/
│   ├── dag-pipeline/                 # One-shot ODAG: generate → transform → output
│   └── pipeline-ctg/                 # Continuous CDAG: producer → processor → sink
├── docs/
│   ├── PLAN.md                       # This file
│   └── architecture.md               # Detailed architecture reference
└── archive/                          # phase-1, phase-2 (reference only)
```

| Piece               | Location                        | Role                                    |
|---------------------|---------------------------------|-----------------------------------------|
| ODAG CRD            | api/v1/odag-crd.yml             | Cluster API — one-shot DAGs             |
| CDAG CRD            | api/v1/cdag-crd.yml             | Cluster API — continuous DAGs           |
| odag-controller     | cmd/odag-controller/            | One-shot: schedule, pods, status        |
| cdag-controller     | cmd/cdag-controller/            | Continuous: reconcile, node tracking    |
| ui-server           | cmd/ui-server/                  | REST API, SSE, SQLite history           |
| dsf CLI             | cmd/cli/                        | odag/cdag submit, list, status, delete, logs |
| Task SDK            | sdk/python/dsf_sdk/             | send/recv in task images                |
| Built-in schedulers | sdk/python/dsf_sdk/schedulers/  | HEFT (Python, for custom schedulers)    |
| Examples            | examples/                       | dag-pipeline (ODAG), pipeline-ctg (CDAG)|

---

## 8. Open scheduling (user-defined scheduler logic)

**Goal:** User (or framework) plugs in custom scheduling (HEFT for one-shot; constraint-aware placement for continuous) without changing controller code.

- **Contract:** Input = DAG spec + cluster state (JSON); output = task → node assignments. Schema in `api/scheduler/schema.json`.
- **Python:** User implements `schedule(dag, cluster_state) -> schedule`; controller invokes via subprocess.
- **Built-ins:** HEFT in `sdk/python/dsf_sdk/schedulers/` and `pkg/scheduler/heft.go`; controllers currently use constraint-aware random placement with HEFT as a reference.

---

## 9. Summary

| Topic                 | Decision / direction                                                                 |
|-----------------------|--------------------------------------------------------------------------------------|
| Retry or report       | **Hybrid:** retry + report (status, events, CLI).                                    |
| Where it sits         | Above k3s: controller + CRDs + CLI.                                                 |
| k3s commands          | **Option C:** separate dsf CLI (`dsf odag` / `dsf cdag`).                           |
| Task communication    | DSF SDK: send/recv; controller injects peer endpoints via env vars; ZMQ transport.   |
| One-shot vs continuous| Both; ODAG (run-to-completion) and CDAG (perpetual); separate CRs and controllers.  |
| Open scheduling       | Scheduler interface (Python); HEFT built-in; controllers invoke via subprocess.      |
| Project structure     | Final layout above (api, cmd, pkg, deployments, sdk/python, ui, examples).           |
