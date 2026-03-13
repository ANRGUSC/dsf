# DSF Framework: Plan (Living Document)

**Status:** Not final — for cleaning and restructuring. This document captures the brainstorm and target shape of DSF as a user-facing framework.

---

## Target product (vision)

1. **User defines a DAG** (tasks + dependencies; possibly a higher-level spec than raw CTG YAML).
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
- **Report always:** CR status (phase, per-task counts), Kubernetes events, and CLI (e.g. `dsf dag status <name>`).

---

## 2. Where DSF sits

- **Above Kubernetes:** Controller + CRDs + CLI. Uses K8s API for Pods/Services; does not replace kubelet or scheduler.
- **Responsibilities:** Controller reconciles CR → pods/services, retry, status, events; CRD = desired state; CLI = submit, status, list, delete, logs.

---

## 3. k3s integration (Option C — decided)

**Decision:** Separate **dsf** CLI (Option C). Commands: `dsf dag submit -f <file>`, `dsf dag status [name]`, `dsf dag list`, `dsf dag delete <name>`, optionally `dsf dag logs`. Uses KUBECONFIG; no k3s fork. Option B (k3s subcommands) only if one-binary UX is needed later.

---

## 4. What to add for “framework” behavior

1. **Reconciliation loop** in the controller (desired vs actual pods; recreate missing).
2. **CR status** (phase, tasks[], desired/ready replicas, last error).
3. **Events** on CR or namespace (fail, recreate, retry limit).
4. **DSF CLI** (submit, status, list, delete, logs).
5. **Optional:** Higher-level DAG spec + translator to CTG YAML.

---

## 5. Task communication API (transport abstraction)

**Goal:** User code uses a simple API (e.g. `dsf.send("task", payload)`, `dsf.recv("task")`); framework chooses transport (ZMQ, shm, socket, resilient) based on placement.

- **DSF SDK** in task images; controller injects peer config via env (e.g. `DSF_PEER_<name>=<transport>://<endpoint>`).
- **Transport:** Same node → shm or Unix socket; remote stable → ZMQ; remote unstable → resilient (e.g. HTTP + retries).
- **Contract:** Controller sets env; SDK reads it. Compatible with existing ZMQ env; add shm/socket later for co-location.

---

## 6. One-shot vs continuous DAGs

**Both supported.** One-shot DAG (run once; one CR + controller); continuous DAG/CTG (long-lived; one CR + controller, reconcile, optional replication). Same CLI and task SDK for both.

---

## 7. Final project structure (Option C)

Target layout (no phase-1/phase-2 naming in final structure):

```
dsf/
├── README.md
├── api/v1/
│   ├── dag-crd.yml
│   ├── continuous-task-graph-crd.yml
│   ├── samples/
│   │   ├── dag-sample.yml
│   │   └── ctg-sample.yml
│   └── README.md
├── api/scheduler/
│   └── schema.json
├── cmd/
│   ├── dag-controller/               # One-shot: schedule steps, create pods
│   ├── ctg-controller/               # Continuous: pods, reconcile, status
│   ├── scheduler-runner/             # Optional: runs user/built-in Python scheduler
│   └── cli/                          # dsf CLI
├── pkg/
│   ├── scheduler/
│   ├── migration/
│   ├── mqtt/
│   └── metrics/
├── deployments/
│   ├── dag-controller/
│   ├── ctg-controller/
│   └── ...
├── sdk/python/
│   ├── dsf_sdk/
│   │   ├── api.py
│   │   ├── scheduler.py             # schedule(dag, cluster_state) -> schedule
│   │   ├── schedulers/              # HEFT, CPOP, MAXMIN, etc.
│   │   └── transport/               # zeromq, shm, router
│   ├── pyproject.toml
│   └── README.md
└── examples/
    ├── dag-diamond/
    ├── gpt2-ctg/
    └── gpt2-layer-ctg/
```

| Piece               | Location                    | Role                          |
|---------------------|----------------------------|-------------------------------|
| One-shot DAG CRD    | api/v1/dag-crd.yml         | Cluster API batch DAGs        |
| CTG CRD             | api/v1/continuous-task-graph-crd.yml | Cluster API continuous |
| dag-controller      | cmd/dag-controller/        | One-shot: schedule, pods      |
| ctg-controller      | cmd/ctg-controller/        | Continuous: reconcile, status |
| dsf CLI             | cmd/cli/                   | submit, status, list, delete, logs |
| Task SDK            | sdk/python/                | send/recv in task images      |
| Scheduler interface | api/scheduler/, dsf_sdk/scheduler.py | User-defined schedulers |
| Built-in schedulers | sdk/python/dsf_sdk/schedulers/ | HEFT, CPOP, MAXMIN        |
| Examples            | examples/                  | dag-diamond, gpt2-ctg, etc.   |

---

## 8. Open scheduling (user-defined scheduler logic)

**Goal:** User (or framework) plugs in custom scheduling (HEFT, CPOP, MAXMIN for one-shot; placement/throughput policies for CTG) without changing controller code.

- **Contract:** Input = DAG spec + cluster state (JSON); output = schedule (task → node for one-shot; placement/replicas for CTG).
- **Python:** User implements `schedule(dag, cluster_state) -> schedule`; controller or scheduler-runner invokes via subprocess or HTTP.
- **Built-ins:** HEFT, CPOP, MAXMIN, etc. in `sdk/python/dsf_sdk/schedulers/`; select by name in DAG/CTG spec or reference user script (ConfigMap/volume).

---

## 9. Summary

| Topic                 | Decision / direction                                                                 |
|-----------------------|--------------------------------------------------------------------------------------|
| Retry or report       | **Hybrid:** retry + report (status, events, CLI).                                    |
| Where it sits         | Above k3s: controller + CRDs + CLI.                                                 |
| k3s commands          | **Option C:** separate dsf CLI.                                                      |
| Task communication    | DSF SDK: send/recv; controller injects peer config; transport from config.           |
| One-shot vs continuous| Both; separate CRs and controllers; same CLI and SDK.                               |
| Open scheduling       | Scheduler interface (Python); built-ins in SDK; controllers/scheduler-runner invoke. |
| Project structure     | Final layout above (api, cmd, pkg, deployments, sdk/python, examples).               |
