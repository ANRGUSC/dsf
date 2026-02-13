# DSF - Distributed Scheduling Framework

DSF is a platform that streamlines the scheduling process for distributed processing networks on Kubernetes. It provides intelligent DAG (Directed Acyclic Graph) workflow scheduling with support for node constraints and data-aware task placement.

## Overview

The framework consists of several components:

- **DAG CRD**: Custom Resource Definition for defining workflow DAGs
- **HEFT Controller**: Implements the Heterogeneous Earliest Finish Time algorithm for optimized scheduling
- **Random Controller**: Simple random node assignment for baseline comparison
- **DAG Scheduler**: Kubernetes scheduler plugin that enforces node affinity constraints
- **Data Agent**: DaemonSet that handles data transfer between tasks across nodes

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│  ┌─────────────────┐    ┌─────────────────┐                 │
│  │ HEFT Controller │    │  DAG Scheduler  │                 │
│  │  (computes      │    │  (enforces node │                 │
│  │   schedule)     │    │   constraints)  │                 │
│  └────────┬────────┘    └────────┬────────┘                 │
│           │                      │                           │
│           ▼                      ▼                           │
│  ┌─────────────────────────────────────────┐                │
│  │              DAG Resources               │                │
│  │  (Custom Resources defining workflows)   │                │
│  └─────────────────────────────────────────┘                │
│           │                                                  │
│           ▼                                                  │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐        │
│  │  Node 1 │  │  Node 2 │  │  Node 3 │  │  Node N │        │
│  │┌───────┐│  │┌───────┐│  │┌───────┐│  │┌───────┐│        │
│  ││ Data  ││  ││ Data  ││  ││ Data  ││  ││ Data  ││        │
│  ││ Agent ││  ││ Agent ││  ││ Agent ││  ││ Agent ││        │
│  │└───────┘│  │└───────┘│  │└───────┘│  │└───────┘│        │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘        │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Kubernetes cluster (k3s, k8s, etc.)
- `kubectl` configured to access the cluster
- Docker (for building custom images)

### Installation

1. **Apply the DAG CRD:**
   ```bash
   kubectl apply -f phase-1/crd/dag-crd.yml
   ```

2. **Deploy the Data Agent DaemonSet:**
   ```bash
   kubectl apply -f phase-1/data-agent/daemonset.yml
   ```

3. **Deploy the DAG Scheduler:**
   ```bash
   kubectl apply -f phase-1/scheduler/rbac.yml
   kubectl apply -f phase-1/scheduler/configmap.yml
   kubectl apply -f phase-1/scheduler/deployment.yml
   ```

4. **Deploy the HEFT Controller:**
   ```bash
   kubectl apply -f phase-1/controller/heft-controller/rbac.yml
   kubectl apply -f phase-1/controller/heft-controller/deployment.yml
   ```

## Creating DAGs

DAGs are defined as Kubernetes custom resources. Each DAG consists of steps (tasks) with dependencies, resource requirements, and optional node constraints.

### Basic DAG Structure

```yaml
apiVersion: workflow.example.com/v1
kind: DAG
metadata:
  name: my-workflow
spec:
  steps:
    - name: step1
      image: busybox
      command: ["sh", "-c"]
      args: ["echo 'Processing...'; sleep 10"]
      dependencies: []          # No dependencies (entry point)
      dataSize: "100MB"         # Output data size for scheduling
      runtime: 10               # Expected runtime in seconds
      cpu: "500m"               # CPU request
      memory: "256Mi"           # Memory request
      
    - name: step2
      image: busybox
      command: ["sh", "-c"]
      args: ["echo 'Step 2'; sleep 5"]
      dependencies: ["step1"]   # Depends on step1
      dataSize: "50MB"
      runtime: 5
      
  schedulerName: dag-scheduler
  namespace: default
```

### Step Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | string | Unique name for the step (must be lowercase, alphanumeric with hyphens) |
| `image` | string | Container image to use |
| `command` | array | Command to execute |
| `args` | array | Arguments for the command |
| `dependencies` | array | List of step names this step depends on |
| `dataSize` | string | Size of output data (e.g., "100MB", "1GB") - used by HEFT for scheduling |
| `runtime` | integer | Expected runtime in seconds - used by HEFT for scheduling |
| `cpu` | string | CPU request (e.g., "500m", "1") |
| `memory` | string | Memory request (e.g., "256Mi", "1Gi") |
| `constraints.nodeNames` | array | List of allowed node names for this step |

### Node Constraints

You can restrict which nodes a step can run on using the `constraints` field:

```yaml
- name: gpu-task
  image: nvidia/cuda:latest
  command: ["python", "train.py"]
  dependencies: ["preprocess"]
  constraints:
    nodeNames:
      - gpu-node-1
      - gpu-node-2
      - gpu-node-3
```

The HEFT scheduler will only consider these nodes when computing the optimal placement.

## Example DAGs

### Diamond Pattern (4 tasks)

```
     step1
    /     \
 step2   step3
    \     /
     step4
```

See: `phase-1/example/dag1-diamond.yml`

### Fork-Join Pattern (6 tasks)

```
        +-> task-b --+
        |            |
task-a -+-> task-c --+-> task-f
        |            |
        +-> task-d --+-> task-e
```

See: `phase-1/example/dag2-fork.yml`

## Running Tests

### Single DAG Test

```bash
# Apply a DAG
kubectl apply -f phase-1/example/dag1-diamond.yml

# Watch pod creation and scheduling
kubectl get pods -w

# Check which nodes pods are scheduled on
kubectl get pods -o wide

# View HEFT controller logs
kubectl logs -n kube-system deployment/heft-controller -f
```

### Multi-DAG Concurrent Test

The framework supports running multiple DAGs concurrently. A test script is provided:

```bash
cd phase-1/example
./run-multi-dag-test.sh
```

This script:
1. Cleans up any previous test runs
2. Submits both DAGs simultaneously
3. Monitors completion of all tasks
4. Reports individual and total makespan
5. Shows pod placement summary

**Sample Output:**
```
==============================================
Multi-DAG HEFT Scheduling Test
==============================================

Applying DAG1 (diamond pattern)...
dag.workflow.example.com/dag1-diamond created
Applying DAG2 (fork pattern)...
dag.workflow.example.com/dag2-fork created

Both DAGs submitted. Waiting for completion...

[10:25:45] DAG1 (diamond) COMPLETED
[10:26:17] DAG2 (fork) COMPLETED

==============================================
RESULTS
==============================================
DAG1 (diamond) Makespan: 92.21 seconds
DAG2 (fork) Makespan:    123.82 seconds
Total Makespan:          123.82 seconds
==============================================

=== Pod Placement Summary ===

--- DAG1 (Diamond) Pods ---
NAME                 NODE     STATUS
dag1-diamond-step1   anrg-2   Succeeded
dag1-diamond-step2   anrg-2   Succeeded
dag1-diamond-step3   anrg-3   Succeeded
dag1-diamond-step4   anrg-7   Succeeded
```

### Cleanup

```bash
# Delete specific DAGs
kubectl delete dag dag1-diamond dag2-fork

# Delete all DAGs
kubectl delete dag --all

# Delete associated pods
kubectl delete pods -l dag-name=dag1-diamond
kubectl delete pods -l dag-name=dag2-fork
```

## HEFT Scheduling Algorithm

The HEFT (Heterogeneous Earliest Finish Time) algorithm optimizes task placement by considering:

1. **Computation Costs**: Expected runtime of each task on each node
2. **Communication Costs**: Data transfer time between nodes based on `dataSize`
3. **Node Constraints**: Only considers nodes specified in `constraints.nodeNames`
4. **Task Dependencies**: Ensures predecessors complete before dependents start

The algorithm computes a priority (rank) for each task and schedules them in order of decreasing rank to the node that minimizes the earliest finish time.

### Makespan Prediction

The controller logs show predicted makespan and actual task placements:

```
[HEFT] Task step1 -> Node anrg-2 (EFT: 10.00, Start: 0.00, CommCost: 0.00)
[HEFT] Task step2 -> Node anrg-2 (EFT: 20.00, Start: 10.00, CommCost: 0.00)
[HEFT] Task step3 -> Node anrg-3 (EFT: 40.00, Start: 10.00, CommCost: 20.00)
[HEFT] Task step4 -> Node anrg-7 (EFT: 104.00, Start: 40.00, CommCost: 54.00)
[HEFT] Schedule computed - Estimated Makespan: 104.00 seconds
```

## Data Transfer

Data transfer between tasks is handled automatically:

- **Same-node transfers**: Direct file copy via shared hostPath (`/data/dag-outputs/`)
- **Cross-node transfers**: HTTP transfer via Data Agent DaemonSet

The controller injects environment variables into each pod:
- `NODE_NAME`: Current node name
- `DEP_<NAME>_NODE`: Node where dependency ran
- `DEP_<NAME>_URL`: URL to fetch dependency output

## Project Structure

```
dsf/
├── phase-1/
│   ├── crd/
│   │   └── dag-crd.yml           # DAG Custom Resource Definition
│   ├── controller/
│   │   ├── heft-controller/      # HEFT scheduling controller
│   │   │   ├── main.go
│   │   │   ├── Dockerfile
│   │   │   ├── deployment.yml
│   │   │   └── rbac.yml
│   │   └── random-controller/    # Random scheduling controller
│   │       ├── main.go
│   │       ├── Dockerfile
│   │       ├── deployment.yml
│   │       └── rbac.yml
│   ├── scheduler/                # Custom Kubernetes scheduler plugin
│   │   ├── cmd/
│   │   ├── plugins/
│   │   ├── Dockerfile
│   │   └── deployment.yml
│   ├── data-agent/               # Data transfer DaemonSet
│   │   ├── main.go
│   │   ├── Dockerfile
│   │   └── daemonset.yml
│   ├── example/                  # Example DAGs and test scripts
│   │   ├── dag1-diamond.yml
│   │   ├── dag2-fork.yml
│   │   └── run-multi-dag-test.sh
│   └── net-metrics/              # Network measurement tools
│       ├── iperf3/
│       ├── pathload/
│       └── link-scorer/
└── README.md
```

## Troubleshooting

### Pods not being scheduled

1. Check if the HEFT controller is running:
   ```bash
   kubectl get pods -n kube-system | grep heft-controller
   ```

2. Restart the controller if needed:
   ```bash
   kubectl rollout restart deployment/heft-controller -n kube-system
   ```

3. Check controller logs:
   ```bash
   kubectl logs -n kube-system deployment/heft-controller --tail=50
   ```

### Data transfer failures

1. Verify Data Agent is running on all nodes:
   ```bash
   kubectl get pods -n kube-system | grep data-agent
   ```

2. Check Data Agent logs:
   ```bash
   kubectl logs -n kube-system daemonset/data-agent
   ```

### Node constraint violations

Ensure the nodes specified in `constraints.nodeNames` exist and are schedulable:
```bash
kubectl get nodes
```

## License

[Add license information]

## Contributing

[Add contribution guidelines]
