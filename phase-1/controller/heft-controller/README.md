# HEFT Scheduler

A Heterogeneous Earliest Finish Time (HEFT) scheduler for Kubernetes DAG workflows.

## Features

- **HEFT Algorithm**: Computes optimal task-to-node mapping upfront before any deployment
- **Task Ranking**: Calculates upward rank considering computation and communication costs
- **Smart Placement**: Minimizes makespan by considering:
  - Computation costs (based on available node resources)
  - Communication costs (data transfer between dependencies)
  - Node availability times

## How It Works

### 1. When DAG is Detected
- Computes average computation cost for each task on each node
- Calculates upward rank for all tasks (task priority)
- Sorts tasks by descending rank
- For each task, finds the node with earliest finish time (EFT)
- Stores the complete schedule

### 2. When Task is Ready
- Uses pre-computed node assignment (no re-calculation)
- Binds pod to the designated node

### Computation Cost
```
weight = (required_cpu / available_cpu) + (required_ram / available_ram)
computation_cost = weight * scaling_factor
```
Lower cost = more available resources = faster execution

### Communication Cost
```
comm_cost = data_size (bytes) / link_bandwidth (bytes/sec)
```
Currently assumes 1 Gbps links between all nodes

### Upward Rank
```
rank(task) = avg_computation_cost(task) + max(comm_cost + rank(successor))
```

## Deployment

### 1. Build and Push Image
```bash
cd /home/anrg/dsf/phase-1/Schedulers/src/heft-controller
docker buildx build -t <your-registry>/heft-controller:latest --push .
```

### 2. Apply RBAC
```bash
kubectl apply -f rbac.yml
```

### 3. Deploy Scheduler
```bash
kubectl apply -f deployment.yml
```

### 4. Test with Sample DAG
```bash
kubectl apply -f test-dag.yml
```

### 5. Monitor
```bash
# Watch pods being scheduled
kubectl get pods -o wide --watch

# Check HEFT scheduler logs
kubectl logs deployment/heft-controller -n kube-system | grep "\[HEFT\]"

# View schedule decisions
kubectl logs deployment/heft-controller -n kube-system | grep "Task.*-> Node"
```

## Example Output

```
[HEFT] Computing schedule for DAG sample-workflow with 4 tasks on 8 nodes
[HEFT] Task ranks: 
  step1: 25.30
  step2: 18.45
  step3: 18.20
  step4: 10.15
[HEFT] Task step1 -> Node anrg-3 (EFT: 12.50, Start: 0.00)
[HEFT] Task step2 -> Node anrg-5 (EFT: 24.80, Start: 13.20)
[HEFT] Task step3 -> Node anrg-7 (EFT: 25.10, Start: 13.50)
[HEFT] Task step4 -> Node anrg-2 (EFT: 35.60, Start: 26.30)
[HEFT] Schedule computed for DAG sample-workflow - Makespan: 35.60 seconds
[HEFT] Successfully bound pod sample-workflow-step1 to pre-assigned node anrg-3
```

## Comparison with Random Scheduler

| Feature | Random | HEFT |
|---------|--------|------|
| Scheduling Strategy | Random node selection | Optimal EFT-based placement |
| Considers Resources | ❌ No | ✅ Yes (CPU/Memory) |
| Considers Communication | ❌ No | ✅ Yes (Data transfer) |
| Considers Dependencies | ❌ No | ✅ Yes (Task ordering) |
| Pre-computes Schedule | ❌ No | ✅ Yes (entire DAG upfront) |
| Makespan Optimization | ❌ No | ✅ Yes |

## Configuration

### Link Bandwidth
Currently hardcoded to 1 Gbps. To change, modify `LinkBandwidthGbps` constant in `main.go`:
```go
const (
    LinkBandwidthGbps = 10.0 // Change to 10 Gbps
)
```

### DAG Requirements
Each task in the DAG should specify:
- `cpu`: Required CPU (e.g., "500m", "2")
- `memory`: Required memory (e.g., "256Mi", "1Gi")
- `dataSize`: Output data size (e.g., "100MB", "1GB")
- `runtime`: Expected runtime in seconds (optional, currently not used)
- `dependencies`: List of parent tasks

## Architecture

```
┌──────────────┐
│   DAG CRD    │
└──────┬───────┘
       │ Detected
       ▼
┌──────────────────┐
│  Compute HEFT    │
│   Schedule       │
│  (Upfront)       │
└──────┬───────────┘
       │ Store: task -> node
       ▼
┌──────────────────┐
│  Create Pods     │
│ (Dependencies)   │
└──────┬───────────┘
       │ Pod pending
       ▼
┌──────────────────┐
│  Bind to Node    │
│ (Pre-assigned)   │
└──────────────────┘
```

## Troubleshooting

### No schedule computed
**Issue**: `No HEFT schedule found for DAG`
**Solution**: Check if DAG was detected. Ensure CRD has `schedulerName: heft-controller`

### Pods not being scheduled
**Issue**: Pods stuck in Pending
**Solution**: 
- Check scheduler logs: `kubectl logs deployment/heft-controller -n kube-system`
- Verify RBAC permissions
- Ensure nodes have sufficient resources

### Incorrect node assignments
**Issue**: Tasks not placed optimally
**Solution**: 
- Verify CPU/Memory specifications in DAG
- Check node metrics are available
- Ensure dataSize is specified for communication cost calculation

