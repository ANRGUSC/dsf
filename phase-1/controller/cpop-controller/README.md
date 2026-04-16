# CPOP Scheduler

A Critical Path on a Processor (CPOP) scheduler for Kubernetes DAG workflows.

## Features

- **CPOP Algorithm**: Computes optimal task-to-node mapping upfront using critical path analysis
- **Dual Ranking**: Calculates both upward and downward ranks for priority computation
- **Critical Path Identification**: Tasks on the critical path are all assigned to a single "CP node"
- **Smart Placement**: Non-critical tasks placed by minimum EFT (same as HEFT)

## How It Works

### 1. When DAG is Detected
- Computes average computation cost for each task across allowed nodes
- Calculates **upward rank** and **downward rank** for all tasks
- Computes **priority** = upward + downward rank
- Identifies **critical path tasks** (priority ≈ max priority)
- Selects **CP node** — the node minimizing total cost for all critical path tasks
- Schedules: CP tasks → CP node, non-CP tasks → min-EFT node

### 2. When Task is Ready
- Uses pre-computed node assignment (no re-calculation)
- Creates pod with node affinity to the designated node

### Key Formulas

```
upward_rank(t)  = avg_comp(t) + max_over_successors(comm + upward_rank(succ))
downward_rank(t) = max_over_predecessors(downward_rank(pred) + avg_comp(pred) + comm)
priority(t) = upward_rank(t) + downward_rank(t)
```

## Deployment

### 1. Build and Push Image
```bash
cd /home/anrg/dsf/phase-1/controller/cpop-controller
docker buildx build -t mohammadalikh/cpop-controller:latest --push .
```

### 2. Apply RBAC
```bash
kubectl apply -f rbac.yml
```

### 3. Deploy Controller
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

# Check CPOP scheduler logs
kubectl logs deployment/cpop-controller -n kube-system | grep "\[CPOP\]"

# View critical path and schedule decisions
kubectl logs deployment/cpop-controller -n kube-system | grep -E "Critical Path|Task.*->|Makespan:"
```

## Example Output

```
[CPOP] Computing schedule for DAG sample-workflow with 4 tasks
[CPOP] Critical Path Tasks: [step1 step2 step4]
[CPOP] Selected Critical Path Node: anrg-3 (Total Cost: 30.00)
[CPOP] Task step1 -> anrg-3 (Priority: 30.00, CP: true)
[CPOP] Task step2 -> anrg-3 (Priority: 30.00, CP: true)
[CPOP] Task step3 -> anrg-5 (Priority: 20.00, CP: false)
[CPOP] Task step4 -> anrg-3 (Priority: 30.00, CP: true)
[CPOP] Schedule computed for DAG sample-workflow - Estimated Makespan: 30.00 seconds
```

## Comparison: CPOP vs HEFT vs Random

| Feature | Random | HEFT | CPOP |
|---------|--------|------|------|
| Scheduling Strategy | Random | Min-EFT for all tasks | CP tasks → CP node, others → min-EFT |
| Uses Upward Rank | ❌ | ✅ | ✅ |
| Uses Downward Rank | ❌ | ❌ | ✅ |
| Critical Path Aware | ❌ | ❌ | ✅ |
| Pre-computes Schedule | ❌ | ✅ | ✅ |

## Configuration

### Link Bandwidth
Currently set to 100 Mbps (`0.1` Gbps). To change, modify `LinkBandwidthGbps` in `main.go`:
```go
const (
    LinkBandwidthGbps = 0.1 // 100 Mbps
)
```

### DAG Task Fields
- `cpu`, `memory`: Resource requirements
- `dataSize`: Output data size (e.g., "100MB", "1GB")
- `runtime`: Expected runtime in seconds
- `dependencies`: List of parent tasks
- `constraints.nodeNames`: Optional list of allowed nodes

## Troubleshooting

### No schedule computed
Check if DAG was detected and CRD exists: `kubectl get crd dags.workflow.example.com`

### Pods stuck in Pending
- Check logs: `kubectl logs deployment/cpop-controller -n kube-system`
- Verify RBAC: `kubectl get clusterrolebinding cpop-controller`
- Ensure nodes have sufficient resources
