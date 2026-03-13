# Link Scorer

A lightweight CLI tool for computing node-to-node link scores in a k3s cluster using Microsoft Retina metrics and Prometheus.

## Overview

Link Scorer passively collects network metrics from Retina (via Prometheus) and computes a score for each directed edge (src → dst) based on:
- Residual egress capacity at source node
- Residual ingress capacity at destination node  
- Drop-based penalty factors for both nodes

Higher scores indicate better link quality for routing traffic.

## Installation

```bash
go build -o link-scorer main.go
```

Or install directly:
```bash
go install ./...
```

## Usage

### Basic Usage

```bash
# Using environment variables
export PROM_URL="http://prometheus:9090"
./link-scorer

# Using flags
./link-scorer -prom-url http://prometheus:9090 -window 30s -topk 20
```

### Configuration Options

| Option | Flag | Env Var | Default | Description |
|--------|------|---------|---------|-------------|
| Prometheus URL | `-prom-url` | `PROM_URL` | *required* | Prometheus API endpoint |
| Bearer Token | `-prom-token` | `PROM_TOKEN` | "" | Optional auth token |
| Time Window | `-window` | `WINDOW` | `30s` | Rate calculation window |
| Link Capacity | `-capacity` | `CAPACITY_BPS` | `1e9` | Per-node link capacity (1 Gbps) |
| Drop Reference | `-drop-ref` | `DROP_REF_BPS` | `1e7` | Reference drop rate for penalty |
| Output Format | `-output` | `OUTPUT` | `json` | `json` or `csv` |
| Top K Edges | `-topk` | `TOPK` | `20` | Number of top edges (0 = all) |
| Node List | `-nodes` | `NODES` | auto | Comma-separated node list |

### Examples

**Get top 10 links:**
```bash
./link-scorer -prom-url http://prometheus:9090 -topk 10
```

**CSV output:**
```bash
./link-scorer -prom-url http://prometheus:9090 -output csv -topk 50
```

**Custom window and capacity:**
```bash
./link-scorer \
  -prom-url http://prometheus:9090 \
  -window 60s \
  -capacity 2e9 \
  -drop-ref 5e6
```

**Specific nodes only:**
```bash
./link-scorer \
  -prom-url http://prometheus:9090 \
  -nodes "anrg-1,anrg-2,anrg-3"
```

## Output Format

### JSON Output

```json
{
  "timestamp": "2026-01-27T02:00:00Z",
  "window": "30s",
  "top_k": 20,
  "nodes": {
    "anrg-1": {
      "node": "anrg-1",
      "egress_bps": 200000000,
      "ingress_bps": 150000000,
      "drop_bps": 1000000,
      "residual_egress": 800000000,
      "residual_ingress": 850000000,
      "penalty": 0.9
    }
  },
  "edges": [
    {
      "src": "anrg-1",
      "dst": "anrg-2",
      "score": 720000000
    }
  ]
}
```

### CSV Output

```csv
src,dst,score
anrg-1,anrg-2,720000000.00
anrg-2,anrg-1,630000000.00
```

## Scoring Algorithm

For each edge (src → dst):

1. **Residual Capacity:**
   - `residual_egress(src) = max(0, capacity - egress_bps(src))`
   - `residual_ingress(dst) = max(0, capacity - ingress_bps(dst))`

2. **Penalty (per node):**
   - `penalty = clamp(1.0 - drop_bps / drop_ref_bps, min=0.1, max=1.0)`

3. **Score:**
   - `score(src→dst) = min(residual_egress(src), residual_ingress(dst)) × penalty(src) × penalty(dst)`

## Prometheus Queries

The tool uses these PromQL queries:

- **Egress:** `sum by (instance) (8 * rate(networkobservability_forward_bytes{direction="egress"}[30s]))`
- **Ingress:** `sum by (instance) (8 * rate(networkobservability_forward_bytes{direction="ingress"}[30s]))`
- **Drops:** `sum by (instance) (8 * rate(networkobservability_drop_bytes[30s]))`

## Running in Cluster

### Finding Prometheus URL

If Prometheus is running in-cluster, find its service:

```bash
# List Prometheus services
kubectl get svc -A | grep prometheus

# Common locations:
# - prometheus-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090
# - prometheus-server.monitoring.svc.cluster.local:80
```

### Deployment

See `deployment.yaml` for a Kubernetes Deployment example.

```bash
# Update PROM_URL in deployment.yaml, then:
kubectl apply -f deployment.yaml
kubectl logs -f deployment/link-scorer
```

### Running as CronJob

To run periodically (e.g., every minute):

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: link-scorer
spec:
  schedule: "*/1 * * * *"  # Every minute
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: link-scorer
            image: link-scorer:latest
            env:
            - name: PROM_URL
              value: "http://prometheus:9090"
            - name: TOPK
              value: "20"
          restartPolicy: OnFailure
```

## Testing

```bash
go test -v
```

## Notes

- Missing metrics are treated as zero (with warnings logged)
- Node discovery uses the `instance` label from Retina metrics
- Output is deterministic (sorted keys, stable ordering)
- All rates are computed in bits/second (bytes × 8)
