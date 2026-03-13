# DAG Scheduler Plugin

A custom Kubernetes scheduler plugin that extends the default scheduler with DAG-aware node constraints filtering.

## Features

- **Node Constraints Filtering**: Filters nodes based on `dag.example.com/allowed-nodes` pod annotation
- **Extends Default Scheduler**: Inherits all default scheduler features (retries, caching, preemption, etc.)
- **Plugin Architecture**: Uses Kubernetes Scheduler Framework for clean integration

## How It Works

1. The random-controller controller creates pods with `schedulerName: dag-scheduler`
2. Pods can have annotations like `dag.example.com/allowed-nodes: anrg-2,anrg-3,anrg-7`
3. The DAGConstraints plugin filters out nodes not in the allowed list
4. The default scheduler handles all other scheduling logic (resource requests, affinity, etc.)

## Build & Deploy

```bash
# Build the image
docker build -t mohammadalikh/dag-scheduler:latest .

# Push to registry
docker push mohammadalikh/dag-scheduler:latest

# Deploy to cluster
kubectl apply -f rbac.yml
kubectl apply -f configmap.yml
kubectl apply -f deployment.yml
```

## Files

- `cmd/dag-scheduler/main.go` - Entry point that registers the plugin
- `plugins/dagconstraints/dag_constraints.go` - The constraint filtering plugin
- `scheduler-config.yml` - Scheduler configuration enabling the plugin
- `deployment.yml` - Kubernetes deployment
- `rbac.yml` - ServiceAccount and ClusterRoleBinding
- `configmap.yml` - ConfigMap for scheduler config
