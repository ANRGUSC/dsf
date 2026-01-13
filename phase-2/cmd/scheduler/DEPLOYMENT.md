# Deployment Guide for CTG Scheduler

This guide explains how to deploy and test the ContinuousTaskGraph scheduler.

## Prerequisites

- Kubernetes cluster (k3s or similar)
- kubectl configured
- Docker (for building images)
- Go 1.24+ (for building scheduler)

## Step 1: Build Scheduler Image

```bash
cd /home/anrg/dsf/phase-2/cmd/scheduler

# Build the scheduler
docker build -t ctg-scheduler:latest .

# If using a registry, tag and push:
# docker tag ctg-scheduler:latest <your-registry>/ctg-scheduler:latest
# docker push <your-registry>/ctg-scheduler:latest
```

## Step 2: Deploy CRD

```bash
# Apply the ContinuousTaskGraph CRD
kubectl apply -f /home/anrg/dsf/phase-2/api/v1/continuous-task-graph-crd.yml
```

## Step 3: Deploy Scheduler RBAC

```bash
# Apply RBAC permissions
kubectl apply -f /home/anrg/dsf/phase-2/cmd/scheduler/rbac.yml
```

## Step 4: Deploy Scheduler

```bash
# Update deployment.yml with your image if needed
# Then deploy:
kubectl apply -f /home/anrg/dsf/phase-2/cmd/scheduler/deployment.yml
```

## Step 5: Build Test Containers

```bash
# Build data-source container
cd /home/anrg/dsf/phase-2/test-containers/data-source
docker build -t data-source:test .

# Build data-processor container
cd ../data-processor
docker build -t data-processor:test .

# If using a registry, tag and push:
# docker tag data-source:test <your-registry>/data-source:test
# docker push <your-registry>/data-source:test
# docker tag data-processor:test <your-registry>/data-processor:test
# docker push <your-registry>/data-processor:test
```

## Step 6: Deploy Test Task Graph

```bash
# Apply the test task graph
kubectl apply -f /home/anrg/dsf/phase-2/api/v1/test-task-graph.yml
```

## Step 7: Monitor Deployment

```bash
# Watch pods being created
kubectl get pods -w

# Check scheduler logs
kubectl logs -n kube-system deployment/ctg-scheduler -f

# Check task graph status
kubectl get ctg -o yaml

# Check services
kubectl get svc

# Check pod logs
kubectl logs -l ctg-name=test-task-graph -f
```

## Troubleshooting

### Scheduler not scheduling pods

1. Check scheduler logs: `kubectl logs -n kube-system deployment/ctg-scheduler`
2. Verify RBAC: `kubectl get clusterrole ctg-scheduler -o yaml`
3. Check if pods are pending: `kubectl get pods`

### ZeroMQ not working

1. Check if services are created: `kubectl get svc`
2. Check pod environment variables: `kubectl exec <pod-name> -- env | grep ZMQ`
3. Check pod logs for ZeroMQ connection errors

### Pods not starting

1. Check pod events: `kubectl describe pod <pod-name>`
2. Check image pull errors
3. Verify resource requests/limits are reasonable

## Cleanup

```bash
# Delete task graph
kubectl delete ctg test-task-graph

# Delete scheduler
kubectl delete -f /home/anrg/dsf/phase-2/cmd/scheduler/deployment.yml
kubectl delete -f /home/anrg/dsf/phase-2/cmd/scheduler/rbac.yml

# Delete CRD (this will delete all ContinuousTaskGraphs)
kubectl delete -f /home/anrg/dsf/phase-2/api/v1/continuous-task-graph-crd.yml
```
