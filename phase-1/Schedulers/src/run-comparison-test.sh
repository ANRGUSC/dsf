#!/bin/bash

# Script to run comparison test between random and HEFT schedulers

set -e

echo "=========================================="
echo "Scheduler Comparison Test"
echo "=========================================="
echo ""

# Clean up any existing DAGs
echo "Step 1: Cleaning up existing DAGs and pods..."
kubectl delete dag --all 2>/dev/null || true
kubectl delete pods -l dag-name --all 2>/dev/null || true
kubectl delete svc -l dag-name --all 2>/dev/null || true
sleep 3
echo "✓ Cleanup complete"
echo ""

# Test Random Scheduler
echo "=========================================="
echo "TEST 1: RANDOM SCHEDULER"
echo "=========================================="
echo ""

# Update test DAG to use random scheduler
sed 's/schedulerName:.*/schedulerName: random-scheduler/' /home/anrg/dsf/phase-1/Schedulers/src/crd/test-dag.yml > /tmp/test-dag-random.yml

echo "Deploying DAG with random scheduler..."
kubectl apply -f /tmp/test-dag-random.yml

echo "Waiting for pods to be created..."
sleep 5

echo ""
echo "Pod placement:"
kubectl get pods -l dag-name=sample-workflow -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,STATUS:.status.phase --no-headers

echo ""
echo "Waiting for all pods to complete..."
cd /home/anrg/dsf/phase-1/Schedulers/src
bash measure-makespan.sh sample-workflow

RANDOM_MAKESPAN=$(kubectl get pods -l dag-name=sample-workflow -o json | jq -r '[.items[].status.containerStatuses[] | select(.name != "data-server") | .state.terminated.finishedAt // empty] | sort | .[-1]' | xargs -I {} date -d {} +%s 2>/dev/null || echo "0")
RANDOM_START=$(kubectl get pods -l dag-name=sample-workflow -o json | jq -r '[.items[].status.containerStatuses[] | select(.name != "data-server") | .state.terminated.startedAt // empty] | sort | .[0]' | xargs -I {} date -d {} +%s 2>/dev/null || echo "0")

echo ""
echo "Pod details:"
kubectl get pods -l dag-name=sample-workflow -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,CONTAINERS:.spec.containers[*].name --no-headers

echo ""
echo "Cleaning up random scheduler test..."
kubectl delete dag sample-workflow
kubectl delete pods -l dag-name=sample-workflow 2>/dev/null || true
kubectl delete svc -l dag-name=sample-workflow 2>/dev/null || true
sleep 5
echo ""

# Test HEFT Scheduler
echo "=========================================="
echo "TEST 2: HEFT SCHEDULER"
echo "=========================================="
echo ""

echo "Deploying DAG with HEFT scheduler..."
kubectl apply -f /home/anrg/dsf/phase-1/Schedulers/src/heft-scheduler/test-dag.yml

echo "Waiting for pods to be created..."
sleep 5

echo ""
echo "Pod placement:"
kubectl get pods -l dag-name=sample-workflow -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,STATUS:.status.phase --no-headers

echo ""
echo "HEFT Schedule (from logs):"
kubectl logs -n kube-system -l app=heft-scheduler --tail=20 | grep -E "Task.*->|Makespan:" || true

echo ""
echo "Waiting for all pods to complete..."
cd /home/anrg/dsf/phase-1/Schedulers/src
bash measure-makespan.sh sample-workflow

HEFT_MAKESPAN=$(kubectl get pods -l dag-name=sample-workflow -o json | jq -r '[.items[].status.containerStatuses[] | select(.name != "data-server") | .state.terminated.finishedAt // empty] | sort | .[-1]' | xargs -I {} date -d {} +%s 2>/dev/null || echo "0")
HEFT_START=$(kubectl get pods -l dag-name=sample-workflow -o json | jq -r '[.items[].status.containerStatuses[] | select(.name != "data-server") | .state.terminated.startedAt // empty] | sort | .[0]' | xargs -I {} date -d {} +%s 2>/dev/null || echo "0")

echo ""
echo "Pod details:"
kubectl get pods -l dag-name=sample-workflow -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,CONTAINERS:.spec.containers[*].name --no-headers

echo ""
echo "=========================================="
echo "COMPARISON SUMMARY"
echo "=========================================="
echo ""
echo "RANDOM SCHEDULER:"
kubectl get pods -l dag-name=sample-workflow -o json | jq -r '.items[] | "  \(.metadata.labels["dag-step"]) -> \(.spec.nodeName)"' | sort
echo ""
echo "HEFT SCHEDULER:"
kubectl get pods -l dag-name=sample-workflow -o json | jq -r '.items[] | "  \(.metadata.labels["dag-step"]) -> \(.spec.nodeName)"' | sort
echo ""
echo "=========================================="
echo ""

