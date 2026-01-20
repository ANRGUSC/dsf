#!/bin/bash

# Multi-DAG Test Script
# Runs two DAGs concurrently and measures the total makespan

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=============================================="
echo "Multi-DAG HEFT Scheduling Test"
echo "=============================================="
echo ""

# Clean up any previous runs
echo "Cleaning up previous test runs..."
kubectl delete dag dag1-diamond dag2-fork 2>/dev/null
kubectl delete pod -l dag-name=dag1-diamond --force --grace-period=0 2>/dev/null
kubectl delete pod -l dag-name=dag2-fork --force --grace-period=0 2>/dev/null
sleep 5

# Check that HEFT controller is running
echo "Checking HEFT controller status..."
kubectl get deployment heft-controller -n kube-system >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: heft-controller not found. Please deploy it first."
    exit 1
fi
echo "HEFT controller is running."
echo ""

# Start timing
START_TIME=$(date +%s.%N)
echo "=== Starting Multi-DAG Test at $(date) ==="
echo ""

# Apply both DAGs simultaneously
echo "Applying DAG1 (diamond pattern)..."
kubectl apply -f "$SCRIPT_DIR/dag1-diamond.yml"

echo "Applying DAG2 (fork pattern)..."
kubectl apply -f "$SCRIPT_DIR/dag2-fork.yml"

echo ""
echo "Both DAGs submitted. Waiting for completion..."
echo ""

# Wait for both DAGs to complete
# DAG1 final step: step4
# DAG2 final step: taskF
dag1_done=false
dag2_done=false
timeout=300  # 5 minutes timeout

for i in $(seq 1 $timeout); do
    sleep 2
    
    # Check DAG1 completion
    if [ "$dag1_done" = false ]; then
        dag1_status=$(kubectl get pod dag1-diamond-step4 -o jsonpath='{.status.phase}' 2>/dev/null)
        if [ "$dag1_status" = "Succeeded" ]; then
            DAG1_END=$(date +%s.%N)
            dag1_done=true
            echo "[$(date +%H:%M:%S)] DAG1 (diamond) COMPLETED"
        fi
    fi
    
    # Check DAG2 completion
    if [ "$dag2_done" = false ]; then
        dag2_status=$(kubectl get pod dag2-fork-task-f -o jsonpath='{.status.phase}' 2>/dev/null)
        if [ "$dag2_status" = "Succeeded" ]; then
            DAG2_END=$(date +%s.%N)
            dag2_done=true
            echo "[$(date +%H:%M:%S)] DAG2 (fork) COMPLETED"
        fi
    fi
    
    # Check if both are done
    if [ "$dag1_done" = true ] && [ "$dag2_done" = true ]; then
        END_TIME=$(date +%s.%N)
        break
    fi
    
    # Progress indicator every 10 seconds
    if [ $((i % 5)) -eq 0 ]; then
        echo -n "."
    fi
done

echo ""
echo ""

# Calculate makespans
if [ "$dag1_done" = true ] && [ "$dag2_done" = true ]; then
    TOTAL_MAKESPAN=$(echo "$END_TIME - $START_TIME" | bc)
    DAG1_MAKESPAN=$(echo "$DAG1_END - $START_TIME" | bc)
    DAG2_MAKESPAN=$(echo "$DAG2_END - $START_TIME" | bc)
    
    echo "=============================================="
    echo "RESULTS"
    echo "=============================================="
    echo "DAG1 (diamond) Makespan: $DAG1_MAKESPAN seconds"
    echo "DAG2 (fork) Makespan:    $DAG2_MAKESPAN seconds"
    echo "Total Makespan:          $TOTAL_MAKESPAN seconds"
    echo "=============================================="
else
    echo "ERROR: Test timed out!"
    echo "DAG1 completed: $dag1_done"
    echo "DAG2 completed: $dag2_done"
fi

echo ""
echo "=== Pod Placement Summary ==="
echo ""
echo "--- DAG1 (Diamond) Pods ---"
kubectl get pods -l dag-name=dag1-diamond -o custom-columns='NAME:.metadata.name,NODE:.spec.nodeName,STATUS:.status.phase'

echo ""
echo "--- DAG2 (Fork) Pods ---"
kubectl get pods -l dag-name=dag2-fork -o custom-columns='NAME:.metadata.name,NODE:.spec.nodeName,STATUS:.status.phase'

echo ""
echo "=== HEFT Controller Logs (last 30 lines) ==="
kubectl logs deployment/heft-controller -n kube-system --tail=30 | grep -E "DAG|Step|task|Created|assigned|EFT"
