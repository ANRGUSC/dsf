#!/bin/bash

# Script to measure DAG makespan

DAG_NAME=${1:-"sample-workflow"}
KUBECTL="/usr/local/bin/k3s kubectl"

echo "Measuring makespan for DAG: $DAG_NAME"
echo "Waiting for all pods to complete..."

# Wait for all pods to complete
while true; do
    PENDING=$($KUBECTL get pods -l dag-name=$DAG_NAME --no-headers 2>/dev/null | grep -c "Pending\|ContainerCreating\|Running")
    if [ "$PENDING" -eq 0 ]; then
        break
    fi
    sleep 2
done

echo "All pods completed. Calculating makespan..."

# Get all pods for this DAG
PODS=$($KUBECTL get pods -l dag-name=$DAG_NAME -o json)

# Extract timestamps
START_TIMES=$(echo "$PODS" | jq -r '.items[].status.containerStatuses[] | select(.name != "data-server") | .state.terminated.startedAt // empty' | sort | head -1)
END_TIMES=$(echo "$PODS" | jq -r '.items[].status.containerStatuses[] | select(.name != "data-server") | .state.terminated.finishedAt // empty' | sort | tail -1)

if [ -z "$START_TIMES" ] || [ -z "$END_TIMES" ]; then
    echo "Error: Could not get timestamps. Pods may not be completed yet."
    exit 1
fi

# Calculate makespan in seconds
START_EPOCH=$(date -d "$START_TIMES" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%SZ" "$START_TIMES" +%s)
END_EPOCH=$(date -d "$END_TIMES" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%SZ" "$END_TIMES" +%s)
MAKESPAN=$((END_EPOCH - START_EPOCH))

echo ""
echo "===== MAKESPAN RESULTS ====="
echo "First task started:  $START_TIMES"
echo "Last task finished:  $END_TIMES"
echo "Makespan:            $MAKESPAN seconds"
echo "============================"
echo ""

# Show pod placement
echo "Pod Placement:"
$KUBECTL get pods -l dag-name=$DAG_NAME -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,START:.status.startTime,DURATION:.status.containerStatuses[0].state.terminated.finishedAt --no-headers

