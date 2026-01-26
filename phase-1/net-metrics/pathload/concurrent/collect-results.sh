#!/bin/bash

# Collect and summarize concurrent pathload measurement results

KUBECTL_CMD="sudo k3s kubectl"

echo "========================================="
echo "Concurrent Pathload Measurement Results"
echo "Full Matrix Summary"
echo "========================================="
echo ""

# Function to extract bandwidth from logs
extract_bandwidth() {
    local pod_name=$1
    local logs=$($KUBECTL_CMD logs $pod_name 2>&1)
    
    # Look for "Available bandwidth range" line
    echo "$logs" | grep -i "Available bandwidth range" | head -1 || echo "N/A"
}

# All measurement pairs
measurements=("3to4" "3to5" "4to3" "4to5" "5to3" "5to4")

echo "=== Bandwidth Results ==="
for measurement in "${measurements[@]}"; do
    receiver_pod="pathload-receiver-$measurement"
    
    if $KUBECTL_CMD get pod $receiver_pod &>/dev/null; then
        echo "${measurement//to/→}: $(extract_bandwidth $receiver_pod)"
    else
        echo "${measurement//to/→}: Pod not found"
    fi
done

echo ""
echo "========================================="
echo "Detailed Logs:"
echo "========================================="
echo ""

for measurement in "${measurements[@]}"; do
    receiver_pod="pathload-receiver-$measurement"
    if $KUBECTL_CMD get pod $receiver_pod &>/dev/null; then
        echo "--- $receiver_pod ---"
        $KUBECTL_CMD logs $receiver_pod 2>&1 | grep -A 5 "RESULT\|Available bandwidth" || echo "No results found"
        echo ""
    fi
done
