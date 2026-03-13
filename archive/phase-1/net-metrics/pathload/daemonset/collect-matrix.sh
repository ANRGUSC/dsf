#!/bin/bash

# Collect full measurement matrix from all orchestrator pods

KUBECTL_CMD="sudo k3s kubectl"

echo "========================================="
echo "Pathload Full Measurement Matrix"
echo "========================================="
echo ""

# Get all orchestrator pods
pods=$($KUBECTL_CMD get pods -l app=pathload-orchestrator -o jsonpath='{.items[*].metadata.name}')

if [ -z "$pods" ]; then
    echo "No orchestrator pods found!"
    exit 1
fi

# Temporary file for results
temp_file=$(mktemp)
trap "rm -f $temp_file" EXIT

# Collect results from each pod
for pod in $pods; do
    echo "Collecting results from $pod..."
    logs=$($KUBECTL_CMD logs $pod 2>&1)
    
    # Extract node name
    node=$(echo "$logs" | grep "Current node:" | awk '{print $3}')
    
    if [ -z "$node" ]; then
        continue
    fi
    
    # Extract measurement results (lines with "->")
    echo "$logs" | grep -A 100 "=== Results ===" | grep "->" | while IFS= read -r line; do
        # Parse: anrg-X -> anrg-Y: bandwidth (latency: time)
        if echo "$line" | grep -q "->"; then
            from=$(echo "$line" | awk '{print $1}')
            to=$(echo "$line" | awk '{print $3}' | sed 's/:$//')
            rest=$(echo "$line" | cut -d: -f2-)
            
            # Extract bandwidth
            bandwidth=$(echo "$rest" | awk '{print $1}')
            
            # Extract latency
            latency=$(echo "$rest" | grep -oP 'latency: \K[0-9.]+' || echo "N/A")
            
            echo "$from|$to|$bandwidth|$latency" >> "$temp_file"
        fi
    done
done

# Get all unique nodes
all_nodes=$(for pod in $pods; do
    $KUBECTL_CMD get pod $pod -o jsonpath='{.spec.nodeName}' 2>/dev/null
done | sort -u)

if [ -z "$all_nodes" ]; then
    echo "No nodes found. Trying to get from cluster..."
    all_nodes=$($KUBECTL_CMD get nodes -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | sort -u)
fi

echo ""
echo "========================================="
echo "Full Measurement Matrix (Bandwidth in Mbps)"
echo "========================================="
echo ""

# Print header
printf "%-10s" "From\\To"
for to_node in $all_nodes; do
    printf "%-15s" "$to_node"
done
echo ""

# Print rows
for from_node in $all_nodes; do
    printf "%-10s" "$from_node"
    for to_node in $all_nodes; do
        if [ "$from_node" == "$to_node" ]; then
            printf "%-15s" "-"
        else
            result=$(grep "^${from_node}|${to_node}|" "$temp_file" | head -1 | cut -d'|' -f3)
            if [ -n "$result" ]; then
                printf "%-15s" "$result"
            else
                printf "%-15s" "N/A"
            fi
        fi
    done
    echo ""
done

echo ""
echo "========================================="
echo "Latency Matrix (seconds)"
echo "========================================="
echo ""

# Print latency header
printf "%-10s" "From\\To"
for to_node in $all_nodes; do
    printf "%-15s" "$to_node"
done
echo ""

# Print latency rows
for from_node in $all_nodes; do
    printf "%-10s" "$from_node"
    for to_node in $all_nodes; do
        if [ "$from_node" == "$to_node" ]; then
            printf "%-15s" "-"
        else
            latency=$(grep "^${from_node}|${to_node}|" "$temp_file" | head -1 | cut -d'|' -f4)
            if [ -n "$latency" ] && [ "$latency" != "N/A" ]; then
                printf "%-15s" "$latency"
            else
                printf "%-15s" "N/A"
            fi
        fi
    done
    echo ""
done

echo ""
echo "========================================="
echo "Detailed Results by Node"
echo "========================================="
echo ""

for pod in $pods; do
    node=$($KUBECTL_CMD get pod $pod -o jsonpath='{.spec.nodeName}' 2>/dev/null)
    if [ -n "$node" ]; then
        echo "--- $node ($pod) ---"
        $KUBECTL_CMD logs $pod 2>&1 | grep -A 20 "=== Results ===" | head -25
        echo ""
    fi
done
