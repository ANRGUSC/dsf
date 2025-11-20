#!/bin/bash

# Script to limit network bandwidth to 100 Mbps on all cluster nodes
# This simulates realistic network conditions for scheduler testing

BANDWIDTH="100mbit"

echo "Limiting network bandwidth to 100 Mbps on all nodes..."

# Get all nodes
NODES=$(/usr/local/bin/k3s kubectl get nodes -o jsonpath='{.items[*].metadata.name}')

for NODE in $NODES; do
    echo ""
    echo "Configuring node: $NODE"
    
    if [ "$NODE" = "$(hostname)" ]; then
        # Local node - apply directly
        echo "  Applying bandwidth limit locally..."
        
        # Find the primary network interface (skip lo)
        IFACE=$(ip route | grep default | awk '{print $5}' | head -1)
        
        if [ -z "$IFACE" ]; then
            echo "  WARNING: Could not find network interface"
            continue
        fi
        
        echo "  Interface: $IFACE"
        
        # Remove existing qdisc if any
        sudo tc qdisc del dev $IFACE root 2>/dev/null || true
        
        # Add bandwidth limit
        sudo tc qdisc add dev $IFACE root tbf rate $BANDWIDTH burst 32kbit latency 400ms
        
        echo "  ✓ Bandwidth limited to $BANDWIDTH on $IFACE"
        
    else
        # Remote node - need SSH access (if configured)
        echo "  Note: For remote nodes, apply manually with:"
        echo "  ssh $NODE 'sudo tc qdisc add dev <interface> root tbf rate $BANDWIDTH burst 32kbit latency 400ms'"
    fi
done

echo ""
echo "Bandwidth limiting complete!"
echo "To verify: tc qdisc show"
echo "To remove limits: sudo tc qdisc del dev <interface> root"

