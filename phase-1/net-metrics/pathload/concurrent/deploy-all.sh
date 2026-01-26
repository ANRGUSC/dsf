#!/bin/bash
set -e

# Deploy all concurrent pathload measurements

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBECTL_CMD="sudo k3s kubectl"

echo "========================================="
echo "Deploying Concurrent Pathload Measurements"
echo "Full Matrix: 6 measurement pairs"
echo "========================================="
echo ""

# Clean up any existing pods
echo "Cleaning up any existing pathload pods..."
$KUBECTL_CMD delete pod -l measurement --ignore-not-found=true --grace-period=0 --force 2>/dev/null || true
sleep 3

# Deploy all measurements concurrently
echo "Deploying all 6 measurement pairs..."
$KUBECTL_CMD apply -f "$SCRIPT_DIR/measure-3-to-4.yaml"
$KUBECTL_CMD apply -f "$SCRIPT_DIR/measure-3-to-5.yaml"
$KUBECTL_CMD apply -f "$SCRIPT_DIR/measure-4-to-3.yaml"
$KUBECTL_CMD apply -f "$SCRIPT_DIR/measure-4-to-5.yaml"
$KUBECTL_CMD apply -f "$SCRIPT_DIR/measure-5-to-3.yaml"
$KUBECTL_CMD apply -f "$SCRIPT_DIR/measure-5-to-4.yaml"

echo ""
echo "All measurements deployed. Waiting for completion..."
echo ""

# Wait for all pods to complete
while true; do
    running=$($KUBECTL_CMD get pods -l measurement -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}' 2>/dev/null | wc -w)
    if [ "$running" -eq 0 ]; then
        break
    fi
    echo "  Still running: $running pods"
    sleep 5
done

echo ""
echo "========================================="
echo "All Measurements Complete!"
echo "========================================="
echo ""
$KUBECTL_CMD get pods -l measurement -o wide
echo ""
echo "Collecting results..."
