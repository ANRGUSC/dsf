#!/bin/bash
# Run from a host where kubectl exec works. If you get 502 Bad Gateway, the API
# server cannot reach the kubelet on the feeder's node. Fix: run this from the
# control-plane node, or open firewall so the API server can reach all nodes:10250.
# Waits for feeder pod to be Ready, then runs 2 auto requests.
set -e
echo "Waiting for feeder pod gpt2-ctg-feeder-0 to be Ready..."
kubectl wait --for=condition=Ready pod/gpt2-ctg-feeder-0 --timeout=120s 2>/dev/null || true
echo "Running feeder test (2 requests)..."
kubectl exec gpt2-ctg-feeder-0 -- python feeder.py --warmup 30 --auto 2
echo "Test done."
