#!/usr/bin/env bash
# Report which benchmark modes (NFS overlay, MQTT broker) are currently active.

set -euo pipefail

echo "=== Default transports ==="
echo "ODAG default: per-node hostPath + data-agent P2P push (DSF_TRANSPORT_PATTERN=file)"
echo "CDAG default: ZeroMQ PUB/SUB direct (DSF_TRANSPORT_PATTERN=pubsub)"
echo

echo "=== NFS overlay (ODAG benchmark mode) ==="
if kubectl get ds -n dsf-system nfs-overlay >/dev/null 2>&1; then
  READY=$(kubectl get ds -n dsf-system nfs-overlay -o jsonpath='{.status.numberReady}')
  DESIRED=$(kubectl get ds -n dsf-system nfs-overlay -o jsonpath='{.status.desiredNumberScheduled}')
  echo "  ACTIVE — $READY/$DESIRED nodes mounting NFS over /data/dsf-outputs"
  echo "  ⚠  ODAG tasks are currently using shared NFS, not P2P."
  echo "  Run scripts/benchmarks/disable-nfs.sh to revert."
else
  echo "  disabled"
fi
echo

echo "=== MQTT broker ==="
if kubectl get deploy -n dsf-system mqtt-broker >/dev/null 2>&1; then
  READY=$(kubectl get deploy -n dsf-system mqtt-broker -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  echo "  broker running (readyReplicas=$READY)"
  echo "  tasks only use it if their spec sets DSF_TRANSPORT_PATTERN=mqtt"
else
  echo "  broker not deployed"
fi
