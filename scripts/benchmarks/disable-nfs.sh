#!/usr/bin/env bash
# Disable NFS-overlay benchmark mode.
#
# Deletes the overlay DaemonSet, unmounts /data/dsf-outputs on every node
# (the mount persists past pod deletion due to bidirectional propagation),
# then restarts data-agents so they observe local disk again.
#
# Leaves nfs-server and its manifests in place for fast re-enable.

set -euo pipefail
cd "$(dirname "$0")/../.."

echo "[nfs] Deleting nfs-overlay DaemonSet..."
kubectl delete ds -n dsf-system nfs-overlay --ignore-not-found

echo "[nfs] Unmounting NFS on every node..."
for node in $(kubectl get nodes -o name --no-headers | sed 's|node/||'); do
  kubectl run "nfs-umount-${node}" -n dsf-system --rm -i --restart=Never \
    --image=alpine:3.19 --overrides="{
      \"spec\": {
        \"nodeName\": \"$node\",
        \"containers\": [{
          \"name\": \"umount\",
          \"image\": \"alpine:3.19\",
          \"securityContext\": {\"privileged\": true},
          \"command\": [\"sh\", \"-c\", \"umount /host-root/data/dsf-outputs 2>/dev/null; echo done\"],
          \"volumeMounts\": [{\"name\": \"hr\", \"mountPath\": \"/host-root\", \"mountPropagation\": \"Bidirectional\"}]
        }],
        \"volumes\": [{\"name\": \"hr\", \"hostPath\": {\"path\": \"/\"}}]
      }
    }" 2>/dev/null || true
done

echo "[nfs] Restarting data-agents..."
kubectl rollout restart -n dsf-system ds/data-agent
kubectl rollout status -n dsf-system ds/data-agent --timeout=180s

echo "[nfs] Back to default: per-node hostPath + data-agent P2P push."
