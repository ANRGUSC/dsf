#!/usr/bin/env bash
# Enable NFS-overlay benchmark mode for ODAGs.
#
# Mounts nfs-server:/data/nfs-export over /data/dsf-outputs on every worker
# node. After this, ODAG task pods' hostPath writes go to shared NFS; the
# data-agent push becomes a redundant HTTP transfer on top of NFS delivery.
#
# Idempotent. Run disable-nfs.sh to revert.

set -euo pipefail
cd "$(dirname "$0")/../.."

echo "[nfs] Ensuring nfs-server is running..."
kubectl apply -f eval/scalability/nfs-server.yml >/dev/null

echo "[nfs] Applying nfs-overlay DaemonSet (privileged, hostPath=/)..."
kubectl apply -f eval/scalability/nfs-overlay-daemonset.yml

echo "[nfs] Waiting for overlay pods to mount..."
kubectl rollout status -n dsf-system ds/nfs-overlay --timeout=120s

echo "[nfs] Overlay active. /data/dsf-outputs on every worker is now NFS."
echo "[nfs] Run disable-nfs.sh to revert."
