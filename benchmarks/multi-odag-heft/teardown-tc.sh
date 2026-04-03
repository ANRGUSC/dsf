#!/usr/bin/env bash
#
# Remove tc bandwidth shaping and restore native link speeds.
#
set -euo pipefail

echo "Removing tc rules on all nodes..."

for node in anrg-3 anrg-4 anrg-5 anrg-6; do
  kubectl delete pod -n dsf-system "tc-setup-${node}" --ignore-not-found 2>/dev/null || true
done

# The tc pods applied rules on the host kernel — deleting the pod doesn't remove them.
# We need to clear the qdisc on each node.
for entry in "anrg-3 192.168.1.164" "anrg-4 192.168.1.156" "anrg-5 192.168.1.154" "anrg-6 192.168.1.208"; do
  node=$(echo "$entry" | awk '{print $1}')
  ip=$(echo "$entry" | awk '{print $2}')
  echo "  clearing tc on $node..."
  cat <<EOF | kubectl apply -f - 2>&1 | grep -v unchanged
apiVersion: v1
kind: Pod
metadata:
  name: tc-clear-${node}
  namespace: dsf-system
spec:
  nodeName: ${node}
  hostNetwork: true
  restartPolicy: Never
  containers:
  - name: tc
    image: alpine
    securityContext:
      privileged: true
    command: ["sh", "-c"]
    args:
    - |
      apk add -q iproute2
      IFACE=\$(ip -o addr show | grep '${ip}' | awk '{print \$2}')
      tc qdisc del dev \$IFACE root 2>/dev/null || true
      echo "${node}: tc rules cleared"
EOF
done

sleep 10

# Clean up clear pods
for node in anrg-3 anrg-4 anrg-5 anrg-6; do
  kubectl delete pod -n dsf-system "tc-clear-${node}" --ignore-not-found 2>/dev/null || true
done

echo ""
echo "All tc rules removed. Native link speeds restored."
