#!/usr/bin/env bash
#
# Apply tc bandwidth shaping to create a heterogeneous network.
#
# Bandwidth matrix:
#   anrg-3 ↔ anrg-4: 1 Gbps  (fast pair)
#   anrg-3 ↔ anrg-5: 500 Mbps (medium)
#   anrg-4 ↔ anrg-5: 500 Mbps (medium)
#   *      ↔ anrg-6: 100 Mbps (slow node)
#
set -euo pipefail

echo "Cleaning up old tc pods..."
kubectl delete pods -n dsf-system -l app=tc-setup --ignore-not-found 2>/dev/null || true
sleep 2

# Node IPs:
# anrg-3: 192.168.1.164
# anrg-4: 192.168.1.156
# anrg-5: 192.168.1.154
# anrg-6: 192.168.1.208

apply_tc() {
  local NODE=$1 IP=$2 RULES=$3
  cat <<EOF | kubectl apply -f - 2>&1
apiVersion: v1
kind: Pod
metadata:
  name: tc-setup-${NODE}
  namespace: dsf-system
  labels:
    app: tc-setup
spec:
  nodeName: ${NODE}
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
      IFACE=\$(ip -o addr show | grep '${IP}' | awk '{print \$2}')
      tc qdisc del dev \$IFACE root 2>/dev/null || true
      ${RULES}
      echo "${NODE}: tc rules applied"
      tc class show dev \$IFACE
      sleep 86400
EOF
}

echo "Applying tc rules..."

# anrg-3: to anrg-4=1Gbps (default), to anrg-5=500Mbps, to anrg-6=100Mbps
apply_tc anrg-3 192.168.1.164 '
tc qdisc add dev $IFACE root handle 1: htb default 10
tc class add dev $IFACE parent 1: classid 1:10 htb rate 1gbit ceil 1gbit
tc class add dev $IFACE parent 1: classid 1:20 htb rate 500mbit ceil 500mbit
tc class add dev $IFACE parent 1: classid 1:30 htb rate 100mbit ceil 100mbit
tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 match ip dst 192.168.1.154/32 flowid 1:20
tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 match ip dst 192.168.1.208/32 flowid 1:30'

# anrg-4: to anrg-3=1Gbps (default), to anrg-5=500Mbps, to anrg-6=100Mbps
apply_tc anrg-4 192.168.1.156 '
tc qdisc add dev $IFACE root handle 1: htb default 10
tc class add dev $IFACE parent 1: classid 1:10 htb rate 1gbit ceil 1gbit
tc class add dev $IFACE parent 1: classid 1:20 htb rate 500mbit ceil 500mbit
tc class add dev $IFACE parent 1: classid 1:30 htb rate 100mbit ceil 100mbit
tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 match ip dst 192.168.1.154/32 flowid 1:20
tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 match ip dst 192.168.1.208/32 flowid 1:30'

# anrg-5: to anrg-3=500Mbps, to anrg-4=1Gbps (default), to anrg-6=100Mbps
apply_tc anrg-5 192.168.1.154 '
tc qdisc add dev $IFACE root handle 1: htb default 10
tc class add dev $IFACE parent 1: classid 1:10 htb rate 1gbit ceil 1gbit
tc class add dev $IFACE parent 1: classid 1:20 htb rate 500mbit ceil 500mbit
tc class add dev $IFACE parent 1: classid 1:30 htb rate 100mbit ceil 100mbit
tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 match ip dst 192.168.1.164/32 flowid 1:20
tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 match ip dst 192.168.1.208/32 flowid 1:30'

# anrg-6: all outbound = 100Mbps
apply_tc anrg-6 192.168.1.208 '
tc qdisc add dev $IFACE root handle 1: htb default 10
tc class add dev $IFACE parent 1: classid 1:10 htb rate 100mbit ceil 100mbit'

echo ""
echo "Waiting for tc pods to initialize..."
sleep 10

echo ""
echo "=== Verification ==="
for pod in tc-setup-anrg3 tc-setup-anrg4 tc-setup-anrg5 tc-setup-anrg6; do
  echo "--- $pod ---"
  kubectl logs -n dsf-system "$pod" 2>&1 | grep -E "applied|rate" | head -5
done
echo ""
echo "Bandwidth shaping active."
