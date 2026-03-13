#!/bin/bash
# Display link scores as an N×N matrix

PROM_URL="${1:-http://localhost:9999}"

echo "=== Fetching full link score matrix ==="
echo ""

# Get full matrix in JSON (stderr has logs, stdout has JSON)
JSON=$(./link-scorer -prom-url "$PROM_URL" -topk 0 2>/dev/null)

if [ $? -ne 0 ]; then
    echo "Error: Failed to query Prometheus at $PROM_URL"
    exit 1
fi

# Extract nodes and create matrix
echo "$JSON" | python3 << 'PYEOF'
import json
import sys

data = json.load(sys.stdin)
nodes = sorted(data['nodes'].keys())
edges = {f"{e['Src']}->{e['Dst']}": e['Score'] for e in data['edges']}

# Print header
print(f"{'FROM/TO':<12}", end="")
for dst in nodes:
    print(f"{dst:>12}", end="")
print()

# Print separator
print("-" * (12 * (len(nodes) + 1)))

# Print matrix
for src in nodes:
    print(f"{src:<12}", end="")
    for dst in nodes:
        if src == dst:
            print(f"{'--':>12}", end="")
        else:
            key = f"{src}->{dst}"
            score = edges.get(key, 0)
            # Format as Mbps for readability
            mbps = score / 1e6
            print(f"{mbps:>11.0f}M", end="")
    print()

print()
print("Values shown in Mbps (M = million bits/second)")
print(f"Window: {data['window']}")
PYEOF

echo ""
echo "=== Node Summary ==="
echo "$JSON" | python3 << 'PYEOF'
import json
import sys

data = json.load(sys.stdin)
nodes = sorted(data['nodes'].keys())

print(f"{'Node':<10} {'Egress':>10} {'Ingress':>10} {'Drops':>10} {'Penalty':>8}")
print("-" * 50)
for node in nodes:
    m = data['nodes'][node]
    print(f"{node:<10} {m['EgressBps']/1e6:>9.0f}M {m['IngressBps']/1e6:>9.0f}M {m['DropBps']/1e6:>9.0f}M {m['Penalty']:>7.2f}")
PYEOF
