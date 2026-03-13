#!/bin/bash
# Quick test command for link-scorer

cd /home/anrg/dsf/phase-1/net-metrics/link-scorer

# Start mock Prometheus
python3 /tmp/mock_prom.py 9999 > /dev/null 2>&1 &
MOCK_PID=$!
sleep 2

# Run test
echo "Testing link-scorer..."
./link-scorer -prom-url http://localhost:9999 -topk 5

# Cleanup
kill $MOCK_PID 2>/dev/null
