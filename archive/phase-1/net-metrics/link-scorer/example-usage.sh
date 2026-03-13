#!/bin/bash
# Example usage of link-scorer

set -e

# Example 1: Basic usage with local Prometheus
echo "=== Example 1: Basic usage ==="
./link-scorer \
  -prom-url http://localhost:9090 \
  -window 30s \
  -topk 10

# Example 2: CSV output
echo ""
echo "=== Example 2: CSV output ==="
./link-scorer \
  -prom-url http://localhost:9090 \
  -output csv \
  -topk 20

# Example 3: Custom parameters
echo ""
echo "=== Example 3: Custom parameters ==="
./link-scorer \
  -prom-url http://localhost:9090 \
  -window 60s \
  -capacity 2e9 \
  -drop-ref 5e6 \
  -topk 0

# Example 4: Specific nodes
echo ""
echo "=== Example 4: Specific nodes ==="
./link-scorer \
  -prom-url http://localhost:9090 \
  -nodes "anrg-1,anrg-2,anrg-3" \
  -topk 5

# Example 5: With authentication
echo ""
echo "=== Example 5: With authentication ==="
export PROM_TOKEN="your-bearer-token-here"
./link-scorer \
  -prom-url https://prometheus.example.com:9090 \
  -topk 10
