#!/usr/bin/env bash
#
# Run the multi-ODAG benchmark with a specified scheduler.
#
# Usage:
#   ./run-benchmark.sh heft          # single HEFT run
#   ./run-benchmark.sh random 3      # 3 random runs
#
set -euo pipefail

SCHEDULER="${1:-heft}"
RUNS="${2:-1}"
DIR="$(cd "$(dirname "$0")" && pwd)"
EXAMPLE_DIR="$DIR/../../examples/multi-odag-heft"
NS="dsf-system"

NAMES=(video-transcode ml-training etl-wide sensor-fusion image-batch)
FILES=(
  odag-1-video-transcode.yml
  odag-2-ml-training.yml
  odag-3-etl-wide.yml
  odag-4-sensor-fusion.yml
  odag-5-image-batch.yml
)
DELAYS=(0 3 6 6 6)

echo "╔══════════════════════════════════════════════════════╗"
echo "║  Multi-ODAG Benchmark: scheduler=$SCHEDULER, runs=$RUNS"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

for run in $(seq 1 "$RUNS"); do
  echo "=== Run $run/$RUNS (scheduler: $SCHEDULER) ==="

  # Cleanup
  for name in "${NAMES[@]}"; do
    kubectl delete odag -n "$NS" "$name" --ignore-not-found 2>/dev/null
  done
  sleep 5

  # Submit with staggered delays
  elapsed=0
  for i in "${!FILES[@]}"; do
    delay=${DELAYS[$i]}
    if (( delay > 0 )); then
      sleep "$delay"
      elapsed=$((elapsed + delay))
    fi
    if [[ "$SCHEDULER" == "heft" ]]; then
      kubectl apply -f "$EXAMPLE_DIR/${FILES[$i]}" 2>&1 | sed "s/^/  [${elapsed}s] /"
    else
      sed "s/scheduler: heft/scheduler: $SCHEDULER/" "$EXAMPLE_DIR/${FILES[$i]}" \
        | kubectl apply -f - 2>&1 | sed "s/^/  [${elapsed}s] /"
    fi
  done

  echo "  Waiting for completion..."

  # Poll until all done or 5 min timeout
  timeout=300
  while (( timeout > 0 )); do
    sleep 10
    timeout=$((timeout - 10))
    total=$(kubectl get odags -n "$NS" --no-headers 2>/dev/null | wc -l)
    done_count=$(kubectl get odags -n "$NS" --no-headers 2>/dev/null | grep -cE "Succeeded|Failed" || true)
    if (( done_count == total && total >= 5 )); then
      break
    fi
  done

  # Results
  echo ""
  echo "  Results:"
  kubectl get odags -n "$NS" -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,SCHEDULER:.spec.scheduler,MAKESPAN:.status.makespan 2>&1 | sed 's/^/    /'
  echo ""
  echo "  Placements:"
  for name in "${NAMES[@]}"; do
    echo -n "    $name: "
    kubectl get odag -n "$NS" "$name" -o jsonpath='{range .status.tasks[*]}{.name}={.node} {end}' 2>&1
    echo ""
  done
  echo ""
done

echo "Benchmark complete."
