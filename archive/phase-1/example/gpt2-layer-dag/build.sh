#!/bin/bash
# Build the single image and tag it for each DAG-node role.
# Run from the directory that contains both dsf/ and gpt2-dag/ (e.g. ~/).
set -euo pipefail

REPO="gpt2-single-layer"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Infer build context: two levels above dsf/phase-1/example/gpt2-layer-dag
BUILD_CTX="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

echo "=== Building image from $BUILD_CTX ==="
docker build \
  -f "$SCRIPT_DIR/Dockerfile" \
  -t "${REPO}:base" \
  "$BUILD_CTX"

# Tag for each role type used in dag.yml
for tag in qkv attn-shard attn-merge mlp-shard mlp-merge; do
  docker tag "${REPO}:base" "${REPO}:${tag}"
  echo "  tagged ${REPO}:${tag}"
done

echo ""
echo "=== Done.  Images: ==="
docker images --filter "reference=${REPO}" --format "  {{.Repository}}:{{.Tag}}"

# If using Kind, load them:
if command -v kind &>/dev/null; then
  echo ""
  echo "Loading into Kind cluster..."
  for tag in qkv attn-shard attn-merge mlp-shard mlp-merge; do
    kind load docker-image "${REPO}:${tag}" 2>/dev/null && echo "  loaded ${REPO}:${tag}" || true
  done
fi
