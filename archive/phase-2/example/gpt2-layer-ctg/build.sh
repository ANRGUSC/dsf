#!/bin/bash
# Build and push images for the GPT-2 CTG single-layer example.
# Run from the directory containing both dsf/ and gpt2-dag/ (e.g. ~/).
set -euo pipefail

REPO="mohammadalikh/gpt2-single-layer-ctg"
VERSION="${1:-v1}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_CTX="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

echo "=== Building base image from $BUILD_CTX ==="
docker build \
  -f "$SCRIPT_DIR/Dockerfile" \
  -t "${REPO}:base-${VERSION}" \
  "$BUILD_CTX"

TAGS=(qkv attn-shard attn-merge mlp-shard mlp-merge feeder)

echo ""
echo "=== Tagging and pushing ==="
for tag in "${TAGS[@]}"; do
  full="${REPO}:${tag}-${VERSION}"
  docker tag "${REPO}:base-${VERSION}" "$full"
  echo "  pushing $full"
  docker push "$full"
done

echo ""
echo "=== Done. Images pushed: ==="
for tag in "${TAGS[@]}"; do
  echo "  ${REPO}:${tag}-${VERSION}"
done
