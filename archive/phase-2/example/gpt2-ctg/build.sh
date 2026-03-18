#!/bin/bash
# Build and push images for the GPT-2 full 12-layer CTG example.
# Run from the directory containing both dsf/ and gpt2-dag/ (e.g. ~/).
set -euo pipefail

REPO="mohammadalikh/gpt2-ctg"
VERSION="${1:-v1}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_CTX="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

IMAGE="${REPO}:${VERSION}"

echo "=== Building $IMAGE from $BUILD_CTX ==="
docker build \
  -f "$SCRIPT_DIR/Dockerfile" \
  -t "$IMAGE" \
  "$BUILD_CTX"

echo ""
echo "=== Pushing $IMAGE ==="
docker push "$IMAGE"

echo ""
echo "=== Done: $IMAGE ==="
