#!/usr/bin/env bash
# run_tests.sh — Build and test the Supertable Docker image
# Run from: tests/docker/
# Usage: ./run_tests.sh [image_tag]

set -euo pipefail

IMAGE="${1:-supertable:test}"
PROJECT_ROOT="$(cd ../.. && pwd)"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Supertable Docker Test Suite"
echo "  Image: $IMAGE"
echo "  Project root: $PROJECT_ROOT"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ---------------------------------------------------------------------------
# 1. Build the image
# ---------------------------------------------------------------------------
echo ""
echo "▶ Step 1: Building Docker image..."
docker build -t "$IMAGE" "$PROJECT_ROOT"
echo "✓ Image built successfully"

# ---------------------------------------------------------------------------
# 2. Run entrypoint unit tests (no Docker needed, pure shell)
# ---------------------------------------------------------------------------
echo ""
echo "▶ Step 2: Running entrypoint.sh unit tests..."
if [ -f "./test_entrypoint.sh" ]; then
    sh ./test_entrypoint.sh "$PROJECT_ROOT/entrypoint.sh"
    echo "✓ Entrypoint unit tests passed"
else
    echo "⚠ test_entrypoint.sh not found, skipping"
fi

# ---------------------------------------------------------------------------
# 3. Run pytest integration tests
# ---------------------------------------------------------------------------
echo ""
echo "▶ Step 3: Running pytest integration tests..."
pytest test_dockerfile.py -v --image "$IMAGE"
echo "✓ Pytest integration tests passed"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✓ All tests passed!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"