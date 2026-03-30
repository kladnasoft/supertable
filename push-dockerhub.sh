#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Push script for Supertable Docker image
# Runs all tests before pushing to Docker Hub
# Usage:
#   ./docker-push.sh v1.0.3
#   ./docker-push.sh v1.0.3 --skip-tests   # Skip tests (not recommended)
# -----------------------------------------------------------------------------
IMAGE="${IMAGE:-kladnasoft/supertable}"
VERSION="${1:-}"
SKIP_TESTS="${2:-}"

GREEN="\033[0;32m"; YELLOW="\033[1;33m"; RED="\033[0;31m"; NC="\033[0m"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="${SCRIPT_DIR}/tests/docker"

if [[ -z "${VERSION}" ]]; then
  echo -e "${RED}❌ Usage:${NC} ./docker-push.sh <version> [--skip-tests]"
  echo "Example: ./docker-push.sh v1.0.3"
  exit 1
fi

echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}  Supertable Docker Build & Push${NC}"
echo -e "${YELLOW}  Image: ${IMAGE}:${VERSION}${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# -----------------------------------------------------------------------------
# Step 1: Ensure Docker Buildx is available
# -----------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}▶ Step 1: Ensuring Docker Buildx is available...${NC}"
docker buildx ls >/dev/null 2>&1 || docker buildx create --use >/dev/null
echo -e "${GREEN}✓ Buildx ready${NC}"

# -----------------------------------------------------------------------------
# Step 2: Build the image
# -----------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}▶ Step 2: Building image ${IMAGE}:${VERSION} (linux/amd64)...${NC}"
docker buildx build --platform linux/amd64 -t "${IMAGE}:${VERSION}" -f Dockerfile . --load
echo -e "${GREEN}✓ Image built successfully${NC}"

# -----------------------------------------------------------------------------
# Step 3: Run tests (unless --skip-tests)
# -----------------------------------------------------------------------------
if [[ "${SKIP_TESTS}" == "--skip-tests" ]]; then
  echo ""
  echo -e "${YELLOW}▶ Step 3: Skipping tests (--skip-tests flag)${NC}"
  echo -e "${YELLOW}⚠ Warning: Publishing without running tests${NC}"
else
  echo ""
  echo -e "${YELLOW}▶ Step 3: Running test suite...${NC}"

  # 3a. Entrypoint unit tests (pure shell, no Docker needed)
  if [[ -f "${TEST_DIR}/test_entrypoint.sh" ]]; then
    echo -e "${YELLOW}   → Running entrypoint.sh unit tests...${NC}"
    if sh "${TEST_DIR}/test_entrypoint.sh" "${SCRIPT_DIR}/entrypoint.sh"; then
      echo -e "${GREEN}   ✓ Entrypoint unit tests passed${NC}"
    else
      echo -e "${RED}❌ Entrypoint unit tests failed. Aborting push.${NC}"
      exit 1
    fi
  else
    echo -e "${YELLOW}   ⚠ test_entrypoint.sh not found, skipping${NC}"
  fi

  # 3b. Pytest integration tests
  if [[ -f "${TEST_DIR}/test_dockerfile.py" ]]; then
    echo -e "${YELLOW}   → Running pytest integration tests...${NC}"
    if pytest "${TEST_DIR}/test_dockerfile.py" -v --image "${IMAGE}:${VERSION}"; then
      echo -e "${GREEN}   ✓ Pytest integration tests passed${NC}"
    else
      echo -e "${RED}❌ Pytest integration tests failed. Aborting push.${NC}"
      exit 1
    fi
  else
    echo -e "${YELLOW}   ⚠ test_dockerfile.py not found, skipping${NC}"
  fi

  echo -e "${GREEN}✓ All tests passed${NC}"
fi

# -----------------------------------------------------------------------------
# Step 4: Tag as latest
# -----------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}▶ Step 4: Tagging image as latest...${NC}"
docker tag "${IMAGE}:${VERSION}" "${IMAGE}:latest"
echo -e "${GREEN}✓ Tagged ${IMAGE}:latest${NC}"

# -----------------------------------------------------------------------------
# Step 5: Login to Docker Hub
# -----------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}▶ Step 5: Logging into Docker Hub (if needed)...${NC}"
if ! docker info 2>/dev/null | grep -q "Username:"; then
  docker login
fi
echo -e "${GREEN}✓ Logged in${NC}"

# -----------------------------------------------------------------------------
# Step 6: Push to Docker Hub
# -----------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}▶ Step 6: Pushing to Docker Hub...${NC}"

echo -e "${YELLOW}   → Pushing ${IMAGE}:${VERSION}${NC}"
docker push "${IMAGE}:${VERSION}"

echo -e "${YELLOW}   → Pushing ${IMAGE}:latest${NC}"
docker push "${IMAGE}:latest"

# -----------------------------------------------------------------------------
# Done
# -----------------------------------------------------------------------------
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  ✅ Successfully pushed:${NC}"
echo -e "${GREEN}     ${IMAGE}:${VERSION}${NC}"
echo -e "${GREEN}     ${IMAGE}:latest${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"