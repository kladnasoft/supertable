#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Push script for Auth Bridge Docker image
# Usage:
#   ./docker-push.sh v1.0.3
# -----------------------------------------------------------------------------
IMAGE="${IMAGE:-kladnasoft/supertable}"
VERSION="${1:-}"

GREEN="\033[0;32m"; YELLOW="\033[1;33m"; RED="\033[0;31m"; NC="\033[0m"

if [[ -z "${VERSION}" ]]; then
  echo -e "${RED}❌ Usage:${NC} ./docker-push.sh <version>"
  echo "Example: ./docker-push.sh v1.0.3"
  exit 1
fi

echo -e "${YELLOW}==> Ensuring Docker Buildx is available...${NC}"
docker buildx ls >/dev/null 2>&1 || docker buildx create --use >/dev/null

echo -e "${YELLOW}==> Building image ${IMAGE}:${VERSION} (linux/amd64)...${NC}"
docker buildx build --platform linux/amd64 -t "${IMAGE}:${VERSION}" -f Dockerfile . --load

echo -e "${YELLOW}==> Tagging image as latest...${NC}"
docker tag "${IMAGE}:${VERSION}" "${IMAGE}:latest"

echo -e "${YELLOW}==> Logging into Docker Hub (if needed)...${NC}"
if ! docker info 2>/dev/null | grep -q "Username:"; then
  docker login
fi

echo -e "${YELLOW}==> Pushing versioned tag: ${IMAGE}:${VERSION}${NC}"
docker push "${IMAGE}:${VERSION}"

echo -e "${YELLOW}==> Pushing latest tag: ${IMAGE}:latest${NC}"
docker push "${IMAGE}:latest"

echo -e "${GREEN}✅ Successfully pushed:${NC}"
echo -e "   ${GREEN}${IMAGE}:${VERSION}${NC}"
echo -e "   ${GREEN}${IMAGE}:latest${NC}"
