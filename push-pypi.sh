#!/usr/bin/env bash
# Prepare an immutable SuperTable release tag.
#
# Production artifacts are built, tested, and published by
# .github/workflows/release.yml from the pushed tag.  This script never edits
# version files, builds from an uncommitted tree, accepts a test bypass, or
# uploads with a long-lived API token.
#
# Usage:
#   ./push-pypi.sh                 # validate the committed release source
#   ./push-pypi.sh --create-tag    # create a signed annotated vX.Y.Z tag
#   ./push-pypi.sh --push          # validate and push an existing tag
#   ./push-pypi.sh --create-tag --push

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

CREATE_TAG=0
PUSH_TAG=0
for arg in "$@"; do
  case "${arg}" in
    --create-tag) CREATE_TAG=1 ;;
    --push) PUSH_TAG=1 ;;
    --help|-h)
      sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    --no-tests|--testpypi|patch|minor|major|[0-9]*.[0-9]*.[0-9]*)
      echo "ERROR: versions must be edited and committed before release; tests cannot be skipped." >&2
      exit 2
      ;;
    *)
      echo "ERROR: unknown argument: ${arg}" >&2
      exit 2
      ;;
  esac
done

command -v git >/dev/null || { echo "ERROR: git is required." >&2; exit 3; }
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
  echo "ERROR: releases must be prepared from a Git checkout." >&2
  exit 3
}

if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
  echo "ERROR: release checkout is not clean. Commit the version and all release changes first." >&2
  git status --short >&2
  exit 4
fi

VERSION="$(python - <<'PY'
import pathlib
import re

text = pathlib.Path("pyproject.toml").read_text(encoding="utf-8")
project = re.search(r"(?ms)^\[project\]\s*$\n(.*?)(?=^\[|\Z)", text)
version = (
    re.search(r'(?m)^version\s*=\s*["\']([^"\']+)["\']\s*(?:#.*)?$', project.group(1))
    if project
    else None
)
if version is None:
    raise SystemExit("ERROR: pyproject.toml has no literal [project].version")
print(version.group(1))
PY
)"
if ! [[ "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "ERROR: project.version is not a release SemVer: ${VERSION}" >&2
  exit 5
fi
TAG="v${VERSION}"

python - "${VERSION}" <<'PY'
import ast
import pathlib
import re
import sys

expected = sys.argv[1]

init_tree = ast.parse(pathlib.Path("supertable/__init__.py").read_text(encoding="utf-8"))
init_version = next(
    node.value.value
    for node in init_tree.body
    if isinstance(node, ast.Assign)
    for target in node.targets
    if isinstance(target, ast.Name)
    and target.id == "__version__"
    and isinstance(node.value, ast.Constant)
)
setup_text = pathlib.Path("setup.py").read_text(encoding="utf-8")
match = re.search(r"\bversion\s*=\s*['\"]([^'\"]+)['\"]", setup_text)
setup_version = match.group(1) if match else None
if init_version != expected or setup_version != expected:
    raise SystemExit(
        "ERROR: version mismatch: "
        f"pyproject={expected}, __init__={init_version}, setup.py={setup_version}"
    )
PY

HEAD_COMMIT="$(git rev-parse HEAD)"
if [[ "${PUSH_TAG}" -eq 1 && "${CREATE_TAG}" -eq 0 ]] \
  && ! git rev-parse -q --verify "refs/tags/${TAG}^{commit}" >/dev/null; then
  echo "ERROR: --push requires the existing signed local tag ${TAG}; use --create-tag first." >&2
  exit 6
fi
if git rev-parse -q --verify "refs/tags/${TAG}^{commit}" >/dev/null; then
  TAG_COMMIT="$(git rev-parse "refs/tags/${TAG}^{commit}")"
  if [[ "${TAG_COMMIT}" != "${HEAD_COMMIT}" ]]; then
    echo "ERROR: ${TAG} already identifies ${TAG_COMMIT}, not HEAD ${HEAD_COMMIT}." >&2
    exit 6
  fi
  if ! git verify-tag "${TAG}" >/dev/null 2>&1; then
    echo "ERROR: existing release tag ${TAG} is not a verifiable signed annotated tag." >&2
    exit 6
  fi
elif [[ "${CREATE_TAG}" -eq 0 ]]; then
  echo "Validated committed version ${VERSION} at ${HEAD_COMMIT}."
  echo "Next: rerun with --create-tag after reviewing the mandatory local gates."
fi

echo "==> Running the complete test suite"
python -m pytest -q

echo "==> Running release lint and type gates"
python -m ruff check supertable tests check_mypy_baseline.py
python check_mypy_baseline.py supertable

echo "==> Running source security gate"
python -m bandit -q -r supertable -lll
python -m pip_audit -r requirements.txt --ignore-vuln GHSA-rgxp-2hwp-jwgg

if ! git rev-parse -q --verify "refs/tags/${TAG}^{commit}" >/dev/null; then
  if [[ "${CREATE_TAG}" -eq 1 ]]; then
    echo "==> Creating signed immutable source tag ${TAG}"
    git tag --sign --annotate "${TAG}" --message "SuperTable ${VERSION}"
    git verify-tag "${TAG}"
  else
    echo "All gates passed; no tag was created."
    exit 0
  fi
fi

if [[ "${PUSH_TAG}" -eq 1 ]]; then
  echo "==> Pushing ${TAG}; protected CI will build and publish this exact source"
  git push origin "refs/tags/${TAG}"
else
  echo "Tag ${TAG} is ready. Review it, then run: $0 --push"
fi
