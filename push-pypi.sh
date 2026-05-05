#!/usr/bin/env bash
# publish.sh — bump version, build, test, and upload to (Test)PyPI
# Usage:
#   ./publish.sh                 # auto-bump patch from current pyproject.toml version
#   ./publish.sh 1.2.3           # set explicit version, uploads to PyPI
#   ./publish.sh patch|minor|major  # bump that segment from current version
#   ./publish.sh --testpypi      # upload to TestPyPI (default bump = patch)
#   ./publish.sh 1.2.3 --no-tests
# Env:
#   PYPI_TOKEN   (required for upload)
#   NO_GIT=1     (optional: skip git tag/push)
#   SKIP_TESTS=1 (optional: skip BOTH the unit-test run and local install tests)

set -euo pipefail

# ---- locate script + project dir ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# ---- helper: read current version from pyproject.toml ----
current_version() {
  grep -E '^version\s*=' pyproject.toml | head -1 | sed -E 's/.*"([^"]+)".*/\1/'
}

# ---- helper: bump a SemVer X.Y.Z by segment ----
bump_version() {
  local cur="$1" segment="$2"
  if ! [[ "${cur}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    echo "ERROR: Cannot bump non-SemVer version '${cur}'." >&2
    exit 6
  fi
  local maj="${BASH_REMATCH[1]}" min="${BASH_REMATCH[2]}" pat="${BASH_REMATCH[3]}"
  case "${segment}" in
    major) echo "$((maj + 1)).0.0" ;;
    minor) echo "${maj}.$((min + 1)).0" ;;
    patch|"") echo "${maj}.${min}.$((pat + 1))" ;;
    *) echo "ERROR: Unknown bump segment '${segment}'." >&2; exit 6 ;;
  esac
}

# ---- parse args ----
[[ -f "pyproject.toml" ]] || { echo "ERROR: pyproject.toml not found in ${SCRIPT_DIR}."; exit 3; }
CURRENT_VERSION="$(current_version)"
VERSION=""              # resolved below
REPO="pypi"             # or "testpypi"
NO_TESTS_FLAG=0
arg_version_set=0

for arg in "$@"; do
  case "$arg" in
    --testpypi) REPO="testpypi" ;;
    --no-tests) NO_TESTS_FLAG=1 ;;
    --help|-h)
      sed -n '1,80p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    major|minor|patch)
      if [[ $arg_version_set -eq 0 ]]; then
        VERSION="$(bump_version "${CURRENT_VERSION}" "${arg}")"
        arg_version_set=1
      else
        echo "Unknown argument: $arg" >&2; exit 1
      fi
      ;;
    *)
      if [[ $arg_version_set -eq 0 ]]; then
        VERSION="$arg"
        arg_version_set=1
      else
        echo "Unknown argument: $arg" >&2
        exit 1
      fi
      ;;
  esac
done

# Default behaviour: auto-bump the patch segment so each invocation
# produces a new release without requiring an explicit number.
if [[ -z "${VERSION}" ]]; then
  VERSION="$(bump_version "${CURRENT_VERSION}" patch)"
  echo "==> No version specified — auto-bumping patch: ${CURRENT_VERSION} -> ${VERSION}"
fi

echo "==> Current version: ${CURRENT_VERSION}"
echo "==> Target  version: ${VERSION}"
echo "==> Target repository: ${REPO}"

if [[ "${VERSION}" == "${CURRENT_VERSION}" ]]; then
  echo "ERROR: Target version equals current version (${VERSION}). Aborting." >&2
  exit 7
fi

# ---- load TOKEN file if present ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "${PYPI_TOKEN:-}" && -f "${SCRIPT_DIR}/TOKEN" ]]; then
  echo "==> Loading PYPI_TOKEN from TOKEN file"
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/TOKEN"
fi

# ---- require PYPI_TOKEN for upload ----
if [[ -z "${PYPI_TOKEN:-}" ]]; then
  echo "ERROR: PYPI_TOKEN is not set in the environment." >&2
  echo "Export it first, or create a TOKEN file next to this script containing:" >&2
  echo "  export PYPI_TOKEN='pypi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'" >&2
  exit 2
fi

# ---- run full test suite BEFORE bumping anything ----
# Honours --no-tests / SKIP_TESTS=1 (same flags as the post-build install
# smoke test). Aborts the publish on any failure so we never ship a broken
# release.
if [[ "${SKIP_TESTS:-0}" == "1" || "${NO_TESTS_FLAG}" == "1" ]]; then
  echo "==> Tests disabled (SKIP_TESTS/--no-tests). Skipping pytest run."
else
  echo "==> Running unit test suite (pytest supertable/)"
  if ! python -m pytest supertable/ -q --no-header; then
    echo "ERROR: Tests failed. Aborting publish." >&2
    exit 8
  fi
fi

# ---- bump versions in pyproject.toml, setup.py, and supertable/__init__.py ----
[[ -f "setup.py" ]] || echo "WARN: setup.py not found; continuing."
[[ -f "supertable/__init__.py" ]] || { echo "ERROR: supertable/__init__.py not found."; exit 3; }

echo "==> Bumping version in pyproject.toml to ${VERSION}"
sed -i -E "0,/^version\s*=\s*\"[^\"]+\"/s//version = \"${VERSION}\"/" pyproject.toml

if [[ -f "setup.py" ]]; then
  echo "==> Bumping version in setup.py to ${VERSION}"
  sed -i -E "s/version\s*=\s*['\"][^'\"]+['\"]/version=\"${VERSION}\"/" setup.py
fi

echo "==> Bumping __version__ in supertable/__init__.py to ${VERSION}"
sed -i -E "s/^__version__\s*=\s*['\"][^'\"]+['\"]/__version__ = \"${VERSION}\"/" supertable/__init__.py

# Verify the three sources agree
echo "==> Verifying version consistency"
PY_VER=$(grep -E '^version\s*=' pyproject.toml | head -1 | sed -E 's/.*"([^"]+)".*/\1/')
INIT_VER=$(grep -E '^__version__\s*=' supertable/__init__.py | head -1 | sed -E "s/.*['\"]([^'\"]+)['\"].*/\1/")
SETUP_VER=""
if [[ -f "setup.py" ]]; then
  SETUP_VER=$(grep -E "version\s*=" setup.py | head -1 | sed -E "s/.*['\"]([^'\"]+)['\"].*/\1/")
fi
echo "    pyproject.toml         = ${PY_VER}"
echo "    supertable/__init__.py = ${INIT_VER}"
[[ -n "${SETUP_VER}" ]] && echo "    setup.py               = ${SETUP_VER}"
if [[ "${PY_VER}" != "${VERSION}" || "${INIT_VER}" != "${VERSION}" ]]; then
  echo "ERROR: Version files disagree. Aborting." >&2
  exit 5
fi
if [[ -n "${SETUP_VER}" && "${SETUP_VER}" != "${VERSION}" ]]; then
  echo "ERROR: setup.py version disagrees. Aborting." >&2
  exit 5
fi

# ---- clean build artifacts ----
echo "==> Cleaning build artifacts"
rm -rf build dist *.egg-info

# ---- build sdist + wheel ----
echo "==> Ensuring build tooling is up to date (quiet)"
# Try to bootstrap pip only if ensurepip exists; suppress any stderr
python - <<'PY' 2>/dev/null || true
import importlib.util
if importlib.util.find_spec("ensurepip"):
    import ensurepip
    try:
        ensurepip.bootstrap()
    except Exception:
        pass
PY
python -m pip install --upgrade pip >/dev/null 2>&1 || true
python -m pip install --upgrade build twine >/dev/null

echo "==> Building distributions"
python -m build

# ---- sanity-check metadata ----
echo "==> Checking built artifacts with twine"
twine check dist/*

# ---- helper: create a temp venv robustly ----
create_temp_env() {
  local envdir="$1"
  echo "==> Creating temp environment: ${envdir}"

  if python -m venv "${envdir}" 2>/dev/null; then
    echo "==> Created venv via python -m venv"
    return 0
  fi

  echo "WARN: python -m venv failed (ensurepip may be missing). Trying virtualenv…"
  if python -m pip --version >/dev/null 2>&1; then
    python -m pip install --user --upgrade virtualenv >/dev/null 2>&1 || true
    if python -m virtualenv "${envdir}" 2>/dev/null; then
      echo "==> Created environment via python -m virtualenv"
      return 0
    fi
  fi

  return 1
}

# ---- local test install (optional) ----
if [[ "${SKIP_TESTS:-0}" == "1" || "${NO_TESTS_FLAG}" == "1" ]]; then
  echo "==> Tests disabled (SKIP_TESTS/--no-tests). Skipping local installs."
else
  TESTENV=".venv-publish"
  if create_temp_env "${TESTENV}"; then
    # shellcheck disable=SC1090
    source "${TESTENV}/bin/activate"
    python -m pip install --upgrade pip >/dev/null 2>&1 || true

    WHEEL_PATH="$(ls -1 dist/*-py3-none-any.whl 2>/dev/null | head -n1 || true)"
    if [[ -z "${WHEEL_PATH}" ]]; then
      echo "ERROR: Wheel not found in dist/." >&2
      deactivate || true
      rm -rf "${TESTENV}"
      exit 4
    fi

    echo "==> Installing wheel locally: ${WHEEL_PATH}"
    pip install "${WHEEL_PATH}"

    echo "==> Import sanity checks"
    python - <<PY
import importlib
import supertable
assert supertable.__version__ == "${VERSION}", (
    f"installed __version__={supertable.__version__} != expected ${VERSION}"
)
for mod in ("supertable","dotenv","colorlog","polars","sqlglot","duckdb"):
    importlib.import_module(mod)
print(f"Local import smoke test: OK (supertable {supertable.__version__})")
PY

    deactivate
    rm -rf "${TESTENV}"
  else
    echo "WARN: Could not create a virtual environment."
    echo "      On Debian/Ubuntu: sudo apt install python3-venv"
    echo "      Proceeding WITHOUT local install tests. (Set --no-tests to silence this.)"
  fi
fi

# ---- upload ----
echo "==> Uploading to ${REPO}"
export TWINE_USERNAME="__token__"
export TWINE_PASSWORD="${PYPI_TOKEN}"

if [[ "${REPO}" == "testpypi" ]]; then
  twine upload --repository-url https://test.pypi.org/legacy/ dist/*
else
  twine upload dist/*
fi

# ---- tag & push (optional) ----
if [[ "${NO_GIT:-0}" == "1" ]]; then
  echo "==> NO_GIT=1 set; skipping git tag/push"
  exit 0
fi

if git rev-parse --git-dir >/dev/null 2>&1; then
  TAG="v${VERSION}"
  echo "==> Tagging and pushing ${TAG}"
  if git rev-parse "${TAG}" >/dev/null 2>&1; then
    echo "Tag ${TAG} already exists; skipping."
  else
    git tag "${TAG}"
    git push --tags
  fi
else
  echo "WARN: Not a git repository; skipping tag/push."
fi

echo "==> Done."
