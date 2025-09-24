#!/usr/bin/env bash
# publish.sh — bump version, build, test, and upload to (Test)PyPI
# Usage:
#   ./publish.sh                 # uses default VERSION=1.2.0, uploads to PyPI
#   ./publish.sh 1.2.3           # set custom version, uploads to PyPI
#   ./publish.sh --testpypi      # upload to TestPyPI
#   ./publish.sh 1.2.3 --no-tests
# Env:
#   PYPI_TOKEN   (required for upload)
#   NO_GIT=1     (optional: skip git tag/push)
#   SKIP_TESTS=1 (optional: skip local install tests)

set -euo pipefail

# ---- parse args ----
VERSION="1.2.5"
REPO="pypi"  # or "testpypi"
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

echo "==> Target version: ${VERSION}"
echo "==> Target repository: ${REPO}"

# ---- require PYPI_TOKEN for upload ----
if [[ -z "${PYPI_TOKEN:-}" ]]; then
  echo "ERROR: PYPI_TOKEN is not set in the environment." >&2
  echo "Export it first, e.g.:" >&2
  echo "  export PYPI_TOKEN='pypi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'" >&2
  exit 2
fi

# ---- bump versions in pyproject.toml and setup.py ----
[[ -f "pyproject.toml" ]] || { echo "ERROR: pyproject.toml not found."; exit 3; }
[[ -f "setup.py" ]] || echo "WARN: setup.py not found; continuing."

echo "==> Bumping version in pyproject.toml to ${VERSION}"
sed -i -E "0,/^version\s*=\s*\"[^\"]+\"/s//version = \"${VERSION}\"/" pyproject.toml

if [[ -f "setup.py" ]]; then
  echo "==> Bumping version in setup.py to ${VERSION}"
  sed -i -E "s/version\s*=\s*['\"][^'\"]+['\"]/version=\"${VERSION}\"/" setup.py
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
    python - <<'PY'
import importlib
for mod in ("supertable","dotenv","colorlog","polars","sqlglot","duckdb"):
    importlib.import_module(mod)
print("Local import smoke test: OK")
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
