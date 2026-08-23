#!/usr/bin/env bash
# Validate and publish an immutable SuperTable release.
#
# The preferred path creates a signed tag and lets .github/workflows/release.yml
# publish through PyPI Trusted Publishing.  A deliberately explicit token mode
# is retained for existing maintainers: it uploads only the wheel and sdist
# produced by a successful release-gate run for the exact current commit.  This
# script never edits version files, builds from an uncommitted tree, or accepts
# a test bypass.
#
# Usage:
#   ./push-pypi.sh                 # validate the committed release source
#   ./push-pypi.sh --create-tag    # create a signed annotated vX.Y.Z tag
#   ./push-pypi.sh --push          # validate and push an existing tag
#   ./push-pypi.sh --create-tag --push
#   ./push-pypi.sh --upload-token [--token-file PATH]
#   ./push-pypi.sh 2.5.0           # compatibility alias for token upload

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

CREATE_TAG=0
PUSH_TAG=0
TOKEN_UPLOAD=0
TOKEN_FILE=""
TOKEN_FILE_EXPLICIT=0
REQUESTED_VERSION=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --create-tag) CREATE_TAG=1 ;;
    --push) PUSH_TAG=1 ;;
    --upload-token) TOKEN_UPLOAD=1 ;;
    --token-file)
      shift
      if [[ "$#" -eq 0 || -z "$1" ]]; then
        echo "ERROR: --token-file requires a path." >&2
        exit 2
      fi
      TOKEN_FILE="$1"
      TOKEN_FILE_EXPLICIT=1
      ;;
    --help|-h)
      sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    --no-tests|--testpypi|patch|minor|major)
      echo "ERROR: versions must be edited and committed before release; tests cannot be skipped." >&2
      exit 2
      ;;
    [0-9]*.[0-9]*.[0-9]*)
      if [[ -n "${REQUESTED_VERSION}" ]]; then
        echo "ERROR: specify the release version at most once." >&2
        exit 2
      fi
      REQUESTED_VERSION="$1"
      TOKEN_UPLOAD=1
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

if [[ "${TOKEN_UPLOAD}" -eq 1 && ("${CREATE_TAG}" -eq 1 || "${PUSH_TAG}" -eq 1) ]]; then
  echo "ERROR: token upload cannot be combined with tag creation or pushing." >&2
  exit 2
fi
if [[ "${TOKEN_FILE_EXPLICIT}" -eq 1 && "${TOKEN_UPLOAD}" -eq 0 ]]; then
  echo "ERROR: --token-file requires --upload-token (or an exact version argument)." >&2
  exit 2
fi

if [[ -x "${SCRIPT_DIR}/.venv/bin/python" ]]; then
  RELEASE_PYTHON="${SCRIPT_DIR}/.venv/bin/python"
elif [[ -n "${SUPERTABLE_RELEASE_PYTHON:-}" ]]; then
  RELEASE_PYTHON="${SUPERTABLE_RELEASE_PYTHON}"
else
  RELEASE_PYTHON="$(command -v python || true)"
fi
if [[ -z "${RELEASE_PYTHON}" || ! -x "${RELEASE_PYTHON}" ]]; then
  echo "ERROR: no release Python is available; create .venv and install requirements-dev.txt." >&2
  exit 3
fi

RELEASE_MODULES=(pytest ruff mypy bandit pip_audit)
if [[ "${TOKEN_UPLOAD}" -eq 1 ]]; then
  RELEASE_MODULES+=(twine)
  command -v gh >/dev/null || {
    echo "ERROR: token upload requires the GitHub CLI (gh)." >&2
    exit 3
  }
  command -v sha256sum >/dev/null || {
    echo "ERROR: token upload requires sha256sum." >&2
    exit 3
  }
fi
"${RELEASE_PYTHON}" - "${RELEASE_MODULES[@]}" <<'PY'
import importlib.util
import sys

missing = [name for name in sys.argv[1:] if importlib.util.find_spec(name) is None]
if missing:
    raise SystemExit(
        "ERROR: release Python is missing required modules: " + ", ".join(missing)
    )
PY

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

VERSION="$("${RELEASE_PYTHON}" - <<'PY'
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
if [[ -n "${REQUESTED_VERSION}" && "${REQUESTED_VERSION}" != "${VERSION}" ]]; then
  echo "ERROR: requested version ${REQUESTED_VERSION} does not match committed version ${VERSION}." >&2
  exit 5
fi
if [[ "${TOKEN_UPLOAD}" -eq 1 && "${VERSION}" != "2.5.0" ]]; then
  echo "ERROR: compatibility token upload is authorized only for SuperTable 2.5.0." >&2
  exit 5
fi

"${RELEASE_PYTHON}" - "${VERSION}" <<'PY'
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
if [[ "${TOKEN_UPLOAD}" -eq 0 && "${PUSH_TAG}" -eq 1 && "${CREATE_TAG}" -eq 0 ]] \
  && ! git rev-parse -q --verify "refs/tags/${TAG}^{commit}" >/dev/null; then
  echo "ERROR: --push requires the existing signed local tag ${TAG}; use --create-tag first." >&2
  exit 6
fi
if [[ "${TOKEN_UPLOAD}" -eq 0 ]] \
  && git rev-parse -q --verify "refs/tags/${TAG}^{commit}" >/dev/null; then
  TAG_COMMIT="$(git rev-parse "refs/tags/${TAG}^{commit}")"
  if [[ "${TAG_COMMIT}" != "${HEAD_COMMIT}" ]]; then
    echo "ERROR: ${TAG} already identifies ${TAG_COMMIT}, not HEAD ${HEAD_COMMIT}." >&2
    exit 6
  fi
  if ! git verify-tag "${TAG}" >/dev/null 2>&1; then
    echo "ERROR: existing release tag ${TAG} is not a verifiable signed annotated tag." >&2
    exit 6
  fi
elif [[ "${TOKEN_UPLOAD}" -eq 0 && "${CREATE_TAG}" -eq 0 ]]; then
  echo "Validated committed version ${VERSION} at ${HEAD_COMMIT}."
  echo "Next: rerun with --create-tag after reviewing the mandatory local gates."
fi

echo "==> Running the complete test suite"
"${RELEASE_PYTHON}" -m pytest -q

echo "==> Running release lint and type gates"
"${RELEASE_PYTHON}" -m ruff check supertable tests check_mypy_baseline.py
"${RELEASE_PYTHON}" check_mypy_baseline.py supertable

echo "==> Running source security gate"
"${RELEASE_PYTHON}" -m bandit -q -r supertable -lll
AUDIT_LOG="$(mktemp)"
if ! "${RELEASE_PYTHON}" -m pip_audit -r requirements.txt \
  --ignore-vuln GHSA-rgxp-2hwp-jwgg >"${AUDIT_LOG}" 2>&1; then
  if ! "${RELEASE_PYTHON}" -m ensurepip --version >/dev/null 2>&1 \
    && "${RELEASE_PYTHON}" - "${AUDIT_LOG}" <<'PY'
import pathlib
import re
import sys

message = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
normalized = re.sub(r"\s+", " ", message)
if "virtual environment was not created successfully because ensurepip is not available" not in normalized:
    raise SystemExit(1)
PY
  then
    echo "WARN: stdlib venv support is unavailable; auditing the checked release environment instead."
    echo "WARN: CI's exact requirements audit remains mandatory before any token upload."
    "${RELEASE_PYTHON}" -m pip check
    "${RELEASE_PYTHON}" -m pip_audit --local \
      --ignore-vuln GHSA-rgxp-2hwp-jwgg
  else
    cat "${AUDIT_LOG}" >&2
    rm -f "${AUDIT_LOG}"
    exit 7
  fi
else
  cat "${AUDIT_LOG}"
fi
rm -f "${AUDIT_LOG}"

upload_green_ci_artifact_with_token() {
  if [[ "$(git symbolic-ref --quiet --short HEAD || true)" != "master" ]]; then
    echo "ERROR: token upload is allowed only from the master branch." >&2
    return 8
  fi

  local origin_url repo_slug remote_head
  origin_url="$(git remote get-url origin)"
  repo_slug="$("${RELEASE_PYTHON}" - "${origin_url}" <<'PY'
import re
import sys
from urllib.parse import urlparse

value = sys.argv[1]
if re.fullmatch(r"(?:[^@/:]+@)?github\.com:[^/]+/[^/]+(?:\.git)?", value):
    path = value.split(":", 1)[1]
else:
    parsed = urlparse(value)
    if parsed.hostname != "github.com" or parsed.scheme not in {"https", "ssh", "git"}:
        raise SystemExit("ERROR: token upload requires origin to be a github.com repository")
    path = parsed.path.lstrip("/")
if path.endswith(".git"):
    path = path[:-4]
if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", path):
    raise SystemExit("ERROR: origin does not identify one unambiguous owner/repository")
print(path)
PY
)"
  if [[ "${repo_slug}" != "kladnasoft/supertable" ]]; then
    echo "ERROR: token upload is pinned to github.com/kladnasoft/supertable." >&2
    return 8
  fi
  remote_head="$(git ls-remote --heads origin refs/heads/master | awk 'NR == 1 {print $1}')"
  if [[ ! "${remote_head}" =~ ^[0-9a-f]{40}$ || "${remote_head}" != "${HEAD_COMMIT}" ]]; then
    echo "ERROR: local HEAD ${HEAD_COMMIT} is not the authoritative origin/master ${remote_head:-missing}." >&2
    return 8
  fi

  local release_tmp
  release_tmp="$(mktemp -d "${TMPDIR:-/tmp}/supertable-token-release.XXXXXX")"
  case "${release_tmp}" in
    "${TMPDIR:-/tmp}"/supertable-token-release.*) ;;
    *) echo "ERROR: invalid release temporary directory." >&2; return 8 ;;
  esac
  cleanup_token_release() {
    if [[ -n "${release_tmp:-}" && -d "${release_tmp}" ]]; then
      rm -rf -- "${release_tmp}"
    fi
  }
  interrupt_token_release() {
    cleanup_token_release
    trap - EXIT INT TERM
    exit 130
  }
  terminate_token_release() {
    cleanup_token_release
    trap - EXIT INT TERM
    exit 143
  }
  trap cleanup_token_release EXIT
  trap interrupt_token_release INT
  trap terminate_token_release TERM

  echo "==> Verifying the successful release-gate run for ${HEAD_COMMIT}"
  env -u GH_HOST -u GH_REPO gh api --hostname github.com --method GET \
    "repos/${repo_slug}/actions/workflows/release.yml/runs" \
    -f head_sha="${HEAD_COMMIT}" -f branch=master -f event=push \
    -f status=success -f per_page=20 >"${release_tmp}/runs.json"

  local run_id
  run_id="$("${RELEASE_PYTHON}" - "${release_tmp}/runs.json" "${HEAD_COMMIT}" <<'PY'
import json
import pathlib
import sys

runs = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))["workflow_runs"]
matches = [
    run for run in runs
    if run.get("head_sha") == sys.argv[2]
    and run.get("head_branch") == "master"
    and run.get("event") == "push"
    and run.get("status") == "completed"
    and run.get("conclusion") == "success"
    and run.get("path") == ".github/workflows/release.yml"
    and run.get("repository", {}).get("id") == 967944110
    and run.get("head_repository", {}).get("id") == 967944110
]
if len(matches) != 1:
    raise SystemExit(
        f"ERROR: expected one successful release-gate run for this commit, found {len(matches)}"
    )
print(matches[0]["id"])
PY
)"

  env -u GH_HOST -u GH_REPO gh api --hostname github.com --method GET \
    "repos/${repo_slug}/actions/runs/${run_id}/jobs" \
    -f per_page=100 >"${release_tmp}/jobs.json"
  "${RELEASE_PYTHON}" - "${release_tmp}/jobs.json" <<'PY'
import json
import pathlib
import sys

expected = {
    "full tests / Python 3.10": "success",
    "full tests / Python 3.11": "success",
    "full tests / Python 3.12": "success",
    "full tests / Python 3.13": "success",
    "dependencies / minimum": "success",
    "dependencies / latest-supported": "success",
    "Delta and Iceberg external conformance": "success",
    "lint, types, and dependency security": "success",
    "build and install immutable artifacts": "success",
    "protected trusted publish": "skipped",
}
jobs = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))["jobs"]
names = [job.get("name") for job in jobs]
if len(names) != len(set(names)):
    raise SystemExit("ERROR: release-gate run contains duplicate job names")
observed = {job.get("name"): job.get("conclusion") for job in jobs}
if observed != expected:
    raise SystemExit(
        f"ERROR: release-gate job map differs from the protected contract: {observed}"
    )
PY

  env -u GH_HOST -u GH_REPO gh api --hostname github.com \
    "repos/${repo_slug}/actions/runs/${run_id}/artifacts" \
    >"${release_tmp}/artifacts.json"
  local artifact_id artifact_digest
  IFS=$'\t' read -r artifact_id artifact_digest < <(
    "${RELEASE_PYTHON}" - "${release_tmp}/artifacts.json" \
      "distributions-${HEAD_COMMIT}" "${HEAD_COMMIT}" "${run_id}" <<'PY'
import json
import pathlib
import sys

artifacts = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))["artifacts"]
matches = [item for item in artifacts if item.get("name") == sys.argv[2] and not item.get("expired")]
if len(matches) != 1:
    raise SystemExit(f"ERROR: expected one live exact-commit artifact, found {len(matches)}")
artifact = matches[0]
workflow_run = artifact.get("workflow_run", {})
if (
    workflow_run.get("id") != int(sys.argv[4])
    or workflow_run.get("head_sha") != sys.argv[3]
    or workflow_run.get("head_branch") != "master"
    or workflow_run.get("repository_id") != 967944110
    or workflow_run.get("head_repository_id") != 967944110
):
    raise SystemExit("ERROR: artifact workflow provenance does not match the canonical run")
digest = artifact.get("digest")
if not isinstance(digest, str) or not digest.startswith("sha256:"):
    raise SystemExit("ERROR: the CI artifact has no SHA-256 provenance digest")
print(f"{artifact['id']}\t{digest.removeprefix('sha256:')}")
PY
  )

  env -u GH_HOST -u GH_REPO gh api --hostname github.com \
    "repos/${repo_slug}/actions/artifacts/${artifact_id}/zip" \
    >"${release_tmp}/artifact.zip"
  local observed_digest
  observed_digest="$(sha256sum "${release_tmp}/artifact.zip" | awk '{print $1}')"
  if [[ "${observed_digest}" != "${artifact_digest}" ]]; then
    echo "ERROR: downloaded CI artifact digest mismatch." >&2
    return 8
  fi

  mkdir "${release_tmp}/dist"
  "${RELEASE_PYTHON}" - "${release_tmp}/artifact.zip" \
    "${release_tmp}/dist" "${VERSION}" <<'PY'
import email
import io
import pathlib
import stat
import sys
import tarfile
import zipfile

archive_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
version = sys.argv[3]
if archive_path.stat().st_size > 256 * 1024 * 1024:
    raise SystemExit("ERROR: CI artifact archive exceeds the release size bound")
expected = {
    f"supertable-{version}-py3-none-any.whl",
    f"supertable-{version}.tar.gz",
}
with zipfile.ZipFile(archive_path) as archive:
    infos = archive.infolist()
    names = {info.filename for info in infos}
    if names != expected or len(infos) != 2:
        raise SystemExit(f"ERROR: CI artifact contents differ from the exact release pair: {sorted(names)}")
    payloads = {}
    for info in infos:
        mode = (info.external_attr >> 16) & 0o170000
        if (
            info.is_dir()
            or "/" in info.filename
            or mode == stat.S_IFLNK
            or info.file_size > 128 * 1024 * 1024
        ):
            raise SystemExit("ERROR: unsafe path or link in CI artifact")
        payloads[info.filename] = archive.read(info)
        (output_path / info.filename).write_bytes(payloads[info.filename])

wheel_name = f"supertable-{version}-py3-none-any.whl"
with zipfile.ZipFile(io.BytesIO(payloads[wheel_name])) as wheel:
    metadata_names = [name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")]
    if len(metadata_names) != 1:
        raise SystemExit("ERROR: wheel has no unique METADATA")
    wheel_metadata = email.message_from_bytes(wheel.read(metadata_names[0]))
    if wheel_metadata.get("Name") != "supertable" or wheel_metadata.get("Version") != version:
        raise SystemExit("ERROR: wheel metadata does not match the committed release")

sdist_name = f"supertable-{version}.tar.gz"
with tarfile.open(fileobj=io.BytesIO(payloads[sdist_name]), mode="r:gz") as sdist:
    metadata_members = [member for member in sdist.getmembers() if member.name.endswith("/PKG-INFO")]
    if len(metadata_members) != 1 or not metadata_members[0].isfile():
        raise SystemExit("ERROR: sdist has no unique PKG-INFO")
    handle = sdist.extractfile(metadata_members[0])
    if handle is None:
        raise SystemExit("ERROR: cannot read sdist PKG-INFO")
    sdist_metadata = email.message_from_bytes(handle.read())
    if sdist_metadata.get("Name") != "supertable" or sdist_metadata.get("Version") != version:
        raise SystemExit("ERROR: sdist metadata does not match the committed release")
PY

  "${RELEASE_PYTHON}" -m twine check "${release_tmp}"/dist/*
  "${RELEASE_PYTHON}" - "${VERSION}" <<'PY'
import json
import sys
import urllib.error
import urllib.request

url = f"https://pypi.org/pypi/supertable/{sys.argv[1]}/json"
try:
    with urllib.request.urlopen(url, timeout=20) as response:
        json.load(response)
except urllib.error.HTTPError as exc:
    if exc.code != 404:
        raise
else:
    raise SystemExit(f"ERROR: supertable {sys.argv[1]} already exists on PyPI")
PY

  if [[ -z "${TOKEN_FILE}" ]]; then
    local parent_token
    parent_token="$(cd "${SCRIPT_DIR}/../.." && pwd)/TOKEN"
    if [[ -f "${parent_token}" ]]; then
      TOKEN_FILE="${parent_token}"
    else
      echo "ERROR: no token file found; use --token-file PATH." >&2
      return 8
    fi
  fi

  local final_remote_head
  final_remote_head="$(git ls-remote --heads origin refs/heads/master | awk 'NR == 1 {print $1}')"
  if [[ "$(git symbolic-ref --quiet --short HEAD || true)" != "master" \
    || "$(git rev-parse HEAD)" != "${HEAD_COMMIT}" \
    || -n "$(git status --porcelain=v1 --untracked-files=all)" \
    || "${final_remote_head}" != "${HEAD_COMMIT}" ]]; then
    echo "ERROR: release source changed while the local gates were running; refusing upload." >&2
    return 8
  fi

  set +x
  echo "==> Uploading the exact green-CI SuperTable ${VERSION} artifacts to PyPI"
  "${RELEASE_PYTHON}" -I - "${TOKEN_FILE}" \
    "${release_tmp}/dist/supertable-${VERSION}-py3-none-any.whl" \
    "${release_tmp}/dist/supertable-${VERSION}.tar.gz" <<'PY'
import os
import pathlib
import re
import stat
import subprocess
import sys

path = pathlib.Path(sys.argv[1])
flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
try:
    descriptor = os.open(path, flags)
except OSError as exc:
    raise SystemExit(f"ERROR: cannot securely open token file: {exc.strerror}") from None
try:
    info = os.fstat(descriptor)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise SystemExit("ERROR: token file must be an owner-only regular file (mode 0600)")
    with os.fdopen(descriptor, "rb", closefd=True) as handle:
        descriptor = -1
        payload = handle.read(16_385)
finally:
    if descriptor >= 0:
        os.close(descriptor)
if len(payload) > 16_384:
    raise SystemExit("ERROR: token file exceeds the size bound")
if b"\r" in payload:
    raise SystemExit("ERROR: token file must use one newline-delimited UTF-8 line")
try:
    text = payload.decode("utf-8")
except UnicodeDecodeError:
    raise SystemExit("ERROR: token file is not valid UTF-8") from None
if text.endswith("\n"):
    text = text[:-1]
if not text or "\n" in text or text != text.strip():
    raise SystemExit("ERROR: token file must contain exactly one unpadded line")
value = text
match = re.fullmatch(
    r"(?:export\s+)?PYPI_TOKEN=(?:\"(pypi-[A-Za-z0-9_-]+)\"|'(pypi-[A-Za-z0-9_-]+)'|(pypi-[A-Za-z0-9_-]+))",
    value,
)
if match:
    value = next(part for part in match.groups() if part is not None)
if not re.fullmatch(r"pypi-[A-Za-z0-9_-]+", value):
    raise SystemExit("ERROR: token file has an invalid format")

environment = os.environ.copy()
for name in (
    "PYTHONPATH",
    "PYTHONHOME",
    "PYTHONSTARTUP",
    "TWINE_REPOSITORY",
    "TWINE_REPOSITORY_URL",
    "TWINE_CONFIG_FILE",
    "TWINE_CERT",
    "TWINE_CLIENT_CERT",
    "TWINE_USER",
    "TWINE_PASSWORD",
    "TWINE_USERNAME",
):
    environment.pop(name, None)
environment.update(
    {
        "TWINE_USERNAME": "__token__",
        "TWINE_PASSWORD": value,
        "TWINE_NON_INTERACTIVE": "1",
    }
)
try:
    subprocess.run(
        [
            sys.executable,
            "-I",
            "-m",
            "twine",
            "upload",
            "--repository-url",
            "https://upload.pypi.org/legacy/",
            "--non-interactive",
            "--disable-progress-bar",
            sys.argv[2],
            sys.argv[3],
        ],
        check=True,
        env=environment,
    )
finally:
    environment["TWINE_PASSWORD"] = ""
    value = ""
PY
  echo "Published SuperTable ${VERSION} from release-gate run ${run_id}."
  echo "No Git tag was created by compatibility token mode."
  cleanup_token_release
  trap - EXIT INT TERM
}

if [[ "${TOKEN_UPLOAD}" -eq 1 ]]; then
  upload_green_ci_artifact_with_token
  exit 0
fi

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
