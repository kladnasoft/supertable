#!/usr/bin/env bash
# Build, validate, and publish the committed SuperTable version with a PyPI token.
#
# Usage:
#   ./publish.sh
#   ./publish.sh --token-file /path/to/TOKEN

set -euo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "${SCRIPT_DIR}"

unset GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_ASKPASS GIT_CEILING_DIRECTORIES
unset GIT_COMMON_DIR GIT_CONFIG_COUNT GIT_CONFIG_GLOBAL GIT_CONFIG_NOSYSTEM
unset GIT_CONFIG_PARAMETERS GIT_CONFIG_SYSTEM GIT_DIR GIT_DISCOVERY_ACROSS_FILESYSTEM
unset GIT_EXEC_PATH GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_PROXY_COMMAND
unset GIT_SSH GIT_SSH_COMMAND GIT_WORK_TREE
export GIT_NO_REPLACE_OBJECTS=1 GIT_TERMINAL_PROMPT=0

TOKEN_FILE="${SCRIPT_DIR}/TOKEN"
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --token-file)
      shift
      if [[ "$#" -eq 0 || -z "$1" ]]; then
        echo "ERROR: --token-file requires a path." >&2
        exit 2
      fi
      TOKEN_FILE="$1"
      ;;
    --help|-h)
      sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

PUBLISH_PYTHON="${SCRIPT_DIR}/.venv/bin/python"
if [[ ! -x "${PUBLISH_PYTHON}" ]]; then
  echo "ERROR: ${PUBLISH_PYTHON} is unavailable; create the project .venv first." >&2
  exit 3
fi
"${PUBLISH_PYTHON}" -I - <<'PY'
import importlib.metadata
import pathlib
import re

required = {"build", "pip", "setuptools", "twine"}
pins = {}
for line in pathlib.Path("requirements-dev.txt").read_text(encoding="utf-8").splitlines():
    match = re.fullmatch(r"([A-Za-z0-9_.-]+)==([^;\s]+)(?:;.*)?", line.strip())
    if match:
        pins[match.group(1).lower().replace("_", "-")] = match.group(2)
missing_pins = sorted(required - pins.keys())
if missing_pins:
    raise SystemExit(
        "ERROR: requirements-dev.txt lacks exact publish-tool pins: "
        + ", ".join(missing_pins)
    )
mismatches = []
for name in sorted(required):
    try:
        actual = importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        actual = "missing"
    if actual != pins[name]:
        mismatches.append(f"{name}={actual} (required {pins[name]})")
if mismatches:
    raise SystemExit("ERROR: .venv publish tooling is not pinned: " + ", ".join(mismatches))
PY

command -v git >/dev/null || { echo "ERROR: git is required." >&2; exit 3; }
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
  echo "ERROR: publish.sh must run from a Git checkout." >&2
  exit 3
}
GIT_TOP_LEVEL="$(git rev-parse --show-toplevel)"
if [[ "$(cd "${GIT_TOP_LEVEL}" && pwd -P)" != "${SCRIPT_DIR}" ]]; then
  echo "ERROR: publish.sh must be at the Git checkout root." >&2
  exit 3
fi
if [[ -n "$(git for-each-ref --format='%(refname)' refs/replace)" \
  || -s "$(git rev-parse --absolute-git-dir)/info/grafts" ]]; then
  echo "ERROR: Git replacement objects/grafts are forbidden for releases." >&2
  exit 3
fi
if [[ "$(git symbolic-ref --quiet --short HEAD || true)" != "master" ]]; then
  echo "ERROR: token publishing is allowed only from the master branch." >&2
  exit 4
fi
if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
  echo "ERROR: publish checkout is not clean; commit the release version first." >&2
  git status --short >&2
  exit 4
fi
ORIGIN_URL="$(git remote get-url origin)"
case "${ORIGIN_URL}" in
  https://github.com/kladnasoft/supertable|https://github.com/kladnasoft/supertable.git|\
  git@github.com:kladnasoft/supertable|git@github.com:kladnasoft/supertable.git|\
  ssh://git@github.com/kladnasoft/supertable|ssh://git@github.com/kladnasoft/supertable.git) ;;
  *)
    echo "ERROR: origin is not the canonical kladnasoft/supertable repository." >&2
    exit 3
    ;;
esac

IFS=$'\t' read -r PROJECT_NAME VERSION INIT_VERSION < <(
  "${PUBLISH_PYTHON}" -I - <<'PY'
import ast
import pathlib

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

with open("pyproject.toml", "rb") as handle:
    project = tomllib.load(handle).get("project", {})
name = project.get("name")
version = project.get("version")
tree = ast.parse(pathlib.Path("supertable/__init__.py").read_text(encoding="utf-8"))
init_versions = [
    node.value.value
    for node in tree.body
    if isinstance(node, ast.Assign)
    for target in node.targets
    if isinstance(target, ast.Name)
    and target.id == "__version__"
    and isinstance(node.value, ast.Constant)
    and isinstance(node.value.value, str)
]
if not isinstance(name, str) or not isinstance(version, str):
    raise SystemExit("ERROR: pyproject.toml has invalid project name/version metadata")
if len(init_versions) != 1:
    raise SystemExit("ERROR: supertable.__version__ must have one literal assignment")
print(f"{name}\t{version}\t{init_versions[0]}")
PY
)
if [[ "${PROJECT_NAME}" != "supertable" ]]; then
  echo "ERROR: project name must be exactly supertable, found ${PROJECT_NAME}." >&2
  exit 5
fi
if ! [[ "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "ERROR: project.version is not a release SemVer: ${VERSION}" >&2
  exit 5
fi
if [[ "${INIT_VERSION}" != "${VERSION}" ]]; then
  echo "ERROR: version mismatch: pyproject=${VERSION}, __init__=${INIT_VERSION}." >&2
  exit 5
fi

HEAD_COMMIT="$(git rev-parse HEAD)"
REMOTE_HEAD="$(git ls-remote --heads origin refs/heads/master | awk 'NR == 1 {print $1}')"
if [[ ! "${REMOTE_HEAD}" =~ ^[0-9a-f]{40}$ || "${REMOTE_HEAD}" != "${HEAD_COMMIT}" ]]; then
  echo "ERROR: local HEAD ${HEAD_COMMIT} is not origin/master ${REMOTE_HEAD:-missing}." >&2
  exit 6
fi
PUBLISH_TMP_ROOT="${TMPDIR:-/tmp}"
RELEASE_DIR="$(mktemp -d "${PUBLISH_TMP_ROOT}/supertable-publish.XXXXXX")"
case "${RELEASE_DIR}" in
  "${PUBLISH_TMP_ROOT}"/supertable-publish.*) ;;
  *) echo "ERROR: invalid publish temporary directory." >&2; exit 6 ;;
esac
cleanup() {
  if [[ -n "${RELEASE_DIR:-}" && -d "${RELEASE_DIR}" ]]; then
    rm -rf -- "${RELEASE_DIR}"
  fi
}
interrupt_publish() {
  cleanup
  trap - EXIT INT TERM
  exit 130
}
terminate_publish() {
  cleanup
  trap - EXIT INT TERM
  exit 143
}
trap cleanup EXIT
trap interrupt_publish INT
trap terminate_publish TERM

ARTIFACT_CACHE_ROOT="${SCRIPT_DIR}/dist/publish-cache"
ARTIFACT_CACHE_DIR="${ARTIFACT_CACHE_ROOT}/${VERSION}-${HEAD_COMMIT}"
if ! git check-ignore -q -- "${ARTIFACT_CACHE_ROOT}/probe"; then
  echo "ERROR: dist/publish-cache must remain ignored by Git." >&2
  exit 7
fi
if [[ -L "${SCRIPT_DIR}/dist" || -L "${ARTIFACT_CACHE_ROOT}" \
  || -L "${ARTIFACT_CACHE_DIR}" ]]; then
  echo "ERROR: publish artifact cache paths must not be symbolic links." >&2
  exit 7
fi
mkdir -p "${ARTIFACT_CACHE_DIR}"
if [[ "$(cd "${ARTIFACT_CACHE_DIR}" && pwd -P)" != "${ARTIFACT_CACHE_DIR}" ]]; then
  echo "ERROR: publish artifact cache resolved outside its expected path." >&2
  exit 7
fi
if [[ "$(stat -c '%u' "${ARTIFACT_CACHE_ROOT}")" != "$(id -u)" \
  || "$(stat -c '%u' "${ARTIFACT_CACHE_DIR}")" != "$(id -u)" ]]; then
  echo "ERROR: publish artifact cache must be owned by the release user." >&2
  exit 7
fi
chmod 700 "${ARTIFACT_CACHE_ROOT}" "${ARTIFACT_CACHE_DIR}"

CACHED_WHEEL="${ARTIFACT_CACHE_DIR}/supertable-${VERSION}-py3-none-any.whl"
CACHED_SDIST="${ARTIFACT_CACHE_DIR}/supertable-${VERSION}.tar.gz"
CACHE_MANIFEST="${ARTIFACT_CACHE_DIR}/manifest.json"
if [[ -L "${CACHED_WHEEL}" || -L "${CACHED_SDIST}" || -L "${CACHE_MANIFEST}" ]]; then
  echo "ERROR: cached publish artifacts must not be symbolic links." >&2
  exit 7
fi

BUILT_NEW_ARTIFACTS=0
CACHE_IS_VALID=0
if [[ -f "${CACHED_WHEEL}" && -f "${CACHED_SDIST}" && -f "${CACHE_MANIFEST}" ]] \
  && "${PUBLISH_PYTHON}" -I - "${ARTIFACT_CACHE_DIR}" "${CACHE_MANIFEST}" \
    "${CACHED_WHEEL}" "${CACHED_SDIST}" "${VERSION}" "${HEAD_COMMIT}" <<'PY'
import hashlib
import json
import os
import pathlib
import stat
import sys

cache_dir, manifest, wheel, sdist = [
    pathlib.Path(value) for value in sys.argv[1:5]
]
version, commit = sys.argv[5:]
for path, required_mode in (
    (cache_dir, 0o700),
    (manifest, 0o600),
    (wheel, 0o600),
    (sdist, 0o600),
):
    info = path.lstat()
    expected_type = stat.S_ISDIR if path == cache_dir else stat.S_ISREG
    if (
        not expected_type(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != required_mode
        or (path != cache_dir and info.st_nlink != 1)
    ):
        raise SystemExit(f"invalid cached path: {path}")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


try:
    document = json.loads(manifest.read_bytes())
except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
    raise SystemExit(f"invalid cache manifest: {exc}") from None
expected = {
    "schema": 1,
    "project": "supertable",
    "version": version,
    "commit": commit,
    "files": {wheel.name: sha256(wheel), sdist.name: sha256(sdist)},
}
if document != expected:
    raise SystemExit("cached artifact hashes do not match the completion manifest")
PY
then
  CACHE_IS_VALID=1
elif [[ -e "${CACHED_WHEEL}" || -e "${CACHED_SDIST}" || -e "${CACHE_MANIFEST}" ]]; then
  echo "WARN: incomplete or invalid publish cache; rebuilding it." >&2
fi

if [[ "${CACHE_IS_VALID}" -eq 1 ]]; then
  WHEEL="${CACHED_WHEEL}"
  SDIST="${CACHED_SDIST}"
  echo "==> Reusing the exact cached artifacts for ${HEAD_COMMIT}"
else
  SOURCE_TREE="${RELEASE_DIR}/source"
  DIST_DIR="${RELEASE_DIR}/dist"
  mkdir -m 700 "${SOURCE_TREE}"
  git archive --format=tar "${HEAD_COMMIT}" | tar -xf - -C "${SOURCE_TREE}"

  SOURCE_DATE_EPOCH="$(git show -s --format=%ct "${HEAD_COMMIT}")"
  if ! [[ "${SOURCE_DATE_EPOCH}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: release commit has no valid source timestamp." >&2
    exit 7
  fi
  export SOURCE_DATE_EPOCH PYTHONHASHSEED=0 TZ=UTC

  echo "==> Building SuperTable ${VERSION} from committed source ${HEAD_COMMIT}"
  "${PUBLISH_PYTHON}" -I -m build --no-isolation --outdir "${DIST_DIR}" "${SOURCE_TREE}"

  WHEEL="${DIST_DIR}/supertable-${VERSION}-py3-none-any.whl"
  SDIST="${DIST_DIR}/supertable-${VERSION}.tar.gz"
  "${PUBLISH_PYTHON}" -I - "${SDIST}" "${SOURCE_DATE_EPOCH}" <<'PY'
import gzip
import os
import pathlib
import sys
import tarfile

sdist = pathlib.Path(sys.argv[1]).resolve(strict=True)
timestamp = int(sys.argv[2])
temporary = sdist.with_name(sdist.name + ".canonical")
with tarfile.open(sdist, mode="r:gz") as source, temporary.open("xb") as raw:
    with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=timestamp) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as target:
            for member in source.getmembers():
                member.uid = 0
                member.gid = 0
                member.uname = ""
                member.gname = ""
                member.mtime = timestamp
                member.pax_headers = dict(
                    sorted(
                        (key, value)
                        for key, value in member.pax_headers.items()
                        if key not in {"atime", "ctime", "mtime"}
                    )
                )
                handle = source.extractfile(member) if member.isreg() else None
                target.addfile(member, handle)
os.replace(temporary, sdist)
print(f"Canonical sdist timestamp: {timestamp}")
PY

  mapfile -t BUILT_FILES < <(find "${DIST_DIR}" -maxdepth 1 -type f -printf '%f\n' | sort)
  EXPECTED_FILES=("$(basename "${WHEEL}")" "$(basename "${SDIST}")")
  if [[ "${#BUILT_FILES[@]}" -ne 2 \
    || "${BUILT_FILES[0]}" != "${EXPECTED_FILES[0]}" \
    || "${BUILT_FILES[1]}" != "${EXPECTED_FILES[1]}" ]]; then
    echo "ERROR: build did not produce the exact wheel/sdist pair." >&2
    printf '  %s\n' "${BUILT_FILES[@]}" >&2
    exit 7
  fi
  BUILT_NEW_ARTIFACTS=1
fi

echo "==> Checking package artifacts"
"${PUBLISH_PYTHON}" -I -m twine check "${WHEEL}" "${SDIST}"
"${PUBLISH_PYTHON}" -I - "${WHEEL}" "${SDIST}" "${VERSION}" <<'PY'
import email
import pathlib
import sys
import tarfile
import zipfile

wheel = pathlib.Path(sys.argv[1]).resolve(strict=True)
sdist = pathlib.Path(sys.argv[2]).resolve(strict=True)
version = sys.argv[3]
metadata_name = f"supertable-{version}.dist-info/METADATA"
with zipfile.ZipFile(wheel) as archive:
    wheel_metadata = email.message_from_bytes(archive.read(metadata_name))
with tarfile.open(sdist, mode="r:gz") as archive:
    member = archive.getmember(f"supertable-{version}/PKG-INFO")
    handle = archive.extractfile(member)
    if handle is None:
        raise SystemExit("ERROR: sdist PKG-INFO cannot be read")
    sdist_metadata = email.message_from_bytes(handle.read())
for label, metadata in (("wheel", wheel_metadata), ("sdist", sdist_metadata)):
    if metadata.get("Name") != "supertable" or metadata.get("Version") != version:
        raise SystemExit(f"ERROR: {label} metadata does not match supertable {version}")

print(f"Artifact metadata: supertable {version}")
PY

echo "==> Installing the wheel in an isolated smoke-test environment"
SMOKE_VENV="${RELEASE_DIR}/smoke-venv"
"${PUBLISH_PYTHON}" -I -m venv --without-pip "${SMOKE_VENV}"
SMOKE_PYTHON="${SMOKE_VENV}/bin/python"
"${PUBLISH_PYTHON}" -I -m pip --python "${SMOKE_PYTHON}" install \
  --isolated --disable-pip-version-check --no-input --prefer-binary "${WHEEL}"
(
  cd "${RELEASE_DIR}"
  "${SMOKE_PYTHON}" -I - "${VERSION}" <<'PY'
import importlib.metadata
import pathlib
import sys

import supertable

version = sys.argv[1]
module_path = pathlib.Path(supertable.__file__).resolve()
venv_path = pathlib.Path(sys.prefix).resolve()
if supertable.__version__ != version or not module_path.is_relative_to(venv_path):
    raise SystemExit("ERROR: isolated wheel import/version smoke test failed")
if importlib.metadata.version("supertable") != version:
    raise SystemExit("ERROR: installed distribution version does not match the wheel")
print(f"Isolated wheel import: supertable {version}")
PY
)

if [[ "${BUILT_NEW_ARTIFACTS}" -eq 1 ]]; then
  install -m 600 "${WHEEL}" "${CACHED_WHEEL}"
  install -m 600 "${SDIST}" "${CACHED_SDIST}"
  cmp --silent "${WHEEL}" "${CACHED_WHEEL}"
  cmp --silent "${SDIST}" "${CACHED_SDIST}"
  "${PUBLISH_PYTHON}" -I - "${CACHE_MANIFEST}" "${CACHED_WHEEL}" \
    "${CACHED_SDIST}" "${VERSION}" "${HEAD_COMMIT}" <<'PY'
import hashlib
import json
import os
import pathlib
import sys
import uuid

manifest, wheel, sdist = [pathlib.Path(value) for value in sys.argv[1:4]]
version, commit = sys.argv[4:]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


document = {
    "schema": 1,
    "project": "supertable",
    "version": version,
    "commit": commit,
    "files": {wheel.name: sha256(wheel), sdist.name: sha256(sdist)},
}
temporary = manifest.with_name(f".{manifest.name}.{uuid.uuid4().hex}")
flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
descriptor = os.open(temporary, flags, 0o600)
try:
    with os.fdopen(descriptor, "w", encoding="utf-8", closefd=True) as handle:
        descriptor = -1
        json.dump(document, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
finally:
    if descriptor >= 0:
        os.close(descriptor)
os.replace(temporary, manifest)
PY
  WHEEL="${CACHED_WHEEL}"
  SDIST="${CACHED_SDIST}"
  echo "==> Retained the exact validated artifacts for safe retry"
fi

echo "==> Rechecking the exact release source before upload"
if [[ "$(git symbolic-ref --quiet --short HEAD || true)" != "master" ]]; then
  echo "ERROR: release branch changed while artifacts were being validated." >&2
  exit 8
fi
CURRENT_HEAD="$(git rev-parse HEAD)"
if [[ "${CURRENT_HEAD}" != "${HEAD_COMMIT}" ]]; then
  echo "ERROR: release HEAD changed from ${HEAD_COMMIT} to ${CURRENT_HEAD}." >&2
  exit 8
fi
if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
  echo "ERROR: release checkout changed while artifacts were being validated." >&2
  git status --short >&2
  exit 8
fi
REMOTE_HEAD="$(git ls-remote --heads origin refs/heads/master | awk 'NR == 1 {print $1}')"
if [[ ! "${REMOTE_HEAD}" =~ ^[0-9a-f]{40}$ || "${REMOTE_HEAD}" != "${HEAD_COMMIT}" ]]; then
  echo "ERROR: origin/master changed while artifacts were being validated." >&2
  exit 8
fi
if [[ "$(git remote get-url origin)" != "${ORIGIN_URL}" ]]; then
  echo "ERROR: origin changed while artifacts were being validated." >&2
  exit 8
fi

set +x
echo "==> Publishing SuperTable ${VERSION} to production PyPI"
"${PUBLISH_PYTHON}" -I - "${TOKEN_FILE}" "${VERSION}" "${WHEEL}" "${SDIST}" <<'PY'
import hashlib
import json
import os
import pathlib
import re
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request


class RegistryIntegrityError(Exception):
    pass


class RegistryUnavailableError(Exception):
    pass


token_path = pathlib.Path(sys.argv[1])
version = sys.argv[2]
artifacts = [pathlib.Path(value).resolve(strict=True) for value in sys.argv[3:]]
expected_types = {
    f"supertable-{version}-py3-none-any.whl": "bdist_wheel",
    f"supertable-{version}.tar.gz": "sdist",
}
if {path.name for path in artifacts} != set(expected_types) or len(artifacts) != 2:
    raise SystemExit("ERROR: uploader did not receive the exact wheel/sdist pair")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


expected = {path.name: sha256(path) for path in artifacts}
registry_url = f"https://pypi.org/pypi/supertable/{version}/json"


def fetch_remote():
    request = urllib.request.Request(
        registry_url,
        headers={
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "User-Agent": "supertable-publish/1",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = response.read(2_000_001)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return {}
        raise RegistryUnavailableError(f"PyPI returned HTTP {exc.code}") from None
    except (OSError, urllib.error.URLError) as exc:
        raise RegistryUnavailableError(str(exc)) from None
    if len(payload) > 2_000_000:
        raise RegistryIntegrityError("PyPI release metadata exceeds the size bound")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RegistryUnavailableError(f"PyPI returned invalid JSON: {exc}") from None
    if not isinstance(document, dict):
        raise RegistryIntegrityError("PyPI release metadata is not an object")
    info = document.get("info")
    urls = document.get("urls")
    if not isinstance(info, dict) or not isinstance(urls, list):
        raise RegistryIntegrityError("PyPI release metadata has an invalid shape")
    if info.get("name") != "supertable" or info.get("version") != version:
        raise RegistryIntegrityError("PyPI release identity does not match this build")
    remote = {}
    for item in urls:
        if not isinstance(item, dict):
            raise RegistryIntegrityError("PyPI returned an invalid artifact record")
        filename = item.get("filename")
        digests = item.get("digests")
        digest = digests.get("sha256") if isinstance(digests, dict) else None
        if not isinstance(filename, str) or filename in remote:
            raise RegistryIntegrityError("PyPI returned a missing/duplicate filename")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise RegistryIntegrityError(f"PyPI returned an invalid hash for {filename}")
        if filename not in expected_types:
            raise RegistryIntegrityError(f"PyPI has an unexpected file: {filename}")
        if item.get("packagetype") != expected_types[filename]:
            raise RegistryIntegrityError(f"PyPI has an invalid package type for {filename}")
        if item.get("yanked") is True:
            raise RegistryIntegrityError(f"PyPI artifact is yanked: {filename}")
        if digest != expected[filename]:
            raise RegistryIntegrityError(f"PyPI hash differs for {filename}")
        remote[filename] = digest
    return remote


remote = None
last_registry_error = None
for delay in (0, 1, 2):
    if delay:
        time.sleep(delay)
    try:
        remote = fetch_remote()
        break
    except RegistryUnavailableError as exc:
        last_registry_error = str(exc)
    except RegistryIntegrityError as exc:
        raise SystemExit(f"ERROR: {exc}") from None
if remote is None:
    raise SystemExit(f"ERROR: cannot verify PyPI before upload: {last_registry_error}")

missing = [path for path in artifacts if path.name not in remote]
if not missing:
    print(f"SuperTable {version} is already published with the exact artifact hashes.")
    raise SystemExit(0)

for path in artifacts:
    if path.name in remote:
        print(f"Already verified on PyPI; skipping {path.name}")

flags = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_NONBLOCK", 0)
)
try:
    descriptor = os.open(token_path, flags)
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
    token = payload.decode("utf-8")
except UnicodeDecodeError:
    raise SystemExit("ERROR: token file is not valid UTF-8") from None
if token.endswith("\n"):
    token = token[:-1]
if not token or "\n" in token or token != token.strip():
    raise SystemExit("ERROR: token file must contain exactly one unpadded line")
assignment = re.fullmatch(
    r"(?:export\s+)?PYPI_TOKEN=(?:\"(pypi-[A-Za-z0-9_-]+)\"|'(pypi-[A-Za-z0-9_-]+)'|(pypi-[A-Za-z0-9_-]+))",
    token,
)
if assignment:
    token = next(part for part in assignment.groups() if part is not None)
if not re.fullmatch(r"pypi-[A-Za-z0-9_-]+", token):
    raise SystemExit("ERROR: token file has an invalid PyPI token format")

environment = os.environ.copy()
network_environment = {
    "ALL_PROXY",
    "CURL_CA_BUNDLE",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "SSLKEYLOGFILE",
    "LD_AUDIT",
    "LD_LIBRARY_PATH",
    "LD_PRELOAD",
}
for name in tuple(environment):
    normalized = name.upper()
    if (
        normalized.startswith("PYTHON")
        or normalized.startswith("TWINE_")
        or normalized.startswith("DYLD_")
        or normalized in network_environment
    ):
        environment.pop(name, None)
environment.update(
    {
        "TWINE_USERNAME": "__token__",
        "TWINE_PASSWORD": token,
        "TWINE_NON_INTERACTIVE": "1",
    }
)
try:
    result = subprocess.run(
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
            *(str(path) for path in missing),
        ],
        check=False,
        env=environment,
    )
finally:
    environment["TWINE_PASSWORD"] = ""
    token = ""
    payload = b""

if result.returncode:
    print(
        f"WARN: Twine exited {result.returncode}; checking whether PyPI accepted the files.",
        file=sys.stderr,
    )

verified = False
last_remote = remote
last_registry_error = None
post_fetch_succeeded = False
for delay in (0, 1, 2, 3, 5, 8, 10, 10):
    if delay:
        time.sleep(delay)
    try:
        last_remote = fetch_remote()
        post_fetch_succeeded = True
    except RegistryUnavailableError as exc:
        last_registry_error = str(exc)
        continue
    except RegistryIntegrityError as exc:
        raise SystemExit(f"ERROR: {exc}") from None
    if last_remote == expected:
        verified = True
        break

if not verified:
    if not post_fetch_succeeded and last_registry_error:
        details = "registry unavailable: " + last_registry_error
    else:
        missing_names = sorted(set(expected) - set(last_remote))
        details = ", ".join(missing_names) or "unknown registry state"
    raise SystemExit(
        "ERROR: PyPI does not yet contain the exact release; missing/unverified: "
        + details
        + ". Rerun ./publish.sh safely."
    )
if result.returncode:
    print("WARN: PyPI contains the exact release despite Twine's nonzero exit.", file=sys.stderr)
print("PyPI filename and SHA-256 verification passed.")
PY

echo "Verified SuperTable ${VERSION}: https://pypi.org/project/supertable/${VERSION}/"
