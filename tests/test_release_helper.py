from __future__ import annotations

import ast
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
from types import SimpleNamespace
import urllib.error
import urllib.request

import pytest


_ROOT = Path(__file__).resolve().parents[1]
_FAKE_PYPI_TOKEN = "pypi" + "-test_token"


def _run(*args: str, cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _release_checkout(tmp_path: Path, *, init_version: str = "2.5.8") -> Path:
    checkout = tmp_path / "checkout"
    (checkout / "supertable").mkdir(parents=True)
    for name in ("push-pypi.sh", "pyproject.toml", "setup.py"):
        shutil.copy2(_ROOT / name, checkout / name)
    (checkout / "supertable" / "__init__.py").write_text(
        f'__version__ = "{init_version}"\n',
        encoding="utf-8",
    )
    _run("git", "init", "-q", cwd=checkout)
    _run("git", "config", "user.name", "Release Test", cwd=checkout)
    _run(
        "git", "config", "user.email", "release-test@example.invalid",
        cwd=checkout,
    )
    _run("git", "add", ".", cwd=checkout)
    _run(
        "git", "-c", "commit.gpgsign=false", "commit", "-qm", "release source",
        cwd=checkout,
    )
    return checkout


def _invoke_release_helper(checkout: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["SUPERTABLE_RELEASE_PYTHON"] = sys.executable
    return subprocess.run(
        ("bash", "push-pypi.sh", "--push"),
        cwd=checkout,
        env=environment,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _write_publish_python_wrapper(path: Path) -> None:
    project_interpreter = _ROOT / ".venv" / "bin" / "python"
    interpreter_path = (
        project_interpreter
        if project_interpreter.is_file()
        else Path(sys.executable)
    )
    # Preserve a venv launcher path instead of resolving its Python symlink;
    # resolving it would bypass pyvenv.cfg and silently use the base runtime.
    interpreter = shlex.quote(str(interpreter_path.absolute()))
    path.write_text(
        "#!/usr/bin/env bash\n"
        f'exec {interpreter} "$@"\n',
        encoding="utf-8",
    )
    path.chmod(0o700)


def test_publish_python_wrapper_falls_back_without_project_venv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys.modules[__name__], "_ROOT", tmp_path / "source")
    wrapper = tmp_path / "python"

    _write_publish_python_wrapper(wrapper)
    result = _run(
        str(wrapper), "-I", "-c", "import sys; print(sys.version_info[:2])",
        cwd=tmp_path,
        check=False,
    )

    assert result.returncode == 0
    assert result.stderr == ""


def _uploader_block() -> str:
    script = (_ROOT / "publish.sh").read_text(encoding="utf-8")
    blocks = re.findall(r"<<'PY'\n(.*?)\nPY", script, flags=re.DOTALL)
    return next(block for block in blocks if "class RegistryIntegrityError" in block)


class _RegistryResponse:
    def __init__(self, document: dict[str, object]) -> None:
        self._payload = json.dumps(document).encode("utf-8")

    def __enter__(self) -> _RegistryResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, limit: int) -> bytes:
        return self._payload[:limit]


def _registry_document(
    version: str,
    artifacts: list[tuple[Path, str]],
) -> dict[str, object]:
    return {
        "info": {"name": "supertable", "version": version},
        "urls": [
            {
                "filename": path.name,
                "packagetype": package_type,
                "digests": {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
                "yanked": False,
            }
            for path, package_type in artifacts
        ],
    }


def _fake_artifacts(tmp_path: Path, version: str) -> tuple[Path, Path]:
    wheel = tmp_path / f"supertable-{version}-py3-none-any.whl"
    sdist = tmp_path / f"supertable-{version}.tar.gz"
    wheel.write_bytes(b"wheel payload")
    sdist.write_bytes(b"sdist payload")
    return wheel, sdist


def test_release_helper_accepts_metadata_free_setup_shim(tmp_path: Path) -> None:
    checkout = _release_checkout(tmp_path)

    result = _invoke_release_helper(checkout)

    assert result.returncode == 6
    assert "--push requires the existing signed local tag v2.5.8" in result.stderr
    assert "version mismatch" not in result.stderr
    assert "setup.py" not in result.stderr


def test_release_helper_rejects_init_version_mismatch(tmp_path: Path) -> None:
    checkout = _release_checkout(tmp_path, init_version="9.9.9")

    result = _invoke_release_helper(checkout)

    assert result.returncode == 1
    assert "version mismatch" in result.stderr
    assert "pyproject=2.5.8" in result.stderr
    assert "__init__=['9.9.9']" in result.stderr


def test_token_publish_helper_keeps_the_short_release_path_safe() -> None:
    script = (_ROOT / "publish.sh").read_text(encoding="utf-8")

    assert 'TOKEN_FILE="${SCRIPT_DIR}/TOKEN"' in script
    assert 'PUBLISH_PYTHON="${SCRIPT_DIR}/.venv/bin/python"' in script
    assert "command -v python" not in script
    assert "git status --porcelain=v1 --untracked-files=all" in script
    assert "git ls-remote --heads origin refs/heads/master" in script
    assert "GIT_NO_REPLACE_OBJECTS=1" in script
    assert 'CURRENT_HEAD="$(git rev-parse HEAD)"' in script
    assert "release checkout changed while artifacts were being validated" in script
    assert "python -m pytest" not in script
    assert "-m build --no-isolation" in script
    assert "git archive --format=tar" in script
    assert "Canonical sdist timestamp" in script
    assert 'ARTIFACT_CACHE_ROOT="${SCRIPT_DIR}/dist/publish-cache"' in script
    assert 'CACHE_MANIFEST="${ARTIFACT_CACHE_DIR}/manifest.json"' in script
    assert "cached artifact hashes do not match the completion manifest" in script
    assert "Retained the exact validated artifacts for safe retry" in script
    assert "-m twine check" in script
    assert "Artifact metadata: supertable" in script
    assert "--python \"${SMOKE_PYTHON}\" install" in script
    assert "Isolated wheel import" in script
    assert "https://pypi.org/pypi/supertable/" in script
    assert "O_NOFOLLOW" in script
    assert "O_NONBLOCK" in script
    assert "stat.S_IMODE(info.st_mode) != 0o600" in script
    assert '"SSLKEYLOGFILE"' in script
    assert '"LD_PRELOAD"' in script
    assert 'normalized.startswith("DYLD_")' in script
    assert 'normalized.startswith("TWINE_")' in script
    assert '"TWINE_USERNAME": "__token__"' in script
    assert '"TWINE_PASSWORD": token' in script
    assert "https://upload.pypi.org/legacy/" in script
    assert '"twine",\n            "upload"' in script


def test_token_publish_helper_rejects_unknown_arguments() -> None:
    result = _run("bash", "publish.sh", "--unknown", cwd=_ROOT, check=False)

    assert result.returncode == 2
    assert "unknown argument: --unknown" in result.stderr


def test_token_publish_helper_embedded_python_is_valid() -> None:
    script = (_ROOT / "publish.sh").read_text(encoding="utf-8")
    python_blocks = re.findall(r"<<'PY'\n(.*?)\nPY", script, flags=re.DOTALL)

    assert len(python_blocks) == 8
    for block in python_blocks:
        ast.parse(block)


def test_token_publish_helper_canonicalizes_sdist_timestamps(tmp_path: Path) -> None:
    script = (_ROOT / "publish.sh").read_text(encoding="utf-8")
    blocks = re.findall(r"<<'PY'\n(.*?)\nPY", script, flags=re.DOTALL)
    canonicalizer = next(
        block for block in blocks if "Canonical sdist timestamp" in block
    )
    archives = [tmp_path / "first.tar.gz", tmp_path / "second.tar.gz"]
    for archive_path, timestamp in zip(archives, (1_000, 2_000), strict=True):
        with archive_path.open("wb") as raw:
            with gzip.GzipFile(
                filename="source.tar",
                mode="wb",
                fileobj=raw,
                mtime=timestamp,
            ) as compressed:
                with tarfile.open(fileobj=compressed, mode="w") as archive:
                    member = tarfile.TarInfo("supertable-9.9.9/payload.txt")
                    member.size = len(b"payload")
                    member.mtime = timestamp
                    archive.addfile(member, io.BytesIO(b"payload"))

    for archive_path in archives:
        result = subprocess.run(
            (sys.executable, "-I", "-", str(archive_path), "1234567890"),
            input=canonicalizer,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert result.returncode == 0, result.stderr

    assert archives[0].read_bytes() == archives[1].read_bytes()
    with tarfile.open(archives[0], mode="r:gz") as archive:
        assert archive.extractfile("supertable-9.9.9/payload.txt").read() == b"payload"


def test_token_publish_helper_cache_manifest_detects_corruption(
    tmp_path: Path,
) -> None:
    script = (_ROOT / "publish.sh").read_text(encoding="utf-8")
    blocks = re.findall(r"<<'PY'\n(.*?)\nPY", script, flags=re.DOTALL)
    validator = next(
        block for block in blocks if "cached artifact hashes do not match" in block
    )
    writer = next(block for block in blocks if "uuid.uuid4" in block)
    version = "9.9.9"
    commit = "a" * 40
    cache = tmp_path / "cache"
    cache.mkdir(mode=0o700)
    wheel, sdist = _fake_artifacts(cache, version)
    wheel.chmod(0o600)
    sdist.chmod(0o600)
    manifest = cache / "manifest.json"

    written = subprocess.run(
        (
            sys.executable,
            "-I",
            "-",
            str(manifest),
            str(wheel),
            str(sdist),
            version,
            commit,
        ),
        input=writer,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert written.returncode == 0, written.stderr
    validate_args = (
        sys.executable,
        "-I",
        "-",
        str(cache),
        str(manifest),
        str(wheel),
        str(sdist),
        version,
        commit,
    )
    valid = subprocess.run(
        validate_args,
        input=validator,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert valid.returncode == 0, valid.stderr

    wheel.write_bytes(b"corrupted wheel")
    invalid = subprocess.run(
        validate_args,
        input=validator,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert invalid.returncode != 0
    assert "cached artifact hashes do not match" in invalid.stderr


def test_token_publish_helper_accepts_an_exact_existing_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = "9.9.9"
    wheel, sdist = _fake_artifacts(tmp_path, version)
    document = _registry_document(
        version,
        [(wheel, "bdist_wheel"), (sdist, "sdist")],
    )
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _RegistryResponse(document),
    )

    def forbidden_upload(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Twine must not run for an exact existing release")

    monkeypatch.setattr(subprocess, "run", forbidden_upload)
    monkeypatch.setattr(
        sys,
        "argv",
        ["publish", str(tmp_path / "missing-token"), version, str(wheel), str(sdist)],
    )

    with pytest.raises(SystemExit) as stopped:
        exec(compile(_uploader_block(), "publish.sh:uploader", "exec"), {})

    assert stopped.value.code == 0


@pytest.mark.parametrize("twine_returncode", [0, 1])
def test_token_publish_helper_resumes_only_the_missing_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    twine_returncode: int,
) -> None:
    version = "9.9.9"
    wheel, sdist = _fake_artifacts(tmp_path, version)
    partial = _registry_document(version, [(wheel, "bdist_wheel")])
    complete = _registry_document(
        version,
        [(wheel, "bdist_wheel"), (sdist, "sdist")],
    )
    responses = iter((partial, complete))
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _RegistryResponse(next(responses)),
    )
    token_path = tmp_path / "TOKEN"
    token_path.write_text(f"{_FAKE_PYPI_TOKEN}\n", encoding="utf-8")
    token_path.chmod(0o600)
    invocations: list[tuple[list[str], dict[str, object]]] = []

    def capture_upload(
        command: list[str],
        **kwargs: object,
    ) -> SimpleNamespace:
        captured = dict(kwargs)
        captured["env"] = dict(captured["env"])
        invocations.append((command, captured))
        return SimpleNamespace(returncode=twine_returncode)

    monkeypatch.setattr(subprocess, "run", capture_upload)
    monkeypatch.setattr(
        sys,
        "argv",
        ["publish", str(token_path), version, str(wheel), str(sdist)],
    )

    exec(compile(_uploader_block(), "publish.sh:uploader", "exec"), {})

    assert len(invocations) == 1
    command, options = invocations[0]
    assert str(sdist) in command
    assert str(wheel) not in command
    assert _FAKE_PYPI_TOKEN not in command
    assert options["env"]["TWINE_PASSWORD"] == _FAKE_PYPI_TOKEN


def test_token_publish_helper_uploads_both_files_for_a_new_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = "9.9.9"
    wheel, sdist = _fake_artifacts(tmp_path, version)
    complete = _registry_document(
        version,
        [(wheel, "bdist_wheel"), (sdist, "sdist")],
    )
    responses: list[dict[str, object] | None] = [None, complete]

    def fake_urlopen(*_args: object, **_kwargs: object) -> _RegistryResponse:
        document = responses.pop(0)
        if document is None:
            raise urllib.error.HTTPError(
                "https://pypi.org/",
                404,
                "Not Found",
                {},
                None,
            )
        return _RegistryResponse(document)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    token_path = tmp_path / "TOKEN"
    token_path.write_text(f"{_FAKE_PYPI_TOKEN}\n", encoding="utf-8")
    token_path.chmod(0o600)
    commands: list[list[str]] = []

    def capture_upload(command: list[str], **_kwargs: object) -> SimpleNamespace:
        commands.append(command)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", capture_upload)
    monkeypatch.setattr(
        sys,
        "argv",
        ["publish", str(token_path), version, str(wheel), str(sdist)],
    )

    exec(compile(_uploader_block(), "publish.sh:uploader", "exec"), {})

    assert len(commands) == 1
    assert str(wheel) in commands[0]
    assert str(sdist) in commands[0]


def test_token_publish_helper_refuses_a_remote_hash_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = "9.9.9"
    wheel, sdist = _fake_artifacts(tmp_path, version)
    document = _registry_document(version, [(wheel, "bdist_wheel")])
    document["urls"][0]["digests"]["sha256"] = "0" * 64
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _RegistryResponse(document),
    )

    def forbidden_upload(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Twine must not run after a hash mismatch")

    monkeypatch.setattr(subprocess, "run", forbidden_upload)
    monkeypatch.setattr(
        sys,
        "argv",
        ["publish", str(tmp_path / "missing-token"), version, str(wheel), str(sdist)],
    )

    with pytest.raises(SystemExit, match="PyPI hash differs"):
        exec(compile(_uploader_block(), "publish.sh:uploader", "exec"), {})


def test_token_publish_helper_requires_the_repository_venv(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    shutil.copy2(_ROOT / "publish.sh", checkout / "publish.sh")

    result = _run("bash", "publish.sh", cwd=checkout, check=False)

    assert result.returncode == 3
    assert ".venv/bin/python is unavailable" in result.stderr


def test_token_publish_helper_rejects_unpinned_tooling(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    (checkout / ".venv" / "bin").mkdir(parents=True)
    shutil.copy2(_ROOT / "publish.sh", checkout / "publish.sh")
    requirements = (_ROOT / "requirements-dev.txt").read_text(encoding="utf-8")
    (checkout / "requirements-dev.txt").write_text(
        requirements.replace("build==1.5.0", "build==0.0.0"),
        encoding="utf-8",
    )
    _write_publish_python_wrapper(checkout / ".venv" / "bin" / "python")

    result = _run("bash", "publish.sh", cwd=checkout, check=False)

    assert result.returncode != 0
    assert "build=1.5.0 (required 0.0.0)" in result.stderr
    assert "Building SuperTable" not in result.stdout
    assert "Publishing SuperTable" not in result.stdout


def test_token_publish_helper_rejects_a_dirty_checkout(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    (checkout / ".venv" / "bin").mkdir(parents=True)
    (checkout / "supertable").mkdir()
    shutil.copy2(_ROOT / "publish.sh", checkout / "publish.sh")
    shutil.copy2(_ROOT / "requirements-dev.txt", checkout / "requirements-dev.txt")
    _write_publish_python_wrapper(checkout / ".venv" / "bin" / "python")
    (checkout / ".gitignore").write_text(".venv/\nTOKEN\n", encoding="utf-8")
    (checkout / "pyproject.toml").write_text(
        '[project]\nname = "supertable"\nversion = "9.9.9"\n',
        encoding="utf-8",
    )
    (checkout / "supertable" / "__init__.py").write_text(
        '__version__ = "9.9.9"\n',
        encoding="utf-8",
    )
    _run("git", "init", "-q", "-b", "master", cwd=checkout)
    _run("git", "config", "user.name", "Release Test", cwd=checkout)
    _run(
        "git", "config", "user.email", "release-test@example.invalid",
        cwd=checkout,
    )
    _run("git", "add", ".", cwd=checkout)
    _run(
        "git", "-c", "commit.gpgsign=false", "commit", "-qm", "release source",
        cwd=checkout,
    )
    (checkout / "dirty.txt").write_text("dirty\n", encoding="utf-8")

    result = _run("bash", "publish.sh", cwd=checkout, check=False)

    assert result.returncode == 4
    assert "publish checkout is not clean" in result.stderr
    assert "dirty.txt" in result.stderr
    assert "Building SuperTable" not in result.stdout
    assert "Publishing SuperTable" not in result.stdout
