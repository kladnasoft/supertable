# Release protection

Production releases use `.github/workflows/release.yml` and PyPI Trusted
Publishing. Configure the repository and PyPI once as follows:

1. Create the GitHub environment `pypi`. Require at least one reviewer,
   disallow self-review and administrator bypass, and restrict deployment to
   tags matching `v*`.
2. In the PyPI `supertable` project, add a Trusted Publisher for this
   repository, workflow `release.yml`, and environment `pypi`. Do not store a
   PyPI API token in GitHub or in the repository.
3. Protect `master`: require the `release-gate` jobs, signed commits, reviewed
   pull requests, conversation resolution, and no force pushes.
4. Protect release tags matching `v*` so only release maintainers can create
   or delete them. Require every release maintainer to register the GPG or SSH
   signing key used by `git tag --sign` with GitHub. The workflow queries the
   exact annotated tag object and refuses publication unless GitHub reports a
   cryptographically valid signature.

The release helper requires a clean, committed version and creates a signed
annotated tag. CI then tests, builds, installs, attests, and uploads artifacts
from that exact tag. PyPI upload is the final job and cannot run if any gate
fails.

## Compatibility token upload

Trusted Publishing remains the preferred production path. Existing maintainers
may use `./push-pypi.sh --upload-token --token-file PATH` (or pass the already
committed exact version, such as `./push-pypi.sh 2.5.0`) while that account-level
configuration is unavailable. This mode never rebuilds locally: after all local
gates pass, it requires the exact `master` commit's successful release-gate run,
downloads its digest-pinned wheel and sdist artifact, revalidates both packages,
and uploads only those files. It neither creates nor pushes a Git tag.

The token file must be an owner-controlled, non-symlink regular file with mode
`0600`; it may contain either a bare PyPI token or one `PYPI_TOKEN=` assignment.
Without `--token-file`, the one-time 2.5.0 compatibility path uses
`../../TOKEN`. Never commit the file. Rotate the credential after a compatibility
upload and complete the Trusted Publisher setup above before the next release.

## Mypy debt baseline

The type gate always runs against the complete `supertable` production package;
only test and benchmark directories are excluded. Existing type debt is listed
verbatim in `mypy-baseline.json`, together with every inline mypy suppression.
The gate fails if a diagnostic/suppression changes or if the production source
file count changes, so a new module cannot silently fall outside the check.

To propose a baseline update, generate a separate review artifact:

```bash
python check_mypy_baseline.py supertable --print-baseline > mypy-baseline.json.new
```

Review the full diff before replacing the committed baseline. An increased
error or suppression count is release-blocking unless the change has an
explicitly approved remediation plan; never update the baseline merely to make
CI green.
