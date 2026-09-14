#!/usr/bin/env bash
#
# SuperTable correctness suite.
#
#   ./test_suite/test_suite.sh --all      # write + read (default)
#   ./test_suite/test_suite.sh --write    # write + tombstone correctness
#   ./test_suite/test_suite.sh --read     # SQL read correctness
#
# Extra options are forwarded:
#   --seed N            seed the randomized write workload (reproduce a failure)
#   --transactions N    change the transaction count (default 120)
#   --keep              leave the workspace on disk for inspection
#   -x, -k EXPR, -v     passed through to pytest
#
# Self-contained: starts its own throwaway Redis container and writes to a
# throwaway directory, so it never touches a developer's or a deployment's
# state and can be run at any time, including while other work is in progress.
# Requires Docker and the project's Python environment.
#
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(dirname "$here")"

if ! command -v docker >/dev/null 2>&1; then
    echo "error: this suite needs Docker to run an isolated Redis." >&2
    exit 2
fi

if ! docker info >/dev/null 2>&1; then
    echo "error: Docker is installed but not responding; is the daemon running?" >&2
    exit 2
fi

# Run from the repo root so `python3 -m test_suite.runtime` resolves, whichever
# directory the script was invoked from.
cd "$repo"
exec python3 -m test_suite.runtime "$@"
