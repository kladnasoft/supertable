"""Private isolated-process entry point for the IslandDB benchmark."""

from __future__ import annotations

import sys

from .runner import worker_main


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: python -m supertable.engine.benchmarks._worker REQUEST RESPONSE")
        return 2
    return worker_main(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    raise SystemExit(main())
