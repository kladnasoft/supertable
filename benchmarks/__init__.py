"""SuperTable performance suites.

Read and write benchmarks that produce comparable, sealed telemetry so one
version's behaviour can be measured against another's.  See ``README.md`` in
this directory, and run ``python -m benchmarks --help``.

Deliberately outside the ``supertable`` package so it is not shipped in the
wheel; ``[tool.setuptools.packages.find]`` only includes ``supertable*``.
"""
