"""SuperTable runnable demos.

Two self-contained demos ship with the package:

- ``supertable.demo.quickstart`` — numbered API tutorial. Run the full suite
  with ``python -m supertable.demo.quickstart`` (or the
  ``supertable-demo-quickstart`` console script).
- ``supertable.demo.webshop`` — synthetic webshop dataset generator and
  loader. Entry points:
  ``supertable-demo-webshop-generate``, ``-load``, ``-topup``.

Both require a reachable Redis instance and a configured storage backend; see
the documentation for environment variables.
"""
