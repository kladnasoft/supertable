"""Synthetic webshop dataset demo.

Three runnable entry points::

    python -m supertable.demo.webshop.generate    # one-shot historical generation
    python -m supertable.demo.webshop.load        # load generated parquet into SuperTable
    python -m supertable.demo.webshop.topup       # continuous incremental top-up

Console-script aliases shipped with the package::

    supertable-demo-webshop-generate
    supertable-demo-webshop-load
    supertable-demo-webshop-topup

Configuration lives in ``supertable.demo.webshop.defaults``.
"""
