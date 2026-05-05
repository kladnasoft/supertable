"""Enable Delta Lake + Iceberg mirroring on the SuperTable.

After this runs, every subsequent write also produces Delta and Iceberg
metadata alongside the native Parquet snapshots.
"""
from supertable.super_table import SuperTable
from supertable.mirroring.mirror_formats import MirrorFormats, FormatMirror
from supertable.config.defaults import logger

from supertable.demo.quickstart.defaults import super_name, organization

st = SuperTable(super_name=super_name, organization=organization)

MirrorFormats.set_with_lock(st, [FormatMirror.DELTA, FormatMirror.ICEBERG])

logger.info(f"Mirror formats enabled: {MirrorFormats.get_enabled(st)}")
