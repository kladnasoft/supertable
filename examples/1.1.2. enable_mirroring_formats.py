from supertable.super_table import SuperTable
from supertable.mirroring.mirror_formats import MirrorFormats, FormatMirror
from supertable.super_table import SuperTable
from examples.defaults import super_name, organization
from supertable.config.defaults import logger

st = SuperTable(super_name=super_name, organization=organization)

# Enable both:
MirrorFormats.set_with_lock(st, [FormatMirror.DELTA, FormatMirror.ICEBERG])

# Or enable one:
MirrorFormats.enable_with_lock(st, "DELTA")

# Disable one:
MirrorFormats.disable_with_lock(st, "ICEBERG")

# Check:
logger.info(f"Mirror formats: {MirrorFormats.get_enabled(st)}")
