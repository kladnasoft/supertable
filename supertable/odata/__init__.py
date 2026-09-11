# route: supertable.odata
"""Support for Core's OData service. Not an OData implementation.

The service owns the protocol — EDM types, entity identities, $metadata,
response serialisation, and the continuation position it hands to clients. This
package holds only what the service cannot do for itself:

    row_identity   proves a snapshot's __rowid__ is safe to use as an entity
                   key, so the service can fail closed instead of serving keys
                   that mean different rows tomorrow
    policy         fingerprints role policy, share filters and column masks, so
                   the service can detect a policy change mid-page
    stream         query_odata_sql_stream — one bounded, cancellable, resumable
                   Arrow read

Nothing here parses an OData URL or emits an OData document. If something in
this package starts to look like protocol, it belongs in the service instead.
"""

from supertable.odata.policy import (
    fingerprint_views,
    query_sql_policy_fingerprint,
)
from supertable.odata.row_identity import (
    IdentityVerdict,
    ROWID_COLUMN,
    WATERMARK_KEY,
    identity_column_present,
    next_watermark,
    snapshot_live_rows,
    snapshot_watermark,
    verify_stable_identity,
)
from supertable.odata.stream import (
    ODataPolicyChanged,
    ODataStream,
    SERVICE_ROWID_ALIAS,
    keyset_predicate,
    query_odata_sql_stream,
)

__all__ = [
    "fingerprint_views", "query_sql_policy_fingerprint",
    "IdentityVerdict", "ROWID_COLUMN", "WATERMARK_KEY",
    "identity_column_present", "next_watermark", "snapshot_live_rows",
    "snapshot_watermark", "verify_stable_identity",
    "ODataPolicyChanged", "ODataStream", "SERVICE_ROWID_ALIAS",
    "keyset_predicate", "query_odata_sql_stream",
]
