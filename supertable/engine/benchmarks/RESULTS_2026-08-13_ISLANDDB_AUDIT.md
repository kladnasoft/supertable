# IslandDB adversarial read-path audit

## Result

The audit removed the dominant warm-planning overhead, sealed every pruning
authority to immutable source identity, made repeated range reads avoid
unnecessary checksum work, and replaced raw LRU range eviction with aged,
frequency/size/refill-cost retention. On the exact 10-GiB selective workload,
IslandDB is now 54.5% faster process-cold and 48.5% faster at the seven-run warm
median than DuckDB. Exact result parity is blocking.

## Implemented correctness boundaries

- Each per-resource stats subset is pinned by Parquet footer SHA-256, exact row
  count, and a canonical typed stats digest. Missing, legacy, substituted,
  truncated, duplicate-slot, or mismatched stats fail open to a wider scan and
  can never prove data absent.
- Remote writes record stable object size plus provider version/ETag/checksum
  identity when available. IslandDB passes this seal to conditional range reads,
  avoiding repeated remote `HEAD` calls without trusting path identity alone.
- Stats and deletion-vector caches are bounded and scoped by organization,
  storage route, and authorization context. Deletion vectors validate schema,
  cardinality, digest, and source membership once; warm reads reuse the seal.
- Cached source-rowid proofs bind the exact DV rowid set to immutable object
  identity. Identity changes or unsealed legacy objects re-run the physical
  uniqueness proof.
- Local scan descriptors are immutable, byte/entry bounded, singleflight, and
  revalidated by path/inode/size/mtime/ctime. Any change discards the warm plan.
- Range hits use `O_NOFOLLOW`, same-descriptor pre/post stat checks, sealed
  manifests, and a bounded process proof cache. A new process hashes a cached
  interval once; unchanged hits skip repeated SHA-256.
- Range capacity is enforced against conservative allocated filesystem bytes,
  including manifests, access records, locks, reservations, directories, and
  filesystem block rounding. Admission rechecks the authoritative footprint
  after publication, so metadata or tiny-range amplification cannot cross the
  configured cap.
- Admission/prune recovery removes crash partials, malformed reservations, and
  torn commits while preserving actively locked entries. Eviction removes the
  complete chunk namespace, and the process interval catalog is bounded by
  both object and interval LRU limits.
- Immutable full-object and range caches now default to no TTL. Capacity or
  corruption rotates content; an explicitly positive TTL remains supported.
- Explicit and streaming range-mode IslandDB consult complete-object cache hits
  without admitting misses and hold eviction leases for the query/stream life.
- Benchmark workers no longer silently force DuckDB to one thread and now record
  the actual DuckDB and Polars execution widths.

## Performance work

- SQL is parsed once and the Island capability/resource plan is prepared once
  per execution.
- A 10,000-file local scan formerly rebuilt 10,000 Arrow fragments/schemas and
  performed repeated realpath/footer work every query. The bounded scan-plan
  cache reduced measured warm planning from 9.44 seconds to 0.28–0.43 seconds;
  total warm execution fell from 11.95 seconds cold-plan state to 2.93–3.16
  seconds, with exact sum `4,999,590,094`.
- Snapshot-pinned object seals remove remote per-file metadata round trips.
- Range eviction uses an aged frequency counter with size and observed refill
  cost, so once-hot entries decay and scan pollution cannot make them immortal.
  Eviction stops as soon as enough capacity is freed.
- A narrow measured local scan gate uses column-parallel decoding for complete
  scans <=32 MiB with four to eight required columns. All other shapes remain
  adaptive.

## Resource-contract correction

Polars and Arrow worker pools are process-global and cannot be resized safely
for one concurrent query. IslandDB now reserves the process pool's full CPU
width at admission rather than pretending a query will use only its estimated
subset. Decoded scan memory remains a conservative route/admission estimate;
it is not falsely documented as a hard per-query allocator cap. Result
collection and query-private spill retain hard byte quotas. Strict per-query
scan memory/CPU isolation would require worker-process/cgroup isolation.

## Remaining architectural limits

These are explicit residual risks, not hidden claims of completion:

- Full-object and range caches still have separate configured capacities; the
  defaults can consume 20 GiB each, plus bounded in-memory stats/DV metadata.
  A single global persistent-cache budget/ledger is still needed.
- Range admission currently scans filesystem manifests, and exact intervals can
  fragment/overlap. A rebuildable transactional index plus canonical blocks or
  row-group/column extents would remove this metadata hot path. Recovery is
  currently admission/prune-driven rather than a background service.
- Arrow reaches remote cached bytes through a Python random-access wrapper,
  which copies buffers. A native filesystem adapter or local Range-capable
  proxy would let both IslandDB and DuckDB share the same sealed block store.
- DuckDB can reuse complete-object hits and shared file pruning, but cannot yet
  consume IslandDB's partial range cache. A DuckDB filesystem extension/proxy is
  the safe route; registering decoded Arrow tables would duplicate memory and
  lose DuckDB pushdown.
- Warm IslandDB RSS on the exact 10-GiB selective query remains 9.48 MiB median
  versus DuckDB's 1.75 MiB, primarily Polars decompression/scan-worker buffers.

## Validation strategy

Correctness tests cover exact DuckDB differential parity, legacy/corrupt/
substituted stats, object-identity changes, local mutations, cache corruption,
concurrent fill/read/eviction, access-record decay, active leases, DV source
rowid integrity, SQL capability fences, resource admission, spill cleanup,
cancellation, and cold/warm benchmark digest stability. On the final tree,
`supertable/engine/tests` passed 543 tests, the sealed stats/object-estimation
set passed 101 tests, and the tombstone-cache set passed 19 tests. `compileall`
and `git diff --check` were also clean.
