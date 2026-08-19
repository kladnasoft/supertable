# Cross-version random-write benchmark

`benchmark_random_write_trace.py` is a version-neutral worker. Mount the exact
same script into each container and select the code under test with either its
installed wheel or `--package-root`. It uses a deterministic append/upsert/
delete trace, hermetic LocalStorage, and Lua-capable fakeredis. The final state
is independently reconstructed from the trace and checked through DataReader
with forced DuckDB using:

```sql
SELECT COUNT(*), COUNT(value), SUM(value), AVG(value), MIN(value), MAX(value)
FROM records
```

It also reads ordered `(id, value, category)` records and gates on a canonical
SHA-256 digest. Therefore two versions agreeing on the same wrong result still
fail independently against the trace oracle.

Each write records wall and process CPU time, sampled CPU-core use, RSS
min/mean/max, process I/O, rusage faults/context switches/block I/O, cgroup-v2
CPU/memory/I/O/pressure counters, storage growth, the DataWriter return value,
and DataWriter's own production monitoring payload (`timings` and `counts`).

Run each version in a fresh hard-limited container. The storage/artifact mount
must be writable; the source mount may be read-only. Pin the image by digest in
the actual campaign:

```bash
docker run --rm --memory=4g --memory-swap=4g --cpus=4 --cpuset-cpus=0-3 \
  -v "$PWD:/src:ro" -v "/absolute/artifacts/old:/artifacts" IMAGE_OLD \
  python /src/supertable/benchmarks/benchmark_random_write_trace.py run \
  --package-root /checkout --work-root /artifacts/work \
  --output /artifacts/report.json --label 426e94b --revision 426e94b

docker run --rm --memory=4g --memory-swap=4g --cpus=4 --cpuset-cpus=0-3 \
  -v "$PWD:/src:ro" -v "/absolute/artifacts/new:/artifacts" IMAGE_NEW \
  python /src/supertable/benchmarks/benchmark_random_write_trace.py run \
  --package-root /checkout --work-root /artifacts/work \
  --output /artifacts/report.json --label HEAD --revision HEAD
```

Use a new empty `--work-root` for every sample. Multiple repetitions should be
run in fresh containers so page cache, DuckDB singletons, fakeredis, and
high-water RSS do not leak between samples. Compare two reports on the host:

```bash
python supertable/benchmarks/benchmark_random_write_trace.py compare \
  --baseline /absolute/artifacts/old/report.json \
  --candidate /absolute/artifacts/new/report.json \
  --output /absolute/artifacts/comparison.json
```

The compare command exits `3` when either version misses the independent
oracle, trace digests differ, or final record digests differ. Exit `0` means
the correctness gate passed; timing ratios must still be interpreted across
enough fresh-container repetitions to control noise.
