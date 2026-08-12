# Performance measurements

These are bounded measurements of named changes, not forecasts for a full dump
or different hardware. They also do not approve a production import: both
batch implementations still require release and cross-language validation
against canonical `open-discogs-model` v0.3.0.

## Results at a glance

| Change | Measured outcome | Scope |
| --- | --- | --- |
| Reference ID cache | 25.4× lower median time; 99.88% fewer allocated bytes | 1,000,000 sequential IDs |
| Durable import contract | Higher fixed latency and allocation | 12-record PostgreSQL fixture |
| Successful-manifest preflight | 95.4% lower p50; avoided 64 MiB file I/O | Cached sparse file |
| Release Master lock order | Prevented deadlock in 10/10 concurrent regression runs | 4 writers × 4 overlapping Masters |

Do not compare rows across these harnesses. Their inputs, paths, and units are
different.

## Release Master lock order

A production-like 2026-08 import with `chunk-size=5000` and `max-workers=4`
exposed a PostgreSQL deadlock after one Release chunk had committed. The server
deadlock graph showed four workers waiting on `UPDATE master AS target`. Before
that failure, Artist committed 10,163,318 roots in 110.797 seconds, Label
2,405,196 in 21.982 seconds, Master 2,579,897 in 82.008 seconds, and Release
5,000 roots before the run failed. Those figures describe the failed run and
are not a successful full-import benchmark.

The regression runs four concurrent Release chunk writers over the same four
Master rows in different input orders. After adding ascending Master row locks,
10/10 runs completed without deadlock. The pre-change production run failed;
the small regression was not run against the old binary, so this is a
correctness result rather than a latency or throughput improvement claim. A
successful full-dump retry is required before reporting end-to-end throughput,
latency distribution, or RSS.

## Reference ID cache

On an Apple M2 Pro, the previous `sync.Map` and segmented bit set were compared
with 1,000,000 sequential IDs, three iterations per sample, and five samples.

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Median time | 234.656 ms | 9.257 ms | 25.4× faster |
| Allocated bytes/op | 109.5 MB | 132.9 KB | 99.88% lower |
| Allocations/op | 2,359,348 | 39 | 99.998% lower |

Release-to-master updates also changed up to 5,000 row-by-row statements at the
default chunk size into one set-based statement. That is a statement-count
fact; no latency improvement was measured for this change.

## Durable import cost

Commit `2a1ddbd` and the recovery implementation were measured on the same
Apple M2 Pro with PostgreSQL 18.4 Alpine on tmpfs, four 3-record fixtures,
`chunk-size=5`, `max-workers=2`, one import per sample, and 20 samples.

| Path | Metric | Before | After | Change |
| --- | --- | ---: | ---: | ---: |
| Initial | p50/p95/p99 | 48.509/54.527/80.881 ms | 73.922/77.397/88.551 ms | +52.4%/+41.9%/+9.5% |
| Initial | median throughput | 0.167 MB/s | 0.109 MB/s | 34.4% lower |
| Initial | allocated bytes/op | 2,327,712 | 2,454,276 | 5.4% higher |
| Forced repeat | p50/p95/p99 | 38.864/43.826/44.841 ms | 72.822/74.057/74.204 ms | +87.4%/+69.0%/+65.5% |
| Forced repeat | median throughput | 0.208 MB/s | 0.111 MB/s | 46.6% lower |
| Forced repeat | allocated bytes/op | 2,340,796 | 2,466,576 | 5.4% higher |

The added fixed cost buys active-run fencing, exact chunk-ledger commits,
coverage validation, and stale-relation reconciliation. A 12-record fixture
exaggerates this cost and cannot represent a full import.

## Successful-manifest preflight

The same successful manifest and a cached 64 MiB sparse file compared checksum
before admission with admission before checksum.

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| p50/p95/p99 | 38.917/50.768/58.719 ms | 1.776/2.728/3.038 ms | 95.4%/94.6%/94.8% lower |
| Median allocations | 40,008 B/op | 6,800 B/op | 83.0% lower |
| Allocations/op | 113 | 106 | 6.2% lower |
| Avoided file I/O | — | 64 MiB/invocation | — |

Peak RSS is not reported: the microbenchmark process includes test and
container lifecycle overhead, while the fixture is too small to represent
production memory.

## Reproduce

```shell
go test ./src/batch -run '^$' \
  -bench '^(BenchmarkBatchImport|BenchmarkDurableBatchImport)$' \
  -benchtime=1x -count=20 -benchmem
go test ./src/batch -run '^$' \
  -bench '^BenchmarkCompletedManifestPreflight$' -benchtime=1x -count=20 -benchmem
go test ./src/cache -run '^$' \
  -bench '^(BenchmarkIDSetLoadMillion|BenchmarkSyncMapIDSetLoadMillion)$' \
  -benchtime=3x -count=5 -benchmem
go test ./src/batch \
  -run '^TestConcurrentReleaseChunksLockOverlappingMastersInOneOrder$' \
  -count=10
```

Each command includes both comparison paths or implementations. The cache
command matches the recorded three iterations and five samples. Exact published
numbers still require the stated Apple M2 Pro and PostgreSQL 18.4 tmpfs
environment, equivalent system load, and the same Go toolchain. Results from
another machine are new measurements, not a reproduction of these tables.

These fixtures do not measure a production-sized dump, cold storage, network
download, or peak process RSS. The preflight benchmark uses a sparse file and
the import benchmark uses 12 records; neither may be extrapolated to the
200-million-row deployment.
