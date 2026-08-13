# Performance measurements

These are bounded measurements of named changes, not forecasts for a full dump
or different hardware. They also do not approve a production import: both
batch implementations still require release and cross-language validation
against canonical `open-discogs-model` v0.3.1.

## Results at a glance

| Change | Measured outcome | Scope |
| --- | --- | --- |
| Reference ID cache | 25.4× lower median time; 99.88% fewer allocated bytes | 1,000,000 sequential IDs |
| Durable import contract | Higher fixed latency and allocation | 12-record PostgreSQL fixture |
| Successful-manifest preflight | 95.4% lower p50; avoided 64 MiB file I/O | Cached sparse file |
| Historical Release Master lock order | Prevented deadlock in 10/10 concurrent regression runs | Superseded per-chunk path; 4 writers × 4 overlapping Masters |
| Historical Release Master lock candidates | 161 s observed maximum to 158.144 ms; about 1,018× faster | Superseded per-chunk path; one real 5,000-Release production chunk |
| Format quantity parser | 89.3–95.0% lower median time; up to 100% fewer allocations | Typical and 52-digit values |

Do not compare rows across these harnesses. Their inputs, paths, and units are
different.

## Current Release backlink finalization

Release chunks no longer query or update `master.main_release_id`. One fixed
set-based reconciliation runs after every Release chunk has committed, and
entity completion follows only after that transaction succeeds. PostgreSQL
contract tests cover assignment, movement, clearing, deterministic winner
selection, rollback, idempotent retry, and skipping already committed chunks.

The controlled full-dump before/after latency, throughput, memory, query-plan,
lock, and WAL measurements belong to the final performance gate and are not yet
reported here.

## Historical Release Master lock order

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

## Historical Release Master lock candidates

The first production retry exposed a separate problem in the ordered lock
query. A predicate combining target IDs, current main-release IDs, and an
`EXISTS` branch with `OR` made PostgreSQL scan all 2,579,897 Master rows for
each Release worker. Four concurrent backends reached 1.20--1.29 GiB PSS each;
three waited in a transaction-lock chain while the running query reached 161
seconds. PostgreSQL cgroup memory reached 12.7 GiB, including about 5.0 GiB
anonymous memory and 7.6 GiB file cache.

The production host had 8 vCPUs, 15.62 GiB RAM, rotational PostgreSQL storage,
PostgreSQL 17.7, `chunk-size=5000`, and `max-workers=4`. The replacement first
unions candidate Master IDs from the primary key,
`master.main_release_id`, and `release_item.id`, then joins and locks only those
Master rows in ascending order. On the same production database, a real
5,000-Release chunk covering IDs 840001--845000 produced 2,275 candidate
Masters and completed `EXPLAIN (ANALYZE, BUFFERS, WAL)` in 158.144 ms. The plan
used indexed primary-key lookups, with 1.270 ms planning time, 6,832 shared
buffer hits, 2,268 shared buffer reads, 7.558 ms read time, and no full Master
scan. Compared with the observed 161-second query, execution latency fell about
99.90%, or 1,018×.

This is a bounded production diagnosis, not a controlled latency distribution:
the old query was stopped to protect the shared database, so p50/p95/p99 and a
same-run before/after RSS comparison are unavailable. The after measurement
ran once inside a rolled-back transaction after refreshing planner statistics
and applying production PostgreSQL limits. Full-import throughput and steady
state RSS remain to be measured during the next import.

Both historical sections describe the removed per-chunk backlink path. Their
numbers remain as incident evidence and are not measurements of the current
post-chunk finalizer.

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

## Release format quantity parser

The release dump contains 19,810,850 format rows, and release `6662697` has a
quantity beyond signed 32-bit storage. Parsing each value with an
arbitrary-precision integer was compared with the bounded-memory ASCII decimal
normalizer now shared semantically by Go and Java. The Go microbenchmark ran on
an Apple M2 Pro, five samples per path; the table reports medians.

| Input | Metric | Big-integer baseline | Linear parser | Change |
| --- | --- | ---: | ---: | ---: |
| `0002` | time/op | 152.5 ns | 16.36 ns | 89.3% lower; 9.3× faster |
| `0002` | bytes; allocations/op | 48 B; 4 | 4 B; 1 | 91.7%; 75.0% lower |
| 52-digit dump value | time/op | 587.1 ns | 29.58 ns | 95.0% lower; 19.8× faster |
| 52-digit dump value | bytes; allocations/op | 280 B; 6 | 0 B; 0 | eliminated |

This isolates quantity parsing, not complete format transformation or database
throughput. Java uses the same digit scan, zero trimming, and lexical int32
boundary, but these Go timings are not presented as JVM measurements.

## Reproduce

```shell
go test ./src/batch -run '^$' \
  -bench '^BenchmarkReleaseFormatQuantityParsing$' -benchmem -count=5
```

The quantity parser results above use the median of these five samples.

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
