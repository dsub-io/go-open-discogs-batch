# Performance measurements

These measurements isolate named changes. They are not estimates of full-dump
throughput on other hardware. See the [README](../README.md) for sizing guidance
and [Import safety and recovery](import-safety.md) for the guarantees whose cost
is measured here.

## Reference ID cache

On an Apple M2 Pro, the previous `sync.Map` and the segmented bit set were
compared with 1,000,000 sequential IDs, three iterations per sample and five
samples.

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Median time | 234.656 ms | 9.257 ms | 25.4× faster |
| Allocated bytes/op | 109.5 MB | 132.9 KB | 99.88% lower |
| Allocations/op | 2,359,348 | 39 | 99.998% lower |

Release-to-master updates also changed up to 5,000 row-by-row statements at the
default chunk size into one set-based statement.

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

## Successful-manifest skip

The same successful manifest and a cached 64 MiB sparse file compared checksum
before admission with admission before checksum.

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| p50/p95/p99 | 38.917/50.768/58.719 ms | 1.776/2.728/3.038 ms | 95.4%/94.6%/94.8% lower |
| Median allocations | 40,008 B/op | 6,800 B/op | 83.0% lower |
| Allocations/op | 113 | 106 | 6.2% lower |
| Avoided file I/O | — | 64 MiB/invocation | — |

Peak RSS was not reported because the microbenchmark process includes test and
container lifecycle overhead while the fixture is too small to represent
production memory.

## Reproduction

```shell
go test ./src/batch -run '^$' \
  -bench '^BenchmarkDurableBatchImport$' -benchtime=1x -count=20 -benchmem
go test ./src/batch -run '^$' \
  -bench '^BenchmarkCompletedManifestPreflight$' -benchtime=1x -count=20 -benchmem
go test -run '^$' -bench 'IDSetLoadMillion' -benchmem ./src/cache
```
