# go-open-discogs-batch

`go-open-discogs-batch` imports the public OpenDiscogs monthly dumps into
PostgreSQL. It uses the canonical schema and generated Go models from
[`open-discogs-model`](https://github.com/dsub-io/open-discogs-model), so the Go
and Java importers write the same database contract.

This is an independent DSUB project. It is not affiliated with or endorsed by
Discogs. The Discogs name identifies only the public data source.

## Import behavior and safety

- The selected database schema, canonical migrations, tables, indexes, and dump catalog refresh run automatically.
- Artist, label, master, and release select their newest available dump
  independently unless an exact `--dump-month` is requested.
- Every run records the selected dump dates, SHA-256 checksums, source URIs,
  and stable identifiers as one immutable manifest.
- A successful manifest is admitted and skipped before dump files are downloaded
  or checksummed only while all of its entity dumps are still the current
  checkpoints and no later failed or abandoned run has dirtied those entities.
  `--force` reruns that same manifest without changing the normalized database
  result.
- A failed or abandoned run resumes only when the manifest, processor name and
  version, entity/dump identity, and chunk size all match exactly. A different
  manifest or `--force` starts at zero.
- Each tracked convergence chunk writes its canonical roots, exact relations,
  committed-chunk ledger, and item counter in one PostgreSQL transaction. A
  retry skips only source ranges represented by valid committed chunk rows.
- An older entity dump is rejected unless `--allow-downgrade` is supplied; the
  override is recorded in import history.
- PostgreSQL advisory locks cover both selected entities and their reference
  dependencies. Master locks Artist and Master; Release locks Artist, Label,
  Master, and Release because it also updates `master.main_release_id`.
  Independent sets such as Artist and Label may still proceed together.
- Downloads are retained by default. `--cleanup` deletes only the selected dump
  files after a successful import or successful-manifest skip. Failed imports
  retain their files for retry.

If the upstream catalog refresh fails, the importer tries the catalog already
stored in PostgreSQL. It fails if that catalog cannot satisfy the request.

An exact `--dump-month` uses a complete matching PostgreSQL catalog entry before
contacting Discogs. If any selected entity is missing, dumps from 2021 onward
are resolved with one monthly checksum request; its filenames and SHA-256 values
define all selected download URIs. Older dumps have irregular publication dates,
so they require one annual catalog request and one checksum request. The importer
stores every selected URI and checksum before downloading files. Retries of that
pinned month reuse the stored catalog without another metadata request. A 429,
5xx, timeout, or malformed response fails the refresh without speculative extra
requests. Latest-per-entity selection still refreshes upstream first because the
local database cannot prove that no newer dump exists.

Catalog discovery and file downloads use the official `data.discogs.com` dump
browser. Direct S3 bucket and object URLs are not part of this contract. The
browser catalog exposes rounded display sizes, so new catalog rows store
`size_bytes=0` instead of presenting an estimate as an exact byte count.
Download progress uses the response `Content-Length` when supplied rather than
this catalog field.

### Durability and idempotency boundaries

`SIGINT` and `SIGTERM` cancel download, parsing, and database work. The active
chunk transaction rolls back, while run failure is recorded with a separate
bounded completion context. `SIGKILL`, host loss, or a database disconnect can
leave a run marked `running`; the next process acquires the entity locks, marks
that abandoned run failed, and transfers its valid ledger in one transaction.
If ledger transfer fails, the new run and copied rows roll back together and the
source ledger remains authoritative.

Every canonical and tracked chunk fences against the owning run row before it
can commit. Once an abandoned run is marked failed, a delayed worker can no
longer commit canonical data, progress, or entity completion. If an in-flight
chunk obtains the fence first, abandonment waits for that atomic commit and the
next run transfers the resulting ledger entry.

An entity is complete only when chunk indexes cover the parsed stream without
gaps, overlaps, or out-of-range indexes and both chunk and item totals match.
The whole run becomes successful only after every selected entity is complete.
Failed ledgers are retained until the current successful checkpoints, produced
by the same processor version, supersede every entity/dump pair in the failed
run. Historical success rows alone never authorize pruning. A failed ledger is
also not resumed when a newer successful checkpoint with different dump or
processor identity has overwritten one of its entity ranges. These rules
prevent an Artist-only run from deleting the only valid resume state for a
failed Artist+Release run or mixing ranges from different snapshots.

Relations owned by each imported root are reconciled to the exact set in the
dump: missing relations are deleted, changed mutable values are updated, and
unchanged relation rows retain their identifiers. Root artist, label, master,
and release rows are upserted; roots absent from a later dump are not currently
deleted. The importer assumes official dump root identifiers are unique.
The v1 schema still identifies several relation values with a 32-bit Java hash;
a collision within one root can merge distinct values. The measured, online
migration to collision-resistant identity is tracked in
[`open-discogs-model#43`](https://github.com/dsub-io/open-discogs-model/issues/43).

Atomicity is per chunk, not per monthly snapshot. Permanent full-dump staging is
intentionally avoided because it would duplicate a catalog exceeding 200
million records. Readers can therefore observe already committed chunks while
an import is running. Deployments that require an all-at-once snapshot switch
must put a separate versioned database or replica promotion boundary around the
import.

### Progress observability

When stderr is a terminal, downloads and source reads display an interactive
progress bar on stderr. Line-delimited `byte_progress` JSON records are emitted
on stdout at start, at most once every five seconds, and at completion or
failure. The records include stage, resource, completed and total compressed
bytes, percentage, byte throughput, and elapsed time. Redirecting or piping
stdout through tools such as `tee` therefore remains parseable without removing
the terminal bar. A source-read percentage describes how much of the gzip
stream has been consumed; it does not claim that the same percentage of
PostgreSQL work has committed. Do not merge stderr into stdout when collecting
structured output.

Tracked entity convergence also writes JSON progress records to stdout at
start, at most once every five seconds while chunks finish, and at completion
or failure. `committed_items` comes from `discogs_import_run_dump.processed_items`
and therefore counts only roots whose canonical entity and complete relation
set committed atomically with the chunk ledger. `initial_committed_items` makes
resumed work explicit, `rows_per_second` covers newly committed roots in the
current process, and `last_committed_progress_at` supports stall detection.
Each emitted sample performs one primary-key summary read, bounded to 0.2 reads
per second per active entity plus the start and finish reads; it never scans the
chunk ledger or entity tables.

`committed_percent` is intentionally omitted while the stream total is unknown.
It becomes `100` only after end-of-stream coverage validation records exact item
and chunk totals. Pre-counting every XML root merely to display a speculative
percentage would add a second full gzip/XML scan. There is no synthetic overall
percentage across Artist, Label, Master, and Release because their relation
fan-out and database cost differ substantially.

## Requirements

- PostgreSQL
- Go 1.26 for source builds, or a published binary/container

## Usage

```shell
go-open-discogs-batch \
  --database-url 'postgresql://user:password@localhost:5432/open_discogs' \
  --database-schema open_discogs \
  --entities artist,label,master,release
```

Use `--dump-month=2026-07` to require that exact month. Without it, each selected
entity uses its own latest available dump.

The PostgreSQL database itself must already exist. A normal batch run needs no separate `init` command: it creates `--database-schema` when missing, then creates or migrates the canonical tables inside it. Creating a missing schema requires `CREATE` on the target database. For an existing schema, the batch role requires `USAGE` and `CREATE`, write access to imported tables, and ownership or equivalent DDL authority for migrations. If those privileges are intentionally unavailable, pre-create the schema with a DBA-managed role and grant the batch role the required schema and table privileges before running the importer.

When `--database-schema` / `OPEN_DISCOGS_BATCH_DATABASE_SCHEMA` is omitted, the importer uses `public` and emits a `WARN` on every startup. This is convenient for compatibility but can mix OpenDiscogs objects with unrelated public tables, so a dedicated name such as `open_discogs` is recommended. Schema names use portable PostgreSQL identifiers: 1–63 lowercase letters, digits, or underscores, starting with a letter or underscore.

| Option | Environment variable | Default | Purpose |
| --- | --- | --- | --- |
| `--database-url` | `OPEN_DISCOGS_BATCH_DATABASE_URL` | required | PostgreSQL URI including percent-encoded credentials |
| `--database-schema` | `OPEN_DISCOGS_BATCH_DATABASE_SCHEMA` | `public` | Schema to create or migrate; `public` emits a startup warning |
| `--entities`, `-e` | `OPEN_DISCOGS_BATCH_ENTITIES` | all four | Comma-separated `artist`, `label`, `master`, `release` |
| `--dump-month`, `-m` | `OPEN_DISCOGS_BATCH_DUMP_MONTH` | latest per entity | Exact dump month in `yyyy-MM` form |
| `--data-dir` | `OPEN_DISCOGS_BATCH_DATA_DIR` | `~/.cache/open-discogs-batch` | Download directory |
| `--chunk-size`, `-b` | `OPEN_DISCOGS_BATCH_CHUNK_SIZE` | `5000` | Import chunk size |
| `--max-workers` | `OPEN_DISCOGS_BATCH_MAX_WORKERS` | runtime CPU allocation | Maximum concurrent import workers |
| `--cleanup`, `-c` | `OPEN_DISCOGS_BATCH_CLEANUP` | `false` | Delete downloads after success |
| `--force`, `-f` | `OPEN_DISCOGS_BATCH_FORCE` | `false` | Rerun an already-successful manifest |
| `--allow-downgrade` | `OPEN_DISCOGS_BATCH_ALLOW_DOWNGRADE` | `false` | Permit and audit older entity dumps |
| `--help`, `-h` | — | — | Show help |
| `--version`, `-v` | — | — | Show version |

Command-line options take precedence over environment variables, which take
precedence over defaults. The two importer implementations accept this same
public contract. Configuration files and the former `new`, `update`, `purge`,
`dsn`, `types`, `year`, `month`, and `data` options are no longer supported.

`--max-workers` is the exact upper bound on application-managed concurrent
import workers. When omitted, it resolves to the CPU allocation used by the Go
runtime; no percentage or physical-core heuristic is applied. It is not a hard
CPU quota. Use the container or workload scheduler's CPU limit when the process
itself must not exceed a CPU allocation.

## Large-import resource model

Dump downloads, gzip decompression, and XML parsing are streamed; a dump is
never loaded into memory as one document. Chunk submission blocks when
`max-workers` chunks are active, so in-flight work cannot grow with the total
dump size. The PostgreSQL pool is limited to `max-workers + 1`: one connection
per possible writer plus the import coordinator's advisory-lock connection.

Integer reference IDs use a segmented concurrent bit set. Each occupied range
of 65,536 IDs allocates 8 KiB of bit storage, so dense IDs use one bit each.
Dependencies for a partial import are streamed from PostgreSQL into these sets.
Release-to-master changes use one set-based update per chunk instead of one SQL
statement per release. Insert batches are also capped below PostgreSQL's 65,535
bind-parameter limit even when a larger `--chunk-size` is requested.

Peak working memory is driven by `chunk-size × max-workers × relation fan-out`,
not by the total row count. Release records have the largest fan-out. For a
large production import, set `--max-workers` explicitly to the smaller of the
container CPU allocation and the number of database write connections reserved
for this job, then tune `--chunk-size` separately from measured heap usage and
database latency. Lower `chunk-size` when one expanded relation chunk is too
large; lower `max-workers` when concurrent chunks or PostgreSQL are the
constraint.

### Measured ID-cache improvement

On an Apple M2 Pro, the previous `sync.Map` ID set and the segmented bit set
were compared with the same 1,000,000 sequential IDs using three iterations per
sample and five samples. Median time fell from 234.656 ms to 9.257 ms (`25.4×`
faster), allocated bytes fell from approximately 109.5 MB/op to 132.9 KB/op
(`99.88%` lower), and allocations fell from 2,359,348 to 39 per operation. The
release-to-master update also changes up to 5,000 row-by-row SQL statements at
the default chunk size into one set-based statement. These figures isolate the
measured operations; end-to-end import throughput still depends on dump shape,
PostgreSQL, storage, and runtime limits.

### Measured durable-import cost and skip improvement

The tracked production path was measured before and after the recovery fixes
against commit `2a1ddbd` on the same Apple M2 Pro, PostgreSQL 18.4 Alpine tmpfs
container, four 3-record fixture dumps, `chunk-size=5`, `max-workers=2`, one
import per sample, and 20 samples. Output was suppressed identically. Initial
import latency changed from p50/p95/p99 `48.509/54.527/80.881 ms` to
`73.922/77.397/88.551 ms` (`52.4%/41.9%/9.5%` higher). Median throughput changed
from `0.167` to `0.109 MB/s` (`34.4%` lower), allocated bytes from 2,327,712 to
2,454,276 B/op (`5.4%` higher), and allocations from 43,524 to 44,333/op (`1.9%`
higher).

Forced-repeat latency changed from p50/p95/p99 `38.864/43.826/44.841 ms` to
`72.822/74.057/74.204 ms` (`87.4%/69.0%/65.5%` higher). Median throughput changed
from `0.208` to `0.111 MB/s` (`46.6%` lower), allocated bytes from 2,340,796 to
2,466,576 B/op (`5.4%` higher), and allocations from 44,597 to 45,990/op (`3.1%`
higher). The added cost buys current-checkpoint validation and a database fence
that prevents delayed workers from committing after abandonment. This tiny
12-record fixture exaggerates fixed transaction cost and is not a full-dataset
throughput estimate.

Fresh tracked relation chunks use one SQL statement for the active-run fence,
ledger insert, and summary update. Resumed chunks add one exact-ledger lookup.
The Artist and Label canonical pre-seed phase adds one active-run fence per
chunk because those roots must exist before forward relations are reconciled.

The production no-op path was measured separately with the same successful
manifest and a cached 64 MiB sparse file. Checksum-before-admission latency was
p50/p95/p99 `38.917/50.768/58.719 ms`; admission-before-checksum was
`1.776/2.728/3.038 ms`, a `95.4%/94.6%/94.8%` reduction and `21.9x` median
speedup. Median allocations changed from 40,008 to 6,800 B/op (`83.0%` lower)
and 113 to 106 allocations/op (`6.2%` lower), while 64 MiB of file I/O was
avoided per invocation.

Peak RSS was not reported from these microbenchmarks because it would include
the test process and container lifecycle while the 12-record fixture is too
small to represent production heap pressure. Allocation counts are reported
instead; production sizing still requires a heap/RSS profile from a
representative large dump under the intended `chunk-size` and `max-workers`.

Reproduce the focused measurements with:

```shell
go test ./src/batch -run '^$' \
  -bench '^BenchmarkDurableBatchImport$' -benchtime=1x -count=20 -benchmem
go test ./src/batch -run '^$' \
  -bench '^BenchmarkCompletedManifestPreflight$' -benchtime=1x -count=20 -benchmem
```

## Container

Release images are published from Release Please release commits:

```shell
docker pull ghcr.io/dsub-io/go-open-discogs-batch:latest
docker run --rm \
  --cpus=4 \
  -e OPEN_DISCOGS_BATCH_DATABASE_URL='postgresql://user:password@db:5432/open_discogs' \
  -e OPEN_DISCOGS_BATCH_DATABASE_SCHEMA='open_discogs' \
  -e OPEN_DISCOGS_BATCH_ENTITIES='artist,label,master,release' \
  -e OPEN_DISCOGS_BATCH_DATA_DIR=/data \
  -e OPEN_DISCOGS_BATCH_MAX_WORKERS=4 \
  -v open-discogs-data:/data \
  ghcr.io/dsub-io/go-open-discogs-batch:latest
```

The image runs as a non-root user. Mount a writable volume when downloads must
survive container removal. Setting `OPEN_DISCOGS_BATCH_CLEANUP=true` removes the
selected files only after success.

Versioned binaries are attached to the repository's GitHub Releases.

## Development

```shell
gofmt -w .
go mod tidy
go vet ./...
go test -race -coverprofile=coverage.out -covermode=atomic ./...
go test -run '^$' -bench 'IDSetLoadMillion' -benchmem ./src/cache
```

Pull requests run formatting, module consistency, vet, race detection, unit and
PostgreSQL integration tests, and a 100% statement coverage gate. The separate E2E workflow
uses deterministic fixtures on GitHub-hosted `ubuntu-latest`; it does not depend
on live Discogs availability.

Pull request titles and commits must use Conventional Commits. Release Please
publishes only release-relevant changes; documentation-only changes do not
create a release.

## License

MIT. See [LICENSE](LICENSE). The `state303` attribution must be retained as
required by the license.
