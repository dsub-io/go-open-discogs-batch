# go-open-discogs-batch

`go-open-discogs-batch` imports the public OpenDiscogs monthly dumps into
PostgreSQL. It uses the canonical schema and generated Go models from
[`open-discogs-model`](https://github.com/dsub-io/open-discogs-model), so the Go
and Java importers write the same database contract.

This is an independent DSUB project. It is not affiliated with or endorsed by
Discogs. The Discogs name identifies only the public data source.

## Import behavior and safety

- Schema migrations and the dump catalog refresh run automatically.
- Artist, label, master, and release select their newest available dump
  independently unless an exact `--dump-month` is requested.
- Every run records the selected dump dates, SHA-256 checksums, source URIs,
  sizes, and stable identifiers as one immutable manifest.
- A manifest that already succeeded is skipped. `--force` reruns that same
  manifest without changing the idempotent database result.
- An older entity dump is rejected unless `--allow-downgrade` is supplied; the
  override is recorded in import history.
- PostgreSQL advisory locks prevent concurrent runs from updating an overlapping
  entity set. Runs with disjoint entity sets may proceed together.
- Downloads are retained by default. `--cleanup` deletes only the selected dump
  files after a successful import or successful-manifest skip. Failed imports
  retain their files for retry.

If the upstream catalog refresh fails, the importer tries the catalog already
stored in PostgreSQL. It fails if that catalog cannot satisfy the request.

## Requirements

- PostgreSQL
- Go 1.26 for source builds, or a published binary/container

## Usage

```shell
go-open-discogs-batch \
  --database-url 'postgresql://user:password@localhost:5432/open_discogs' \
  --entities artist,label,master,release
```

Use `--dump-month=2026-07` to require that exact month. Without it, each selected
entity uses its own latest available dump.

| Option | Environment variable | Default | Purpose |
| --- | --- | --- | --- |
| `--database-url` | `OPEN_DISCOGS_BATCH_DATABASE_URL` | required | PostgreSQL URI including percent-encoded credentials |
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

## Container

Release images are published from Release Please release commits:

```shell
docker pull ghcr.io/dsub-io/go-open-discogs-batch:latest
docker run --rm \
  --cpus=4 \
  -e OPEN_DISCOGS_BATCH_DATABASE_URL='postgresql://user:password@db:5432/open_discogs' \
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
PostgreSQL integration tests, and a coverage gate. The separate E2E workflow
uses deterministic fixtures on GitHub-hosted `ubuntu-latest`; it does not depend
on live Discogs availability.

Pull request titles and commits must use Conventional Commits. Release Please
publishes only release-relevant changes; documentation-only changes do not
create a release.

## License

MIT. See [LICENSE](LICENSE). The `state303` attribution must be retained as
required by the license.
