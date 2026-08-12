# Go OpenDiscogs Batch

Imports Discogs monthly public data dumps into PostgreSQL. Go and Java use the
same schema published by
[`open-discogs-model`](https://github.com/dsub-io/open-discogs-model).

This project is independent from and not endorsed by Discogs.

## Quick start

The PostgreSQL database must exist. The importer creates the selected schema,
applies canonical migrations, downloads the dumps, and imports them.

```shell
go-open-discogs-batch \
  --database-url 'postgresql://user:password@localhost:5432/open_discogs' \
  --database-schema open_discogs \
  --entities artist,label,master,release
```

Without `--dump-month`, every selected entity uses its latest available dump.
Use `--dump-month=2026-07` to require one exact month.

## Import contract

- Supported entities are `artist`, `label`, `master`, and `release`.
- Downloads and XML parsing are streamed; a dump is never loaded as one
  in-memory document.
- A successful manifest is skipped before downloading when its entity
  checkpoints are still current.
- A compatible failed or interrupted run resumes committed chunks. `--force`
  starts again from zero.
- Each root and its complete relation set commit atomically with durable
  progress.
- Relations removed from a later representation of a root are deleted. Roots
  absent from a complete dump are not deleted.
- Older dumps are rejected unless `--allow-downgrade` is explicitly supplied.
- Downloads remain on disk by default. `--cleanup` removes only files selected
  by a successful import or successful-manifest skip.

See [Import safety and recovery](docs/import-safety.md) for manifest selection,
locking, convergence, interruption, and snapshot visibility.

## Database and permissions

The database itself is never created. A normal run creates a missing
`--database-schema` and the canonical tables inside it; there is no separate
`init` command.

- Creating a schema requires `CREATE` on the database.
- Migrating an existing schema requires `USAGE` and `CREATE` on that schema,
  write access to imported tables, and DDL ownership or equivalent authority.
- If the batch role cannot create or migrate schemas, a DBA must prepare and
  grant the schema first.

The default schema is `public`, and every such startup emits a warning. Prefer
a dedicated schema such as `open_discogs`. Names must be 1–63 lowercase
letters, digits, or underscores and begin with a letter or underscore.

## Configuration

Precedence is `CLI > ENV > default`.

| CLI | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--database-url` | `OPEN_DISCOGS_BATCH_DATABASE_URL` | required | PostgreSQL URI; percent-encode credentials |
| `--database-schema` | `OPEN_DISCOGS_BATCH_DATABASE_SCHEMA` | `public` | Schema to create or migrate |
| `--entities`, `-e` | `OPEN_DISCOGS_BATCH_ENTITIES` | all | Comma-separated entity list |
| `--dump-month`, `-m` | `OPEN_DISCOGS_BATCH_DUMP_MONTH` | latest per entity | Exact month in `yyyy-MM` |
| `--data-dir` | `OPEN_DISCOGS_BATCH_DATA_DIR` | `~/.cache/open-discogs-batch` | Download directory |
| `--chunk-size`, `-b` | `OPEN_DISCOGS_BATCH_CHUNK_SIZE` | `5000` | Roots per transaction chunk |
| `--max-workers` | `OPEN_DISCOGS_BATCH_MAX_WORKERS` | runtime CPU allocation | Concurrent import workers |
| `--cleanup`, `-c` | `OPEN_DISCOGS_BATCH_CLEANUP` | `false` | Delete selected dumps after success |
| `--force`, `-f` | `OPEN_DISCOGS_BATCH_FORCE` | `false` | Reprocess a successful manifest |
| `--allow-downgrade` | `OPEN_DISCOGS_BATCH_ALLOW_DOWNGRADE` | `false` | Permit and audit older dumps |
| `--help`, `-h` | — | — | Show help without a database connection |
| `--version`, `-v` | — | — | Show version without a database connection |

`--max-workers` is an application concurrency limit, not a CPU quota. When it
is omitted, the Go runtime's CPU allocation is used without a percentage
heuristic. Use container or scheduler CPU limits for a hard allocation.

## Progress and logs

Interactive terminals show one progress bar on stderr and suppress structured
progress on terminal stdout. Redirected or piped stdout receives line-delimited
JSON instead; carriage-return bars remain on stderr.

```shell
go-open-discogs-batch [options] | tee import.jsonl >/dev/null
```

Byte progress measures download or compressed source consumption. Import
progress reports only roots committed with their complete relation set.
`committed_percent` is unavailable until end-of-stream validation establishes
the exact total; the importer does not pre-scan large dumps merely to count
roots.

Do not merge stderr into stdout when collecting JSON.

## Container

Release images support `linux/amd64` and `linux/arm64`.

```shell
docker run --rm --cpus=4 \
  -e OPEN_DISCOGS_BATCH_DATABASE_URL='postgresql://user:password@db:5432/open_discogs' \
  -e OPEN_DISCOGS_BATCH_DATABASE_SCHEMA=open_discogs \
  -e OPEN_DISCOGS_BATCH_ENTITIES=artist,label,master,release \
  -e OPEN_DISCOGS_BATCH_DATA_DIR=/data \
  -e OPEN_DISCOGS_BATCH_MAX_WORKERS=4 \
  -v open-discogs-data:/data \
  ghcr.io/dsub-io/go-open-discogs-batch:latest
```

The image runs as non-root. Mount writable storage when downloads must survive
container replacement. Inject the database URI through the deployment
platform's secret mechanism.

Versioned binaries are available from GitHub Releases.

## Resource sizing

Peak application memory is driven by
`chunk-size × max-workers × relation fan-out`, not total dump size. Release has
the largest fan-out.

1. Set `--max-workers` to the smaller of available CPU and database write
   connections reserved for this job.
2. Reduce `--chunk-size` when one expanded relation chunk uses too much memory.
3. Reduce workers when concurrent chunks or PostgreSQL are the bottleneck.

The PostgreSQL pool is bounded to `max-workers + 1`. Reference IDs use a
segmented bit set, and writes stay below PostgreSQL's bind-parameter limit.
Measured changes and reproduction commands are in
[Performance](docs/performance.md).

## Development

Source builds require Go 1.26. Integration tests require Docker and clean up
their exact containers, networks, and volumes on success or failure.

```shell
gofmt -w .
go mod tidy
go vet ./...
go test -race -coverprofile=coverage.out -covermode=atomic ./...
```

CI checks formatting, module consistency, vet, race detection, PostgreSQL
integration behavior, deterministic dump E2E, and 100% statement coverage.
Pull requests and commits use Conventional Commits. Release Please publishes
release-relevant changes; documentation-only changes do not bump the version.

## License

MIT. See [LICENSE](LICENSE). The `state303` attribution must be retained as
required by the license.
