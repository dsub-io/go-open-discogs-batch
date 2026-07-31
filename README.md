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
| `--cleanup`, `-c` | `OPEN_DISCOGS_BATCH_CLEANUP` | `false` | Delete downloads after success |
| `--force`, `-f` | `OPEN_DISCOGS_BATCH_FORCE` | `false` | Rerun an already-successful manifest |
| `--allow-downgrade` | `OPEN_DISCOGS_BATCH_ALLOW_DOWNGRADE` | `false` | Permit and audit older entity dumps |
| `--help`, `-h` | — | — | Show help |
| `--version`, `-v` | — | — | Show version |

Command-line options take precedence over environment variables, which take
precedence over defaults. The two importer implementations accept this same
public contract. Configuration files and the former `new`, `update`, `purge`,
`dsn`, `types`, `year`, `month`, and `data` options are no longer supported.

## Container

Release images are published from Release Please release commits:

```shell
docker pull ghcr.io/dsub-io/go-open-discogs-batch:latest
docker run --rm \
  -e OPEN_DISCOGS_BATCH_DATABASE_URL='postgresql://user:password@db:5432/open_discogs' \
  -e OPEN_DISCOGS_BATCH_ENTITIES='artist,label,master,release' \
  -e OPEN_DISCOGS_BATCH_DATA_DIR=/data \
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
