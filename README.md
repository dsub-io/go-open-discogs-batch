# go-open-discogs-batch

`go-open-discogs-batch` imports selected OpenDiscogs data dumps into PostgreSQL.
It uses the canonical schema and generated Go models published by
[`open-discogs-model`](https://github.com/dsub-io/open-discogs-model), so Go and
Java importers write the same database contract.

## Import safety

Each run records an immutable manifest containing the exact dump date, SHA-256
checksum, source URI, size, and ETag for every selected entity.

- A manifest that already completed successfully is skipped.
- `--force` reruns that same manifest, while preserving idempotent database state.
- An older dump is rejected even with `--force`.
- `--allow-downgrade` is the separate, explicit override for older dumps, and the
  override is recorded in import history.
- PostgreSQL advisory locks are acquired in canonical entity order. Runs for
  disjoint entity sets may proceed together; a run that overlaps any active
  entity fails before modifying data.
- Artists, labels, masters, and releases keep independent successful checkpoints,
  because Discogs may publish those dumps on different dates.

## Requirements

- PostgreSQL
- Go 1.26 or a published release binary/container

The application applies the versioned migrations embedded in
`open-discogs-model` when `--new` is supplied.

## Usage

```shell
go-open-discogs-batch \
  --dsn 'postgresql://user:password@localhost:5432/open_discogs' \
  --new \
  --update \
  --year 2026 \
  --month 7 \
  --types artists,labels,masters,releases
```

Important options:

| Option | Default | Purpose |
| --- | --- | --- |
| `--dsn`, `-s` | required | PostgreSQL connection URL |
| `--types`, `-t` | all entities | Selected entity dumps |
| `--year`, `-y` | current year | Catalog year |
| `--month`, `-m` | current month | Catalog month |
| `--data`, `-d` | user data directory | Download cache |
| `--chunk`, `-b` | `5000` | Insert chunk size |
| `--update`, `-u` | `false` | Refresh the upstream dump catalog |
| `--new`, `-n` | `false` | Apply canonical schema migrations |
| `--purge`, `-p` | `false` | Delete downloaded files after success |
| `--force`, `-f` | `false` | Rerun an already-successful manifest |
| `--allow-downgrade` | `false` | Permit and audit an older entity dump |

Configuration may also come from YAML, TOML, or JSON via `--config`. Environment
variables use the `OPEN_DISCOGS_BATCH_` prefix; for example:

```shell
export OPEN_DISCOGS_BATCH_DSN='postgresql://user:password@localhost:5432/open_discogs'
go-open-discogs-batch --new --update
```

## Container

Release images are published only from Release Please release commits:

```shell
docker pull ghcr.io/dsub-io/go-open-discogs-batch:latest
docker run --rm ghcr.io/dsub-io/go-open-discogs-batch:latest --version
```

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
makes one bounded request to the public dump index and runs only on GitHub-hosted
`ubuntu-latest`.

Pull request titles and commits must use Conventional Commits. Release Please
publishes release artifacts only for release-relevant commit types; documentation
changes alone do not create a release.

## License

MIT. See [LICENSE](LICENSE). Attribution to `state303` must be retained as required
by the license.
