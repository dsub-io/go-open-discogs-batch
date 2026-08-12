# Changelog

## [2.3.5](https://github.com/dsub-io/go-open-discogs-batch/compare/v2.3.4...v2.3.5) (2026-08-12)


### Bug Fixes

* keep interactive progress output readable ([#62](https://github.com/dsub-io/go-open-discogs-batch/issues/62)) ([b568812](https://github.com/dsub-io/go-open-discogs-batch/commit/b568812b5ae17c0bba525e66f5bdf5a1f59813bf))
* make a normal interactive invocation show only the stderr progress bar by
  suppressing `byte_progress` and `import_progress` JSON on terminal stdout
* preserve line-delimited JSON when stdout is redirected or piped, including
  the documented `| tee import.jsonl >/dev/null` pattern for log capture without
  replaying structured records beside the bar

### Measured Output and Validation

* interactive structured progress falls from up to `0.2` records per second per
  active reporter to `0` (`100%` reduction); redirected and piped output remains
  bounded at `0.2` records per second plus start and finish records
* the stderr TTY progress bar remains enabled, and the race-enabled full suite,
  PostgreSQL E2E, and `100.0%` statement coverage gate passed

## [2.3.4](https://github.com/dsub-io/go-open-discogs-batch/compare/v2.3.3...v2.3.4) (2026-08-12)


### Bug Fixes

* prevent batch deadlocks and preserve progress bars ([#60](https://github.com/dsub-io/go-open-discogs-batch/issues/60)) ([9d76b04](https://github.com/dsub-io/go-open-discogs-batch/commit/9d76b04a5070d94807b9c738938ce1626f7052dc))
* sort shared genre and style reference inserts so concurrent workers acquire
  PostgreSQL uniqueness locks in one deterministic order for both Master and
  Release imports
* keep interactive progress bars on stderr when it is a TTY, while emitting
  line-delimited `byte_progress` JSON on stdout for pipelines and log collectors
* suppress ANSI-formatted GORM SQL dumps while preserving the bounded returned
  error at the CLI boundary

### Production Recovery and Measured Output

* the failure that prompted this fix occurred with `max-workers=4` after Artist
  and Label completed; the durable ledger retained Master's first `30,000`
  items in `6` chunks, so the corrected importer can resume without rewriting
  completed entities or requesting Discogs metadata again
* non-TTY source progress is bounded from up to `4` carriage-return renders per
  second to `0.2` JSON records per second (`95%` fewer progress writes), and
  download progress from up to `2` renders per second to `0.2` (`90%` fewer)
* the race-enabled full suite passed at `100.0%` statement coverage and the
  PostgreSQL E2E passed with no residual test container, network, or volume

## [2.3.3](https://github.com/dsub-io/go-open-discogs-batch/compare/v2.3.2...v2.3.3) (2026-08-12)


### Bug Fixes

* cache pinned dump catalogs before refresh ([#58](https://github.com/dsub-io/go-open-discogs-batch/issues/58)) ([922cdbe](https://github.com/dsub-io/go-open-discogs-batch/commit/922cdbe672bca57dc61ee3dd7001cbcadefb1a24))
* reduce metadata traffic for an exact dump month from two upstream requests to
  one for uncached 2021+ dumps, and from two requests to zero after the catalog
  has been stored in PostgreSQL
* derive all selected dump URIs and SHA-256 values from the single monthly
  checksum document before starting file downloads
* stop after the first 429, 5xx, timeout, or malformed modern checksum response
  instead of issuing a speculative catalog fallback request
* retain the two-request annual catalog path only for pre-2021 dumps whose
  publication dates were not consistently the first day of the month

## [2.3.2](https://github.com/dsub-io/go-open-discogs-batch/compare/v2.3.1...v2.3.2) (2026-08-11)


### Bug Fixes

* use official Discogs dump browser ([#56](https://github.com/dsub-io/go-open-discogs-batch/issues/56)) ([5b172dd](https://github.com/dsub-io/go-open-discogs-batch/commit/5b172dd1c7a456f8996831517f9626c83e761900))
* restore fresh-database imports after direct S3 catalog and object requests
  began returning HTTP 403 by using the official browser index, year catalog,
  checksum, and download routes
* keep catalog discovery bounded to one request for a pinned month or two for
  latest selection, plus at most one checksum document per selected dump date
* record `size_bytes=0` when the browser exposes only a rounded display size;
  download progress continues to use response `Content-Length` when available

## [2.3.1](https://github.com/dsub-io/go-open-discogs-batch/compare/v2.3.0...v2.3.1) (2026-08-11)


### Bug Fixes

* replace the fixed 250ms delay in out-of-order resume and manifest-expansion tests with explicit later-chunk completion synchronization ([b4028fb](https://github.com/dsub-io/go-open-discogs-batch/commit/b4028fb8f49af358f86ece25ce67d91eb08b63d9))
* bound failed synchronization at 10 seconds instead of allowing a hung release, with 40 consecutive affected scenario executions passing before release

## [2.3.0](https://github.com/dsub-io/go-open-discogs-batch/compare/v2.2.0...v2.3.0) (2026-08-11)


### Features

* add operator-selected PostgreSQL schemas through `--database-schema` and `OPEN_DISCOGS_BATCH_DATABASE_SCHEMA` ([cdbc356](https://github.com/dsub-io/go-open-discogs-batch/commit/cdbc356fea091ef7e8edec30b7fd40a4bab4e412))
* create a missing selected schema and keep canonical migrations, import tables, and migration history inside it
* retain `public` as the compatibility default while warning on every startup and documenting database and role prerequisites

## [2.2.0](https://github.com/dsub-io/go-open-discogs-batch/compare/v2.1.1...v2.2.0) (2026-08-11)


### Features

* replace the unknown-length gzip spinner with exact local compressed-byte
  percentage, throughput, elapsed time, and source ETA ([#49](https://github.com/dsub-io/go-open-discogs-batch/pull/49))
* emit JSON `import_progress` events with exact durable committed items,
  resume baseline, current-run rows per second, last commit time, and explicit
  started, running, completed, failed, and non-fatal observation-error states

### Scale and Validation

* avoid a full XML pre-count pass; each emitted sample performs one primary-key
  summary read, with running observations bounded to once every five seconds
  (`0.2 reads/second` per active entity) plus start and finish reads
* pass the race-enabled full suite at `100.0%` statement coverage and the tagged
  dump-to-PostgreSQL failure/resume E2E, with no residual test container,
  network, or volume
* the first production-sized dump remains intentionally deferred, so no
  200-million-row throughput, memory, or completion-time claim is inferred from
  fixtures

## [2.1.1](https://github.com/dsub-io/go-open-discogs-batch/compare/v2.1.0...v2.1.1) (2026-08-11)


### Performance Improvements

* apply the model `V007` API query indexes during batch schema migration ([f656a09](https://github.com/dsub-io/go-open-discogs-batch/commit/f656a09a261a0d0541de5341ffe835e790c91877))

### Measured Impact

* on the same warm-cache PostgreSQL 18.4 synthetic dataset, deep release pagination p95 fell from `183.106 ms` to `0.038 ms` (`99.979%` lower, `4,818.6x` faster) when consumers use keyset pagination
* indexed title-contains search p95 fell from `194.535 ms` to `0.136 ms` (`99.930%` lower, `1,430.4x` faster), and reverse artist-relation lookup p95 fell from `17.309 ms` to `0.061 ms` (`99.648%` lower, `283.8x` faster)
* measured database size increased from `314,308,287` to `486,389,439` bytes (`+164.1 MiB`, `+54.7%`) for the synthetic benchmark; full 200M+ dump import duration, index size, cold-I/O behavior, and concurrent throughput remain to be measured before production import

## [2.1.0](https://github.com/dsub-io/go-open-discogs-batch/compare/v2.0.0...v2.1.0) (2026-08-10)


### Durability and Idempotency

* persist immutable import manifests, per-entity progress, and exact committed source-chunk ledgers; resume only compatible failed runs and skip only currently clean successful manifests
* reconcile owned relations exactly, fence delayed workers after abandonment, and lock selected entities with their Artist, Label, and Master reference dependencies
* retain downloads and valid ledgers after failure, clean selected files only after durable success, and keep peak work bounded by `chunk-size × max-workers × relation fan-out`

### Correctness

* return dump-listing and payload failures without panicking, make XML cancellation and close ordering race-free, and preserve requested file-copy permissions
* validate rollback, lock release, resumed ledgers, completion fencing, timeout, cancellation, and cleanup paths with real PostgreSQL integration coverage

### Measured Impact

* completed-manifest preflight p50/p95/p99 fell from `38.917/50.768/58.719 ms` to `1.776/2.728/3.038 ms` (`95.4%/94.6%/94.8%` lower), a `21.9x` median speedup that avoids 64 MiB of file I/O per skipped invocation
* median allocations on that path fell from `40,008` to `6,800 B/op` (`83.0%` lower) and from `113` to `106 allocs/op` (`6.2%` lower)
* durable initial-import p50 changed from `48.509` to `73.922 ms` (`+52.4%`) and forced-repeat p50 from `38.864` to `72.822 ms` (`+87.4%`); this is the measured fixed cost of checkpoint validation and active-run fencing on a 12-record fixture, not a full-dataset estimate
* whole-suite statement coverage increased from `79.0%` to `100.0%` (`+21.0` percentage points), including race detection and tagged PostgreSQL E2E paths

### Distribution

* publish GoReleaser artifacts and non-root `linux/amd64` and `linux/arm64` GHCR images through the protected release workflow, including a post-publish architecture check

## [2.0.0](https://github.com/dsub-io/go-open-discogs-batch/compare/v1.2.0...v2.0.0) (2026-08-09)


### ⚠ BREAKING CHANGES

* unify runtime options and environment variables

### Features

* bound concurrent import workers ([b7087e6](https://github.com/dsub-io/go-open-discogs-batch/commit/b7087e664ef969734cff6d27c7e4e530bc13fb09))
* unify runtime options and environment variables ([4783137](https://github.com/dsub-io/go-open-discogs-batch/commit/47831370f9712b6b68a76bc7a6ed7515a9b7ca0e))


### Performance Improvements

* replace `sync.Map` ID caches with segmented bit sets; in the same 1,000,000-ID benchmark, median elapsed time fell from 234.656 ms to 9.257 ms (`25.4x` faster), allocated bytes fell from about 109.5 MB/op to 132.9 KB/op (`99.88%` lower), and allocations fell from 2,359,348 to 39 per operation ([37255eb](https://github.com/dsub-io/go-open-discogs-batch/commit/37255eb262d1f573572c4bdb10a692559f7742f7))
* bound the PostgreSQL pool to `max-workers + 1`, remove one-second connection recycling, stream dependency IDs, and cap inserts below PostgreSQL's 65,535 bind-parameter limit
* replace up to 5,000 row-by-row master-main updates at the default chunk size with one set-based SQL statement (`99.98%` fewer statements)

### Distribution

* publish GoReleaser binaries and packages to GitHub Releases
* publish non-root `linux/amd64` and `linux/arm64` images to `ghcr.io/dsub-io/go-open-discogs-batch`, with a post-publish architecture check

## [1.2.0](https://github.com/dsub-io/go-open-discogs-batch/compare/v1.1.3...v1.2.0) (2026-07-31)


### Maintenance

* **deps:** bump github.com/docker/docker ([cd63ba4](https://github.com/dsub-io/go-open-discogs-batch/commit/cd63ba4afe956a69bdc9167656b00a098baba449))
* **deps:** bump github.com/docker/docker from 25.0.3+incompatible to 25.0.5+incompatible ([714891e](https://github.com/dsub-io/go-open-discogs-batch/commit/714891e91dd51ed77172ed8716d7c07568cc5f5a))
* **deps:** bump github.com/jackc/pgx/v5 from 5.5.3 to 5.5.4 ([a008648](https://github.com/dsub-io/go-open-discogs-batch/commit/a0086480dd0c287ba4826dbaf9bb5bfb4eb3b0f4))
* **deps:** bump github.com/jackc/pgx/v5 from 5.5.3 to 5.5.4 ([e643d77](https://github.com/dsub-io/go-open-discogs-batch/commit/e643d77c4dd6c6b3b334cc9c958982e8812f39b7))
* **deps:** bump google.golang.org/protobuf from 1.32.0 to 1.33.0 ([c816733](https://github.com/dsub-io/go-open-discogs-batch/commit/c8167339e3a75fe3c0ab93bf35f1fb3640b0718c))
* **deps:** bump google.golang.org/protobuf from 1.32.0 to 1.33.0 ([8481b8f](https://github.com/dsub-io/go-open-discogs-batch/commit/8481b8fe35c44172480c6bf1e1ea3c7e972e732a))


### Features

* add entity-safe idempotent imports ([#36](https://github.com/dsub-io/go-open-discogs-batch/issues/36)) ([22d0751](https://github.com/dsub-io/go-open-discogs-batch/commit/22d0751d510b07b5b2ffee813032824d08c9e61a))

## [1.1.5](https://github.com/state303/go-discogs/compare/v1.1.4...v1.1.5) (2024-02-10)


### Bug Fixes

* releases and masters will not contain empty genre, style entry ([60c59b5](https://github.com/state303/go-discogs/commit/60c59b5e4d4814c015e6c71087ee5fec2125bfeb))

## [1.1.4](https://github.com/state303/go-discogs/compare/v1.1.3...v1.1.4) (2024-02-10)


### Bug Fixes

* release item not to contain an empty str field ([72cbcfb](https://github.com/state303/go-discogs/commit/72cbcfb81a67b801f8667372ee8816a48f91a7b5))

## [1.1.3](https://github.com/state303/go-discogs/compare/v1.1.2...v1.1.3) (2024-02-10)


### Bug Fixes

* **dep:** replace to new checksum ([8251672](https://github.com/state303/go-discogs/commit/825167271dc7500a9e5bf4246bcbcc58acd7f400))

## [1.1.2](https://github.com/state303/go-discogs/compare/v1.1.1...v1.1.2) (2022-12-27)


### Bug Fixes

* gofmt linting ([524cddb](https://github.com/state303/go-discogs/commit/524cddb60be16e9972b4000025112905b9373256))

## [1.1.1](https://github.com/state303/go-discogs/compare/v1.1.0...v1.1.1) (2022-12-27)


### Bug Fixes

* adds tests and housekeeping src ([23cfcfc](https://github.com/state303/go-discogs/commit/23cfcfc3ed63d8d472cf2250dd74a2e0e68e7154))
* removes redundant regexp ([9b1ded4](https://github.com/state303/go-discogs/commit/9b1ded4731ccde7d3ecd09bc975a8e7c80d190ae))
* tidy mod ([da71716](https://github.com/state303/go-discogs/commit/da717161219412bbc24c2978a0d37804e6943937))

# [1.1.0](https://github.com/state303/go-discogs/compare/v1.0.1...v1.1.0) (2022-12-27)


### Bug Fixes

* trim spaces for artist rols on releases ([f2dff81](https://github.com/state303/go-discogs/commit/f2dff810532b6c5ae91514d3f65ff373a9445120))


### Features

* simplifies master and release step ([c40e958](https://github.com/state303/go-discogs/commit/c40e958e7bdefc72a94922338bccee5bcad7d772))

## [1.0.1](https://github.com/state303/go-discogs/compare/v1.0.0...v1.0.1) (2022-11-30)


### Bug Fixes

* removes line where genre being printed from release step ([149731b](https://github.com/state303/go-discogs/commit/149731b71beff925592d9b0e053a1b022d4dafb9))

# 1.0.0 (2022-11-30)


### Bug Fixes

* adds composite wait strategy for postgres tc ([8e27803](https://github.com/state303/go-discogs/commit/8e278038fc70f78f0266ecfb3e5edcb3779d3f5b))
* updates wait strategy for testcontainers ([d611412](https://github.com/state303/go-discogs/commit/d6114129140ccbf01df67d447454175182bd6520))


### Features

* initial implementation ([c6707c9](https://github.com/state303/go-discogs/commit/c6707c9b8e4e6f5e242d9f06edcbbbba50087e6f))
