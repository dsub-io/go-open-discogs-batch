# Changelog

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
