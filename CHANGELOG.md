## [1.1.3](https://github.com/state303/go-discogs/compare/v1.1.2...v1.1.3) (2024-02-10)


### Bug Fixes

* **dep:** replace to new checksum ([8251672](https://github.com/state303/go-discogs/commit/825167271dc7500a9e5bf4246bcbcc58acd7f400))
* release item not to contain an empty str field ([72cbcfb](https://github.com/state303/go-discogs/commit/72cbcfb81a67b801f8667372ee8816a48f91a7b5))
* releases and masters will not contain empty genre, style entry ([60c59b5](https://github.com/state303/go-discogs/commit/60c59b5e4d4814c015e6c71087ee5fec2125bfeb))
* removes redundant ctx reference ([b823bcc](https://github.com/state303/go-discogs/commit/b823bcc976f432f76ce64a7d5de04c3d0001a19b))

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
