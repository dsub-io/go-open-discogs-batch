# Import safety and recovery

This is the detailed contract for dump selection, concurrent runs, commits,
recovery, and reader visibility. For normal setup, start with the
[README](../README.md).

## At a glance

| Event | Result |
| --- | --- |
| Same successful manifest | Skip before downloading while checkpoints remain current |
| Compatible interrupted run | Resume only verified committed chunks |
| Different manifest or `--force` | Start from zero |
| Older entity dump | Reject unless `--allow-downgrade` is set |
| Failed import | Keep downloaded files and durable progress |
| Successful import with `--cleanup` | Remove only files selected by that import |

## Dump discovery

Artist, label, master, and release select their newest dumps independently
unless `--dump-month` requests an exact month. Every run records dump dates,
SHA-256 checksums, source URIs, and stable identifiers in one immutable
manifest.

Discovery depends on the request:

- An exact month first uses a complete matching PostgreSQL catalog entry.
- Missing entities from 2021 onward require one monthly checksum request.
- Older irregular publication dates require one annual catalog request and one
  checksum request.
- Latest-per-entity selection refreshes upstream because a local catalog cannot
  prove that no newer dump exists.

Selected URIs and checksums are stored before file download. Retries reuse this
pinned catalog.

Catalog and file access use the official `data.discogs.com` browser. Rounded
catalog display sizes are stored as unknown (`size_bytes=0`); byte progress
uses HTTP `Content-Length` or the exact local file size. A 429, 5xx, timeout, or
malformed catalog response fails without speculative request retries. If a
latest-catalog refresh fails, a locally stored catalog may be used only when it
fully satisfies the request.

## Admission and locking

A successful manifest is skipped before file download and checksum work only
while all selected entity dumps remain current checkpoints and no later failed
or abandoned run has dirtied those entities.

PostgreSQL advisory locks cover selected entities and their references:

| Import | Locks |
| --- | --- |
| Artist | Artist |
| Label | Label |
| Master | Artist, Master |
| Release | Artist, Label, Master, Release |

Release takes the full set because it also updates `master.main_release_id`.

Independent sets such as Artist and Label may run together. Overlapping Go and
Java imports cannot write concurrently.

## Commit and convergence boundary

For every tracked chunk, one PostgreSQL transaction contains:

- canonical roots and each root's exact relation set;
- the committed-chunk ledger entry;
- the processed-item counter.

Missing relations are deleted, mutable values are updated, and unchanged rows
retain their surrogate IDs. Root rows are upserted; roots absent from the
complete dump are not currently deleted.

An entity completes only when chunk indexes cover the parsed stream with no
gaps, overlaps, or out-of-range indexes and item and chunk totals match. A run
becomes successful only after every selected entity completes.

The schema still uses signed 32-bit Java hashes for several relation identities.
Distinct values can collide within one root. Migration to collision-resistant
identity is tracked in
[`open-discogs-model#43`](https://github.com/dsub-io/open-discogs-model/issues/43).

## Interruption and resume

`SIGINT` and `SIGTERM` cancel download, parsing, and database work. The active
chunk rolls back; run failure is recorded with a separate bounded completion
context. `SIGKILL`, host loss, or a database disconnect may leave a run marked
`running`.

After acquiring the required locks, the next process marks an abandoned run
failed and transfers its valid ledger atomically. Resume requires an exact
match on:

- manifest;
- processor name and version;
- entity and dump identity;
- chunk size.

If transfer fails, the new run rolls back and the source ledger remains
authoritative.

Every chunk fences on its owning run row. A delayed worker cannot commit after
that run is abandoned. Failed ledgers remain until compatible current success
checkpoints supersede every entity and dump pair they protect.

## Snapshot visibility

Atomicity is per chunk, not per monthly snapshot. Permanent full-dump staging
is intentionally avoided because it would duplicate a catalog exceeding 200
million records. Readers may observe committed chunks during an import.

Deployments requiring an all-at-once switch must import into a separate
versioned database or replica and promote it after validation.
