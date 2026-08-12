# Import safety and recovery

This is the operational contract for dump discovery, admission, commits,
recovery, and reader visibility. The schema contract is canonical
[`open-discogs-model`](https://github.com/dsub-io/open-discogs-model) v0.3.1,
shared with the Java importer.

> [!CAUTION]
> Production import is not approved yet. Release both importers against model
> v0.3.1 and finish cross-language migration, recovery, and full-dump validation
> before starting or resuming one.

## Decision table

| Event | Result |
| --- | --- |
| Same successful manifest | Skip before download only while selected checkpoints remain current and no later failed or abandoned run has dirtied them |
| Compatible interrupted run | Resume verified committed chunks |
| Different manifest or `--force` | Start from zero |
| Older entity dump | Reject unless `--allow-downgrade` is set |
| Failed import | Keep downloads and durable progress |
| Successful import with `--cleanup` | Remove only files selected by that import |

## Catalog request budget

Artist, label, master, and release select their newest dumps independently
unless `--dump-month` requests an exact month. Every run pins dump dates,
SHA-256 checksums, source URIs, and stable identifiers in one immutable
manifest before download.

| Selection | Complete durable catalog | Upstream requests when needed |
| --- | --- | --- |
| Exact month, 2021 or newer | Reuse it; **0 requests** | Exactly **1** direct monthly checksum-manifest request |
| Exact month, before 2021 | Reuse it; **0 requests** | Exactly **1** annual catalog request plus **1 checksum request per distinct selected dump date** |
| Latest per entity | Refresh because local state cannot prove newest | Exactly **1** root-index request, **1** latest-year catalog request, and **1 checksum request per distinct selected dump date** |

A document is requested once; HTTP 429, 5xx, timeout, and malformed content do
not trigger speculative retries. If latest discovery fails, a durable catalog
is accepted only when it contains a complete selection. Exact-month discovery
fails after its single bounded refresh attempt.

Selected URIs and checksums are persisted before file download, so a retry
reuses the pinned catalog. Rounded HTML sizes are stored as unknown
(`size_bytes=0`); byte progress uses an exact HTTP `Content-Length` or local
file size when available.

## Admission and locking

A successful manifest is skippable only while every selected entity remains
the current checkpoint and no newer failed or abandoned run has dirtied it.

PostgreSQL advisory locks cover selected entities and their references:

| Import | Locks |
| --- | --- |
| Artist | Artist |
| Label | Label |
| Master | Artist, Master |
| Release | Artist, Label, Master, Release |

Release takes all locks because it also updates `master.main_release_id`.
Independent sets such as Artist and Label may run together; overlapping Go and
Java imports cannot write concurrently. Schema migration takes the same shared
lock family, so migration cannot race an active importer.

Concurrent Release chunks may touch the same Master when a main release changes
or when several releases for one Master cross chunk boundaries. Each chunk
therefore locks every affected Master row in ascending ID order before writing
Release roots or reconciling `main_release_id`. This database lock order is
independent of XML and worker scheduling order. Candidate IDs are collected by
three indexed lookups and then joined to `master`; do not combine those paths
with `OR` in the locking query because PostgreSQL may scan the entire Master
table once per worker.

A partial Master or Release import is admitted only when each omitted reference
entity has a compatible completed checkpoint at the current import contract
revision. Entity completion remains durable when a later entity makes the
parent run fail or the process exits before final run completion. A missing,
incomplete, stale, or same-date reissued checkpoint fails before the batch
writes data. Selecting the dependency in the same run satisfies this preflight.

## Atomicity and idempotency

For each tracked chunk, one PostgreSQL transaction contains:

- canonical root rows and each root's exact supported relation set;
- the committed-chunk ledger entry;
- the processed-item counter.

The model v0.3.1 import contract is part of resume and success compatibility.
An older successful Release contract cannot suppress the corrected Release
import semantics published by that model.

Missing supported relations are deleted, mutable values are updated, and
unchanged rows retain their surrogate IDs. Exact relation duplicates collapse
by canonical PostgreSQL conflict keys; conflicting payloads for one key fail
before SQL instead of applying first- or last-write-wins. Root rows are
upserted, but roots absent from the complete dump are not deleted.

An entity completes only when chunk indexes cover the parsed stream without
gaps, overlaps, or out-of-range values and all item and chunk totals match. A
run succeeds only after every selected entity completes.

Release contract revision 3 stores a model-defined SHA-256 identity for
credited artists, formats, identifiers, tracks, videos, and label/company work
relations. The legacy signed 32-bit hash remains only as a deterministic
compatibility slot.
Exact semantic duplicates collapse, while distinct payloads sharing the old
hash receive separate slots. Stale reconciliation compares both values, so a
legacy null digest or a changed slot assignment is replaced transactionally.

Release `4846884` proves that tracks `6/Яд` and `7/Ад` share legacy hash
`86171`; both must survive. Format identity includes name, reduced
descriptions, canonical quantity, and text. Release `48967` has otherwise
identical `CD`/`Compilation` formats with quantities `1` and `2`, and release
`6662697` has a quantity larger than signed 32-bit storage. The canonical
decimal is retained in `quantity_text`; `quantity` is populated only when it
fits.

The 2026-08 dump audit streamed all 19,341,287 release roots in 2,132.79
seconds with zero duplicate or non-monotonic roots. The corrected allocator
accepted every root, including four conflicting identifier rows and 14
conflicting track rows that the legacy 32-bit keys could not distinguish.

## Interruption and resume

`SIGINT` and `SIGTERM` cancel download, parsing, and database work. The active
chunk rolls back; run failure is recorded through a separate bounded completion
context. `SIGKILL`, host loss, or database loss may leave a run marked
`running`.

After taking the required locks, the next process marks an abandoned run failed
and atomically transfers its valid ledger. Resume requires an exact compatible
match on:

- manifest and per-entity import contract revision;
- processor name and version;
- entity and dump identity;
- chunk size.

If transfer fails, the new run rolls back and the source ledger remains
authoritative. Each chunk fences on its owning run row, so a delayed worker
cannot commit after abandonment. Failed ledgers remain until compatible current
success checkpoints supersede every protected entity/dump pair.

## Visibility and cleanup

Atomicity is per chunk, not per monthly snapshot. Permanent full-dump staging
is avoided because it would duplicate a catalog exceeding 200 million records.
Readers may observe committed chunks during import. Deployments requiring an
all-at-once switch must import into a separate versioned database or replica,
validate it, and then promote it.

Downloaded data cleanup and test infrastructure cleanup are separate:

- `--cleanup` removes only manifest-selected dump files after durable success.
- Integration tests use per-run Docker labels and PostgreSQL tmpfs. In-process
  cleanup and CI's always-run teardown remove only owned containers, networks,
  and volumes, then verify zero residue.
