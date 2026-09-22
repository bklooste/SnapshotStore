# SnapshotStore

[![CI](https://github.com/bklooste/SnapshotStore/actions/workflows/ci.yml/badge.svg)](https://github.com/bklooste/SnapshotStore/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/SnapshotStore.svg)](https://www.nuget.org/packages/SnapshotStore)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Incremental read-through snapshot cache. `LoadAsync(name, source)` starts from a stored snapshot, asks the
source only for records changed since its high-water mark, merges by key, and writes the result back when
enough changed. The snapshot is an optimisation, never a source of truth: cache failures are logged and
absorbed, source failures propagate.

Built for the common **two-level shape**: a Table (or any paged store that only supports "changed since") is
the system of record, and a Blob holds the materialised snapshot on top of it — one big cheap read instead of
re-reading every row, every time. `SnapshotStore.Table` + `SnapshotStore.Azure` ship exactly that pair. Redis
is a single-storage alternative when you want both sides in the same place.

## Why this instead of rolling your own

- **The hard part is the CAS.** Two writers loading concurrently must never let a slow one overwrite a
  fresher snapshot. Both backings get this via a version token (Blob ETag, or a Redis-side counter) with the
  exact same contract, so switching backing doesn't change `SnapshotStore<T>`'s behaviour at all.
- **The merge is idempotent by construction.** The high-water read bound is inclusive on purpose — the
  record that set the mark is safely re-read — so there is no off-by-one to get wrong at the boundary.
- **A failed cache never breaks correctness.** Read, decode or write failures on the *snapshot* are logged
  and absorbed; only a failure of the *source* (the real data) propagates. Get this backwards and a cache bug
  looks like data loss.
- **AOT-friendly and dependency-light.** The core (`SnapshotStore`) has two dependencies total
  (`Microsoft.Extensions.Logging.Abstractions`, `OpenTelemetry.Api`) and no storage SDK — see
  [Storage backings](#storage-backings).

## Quick start

The common case — Table Storage as your data, Blob Storage as the cached snapshot:

```bash
dotnet add package SnapshotStore
dotnet add package SnapshotStore.Table
dotnet add package SnapshotStore.Azure
```

```csharp
var source = new TableRecordSource<Order>(tableClient, OrderJson.Default.Order);   // SnapshotStore.Table
await source.WriteAsync(order, ct);   // however your data actually gets into Table

var store = new SnapshotStore<Order>(
    new AzureBlobSnapshotStore(blobContainer),                                     // SnapshotStore.Azure
    new JsonGzipSnapshotSerializer<Order>(OrderJson.Default.Order));

Snapshot<Order> snap = await store.LoadAsync($"{tenant}/{accountId}/Order", source, ct);
```

`LoadAsync` reads the Blob snapshot, asks Table only for rows changed since its high-water mark, and writes
the merged result back to Blob. A runnable version, against a real Azurite, is in
[samples/SnapshotStore.Sample](samples/SnapshotStore.Sample).

Already have a different source of "changed since" — a paged API, your own Table client, anything else?
Implement `IRecordSource<T>` (one method) instead of using `SnapshotStore.Table`; the Blob snapshot side is
unchanged.

Or use Redis for both sides instead of Table+Blob (`SnapshotStore.Redis`):

```csharp
var store = new SnapshotStore<Order>(
    new RedisSnapshotStore(database, keyPrefix: "myservice:"),   // ISnapshotBlobStore
    new JsonGzipSnapshotSerializer<Order>(OrderJson.Default.Order));

var source = new RedisRecordSource<Order>(database, name: "orders", OrderJson.Default.Order, keyPrefix: "myservice:");
await source.WriteAsync(order, ct);
```

## Storage backings

Two things are pluggable, independently: where the **snapshot** is kept (`ISnapshotBlobStore`) and where the
**source data** comes from (`IRecordSource<T>`). Each backing is its own package that pulls in only its own
client — none of `SnapshotStore.Table`, `.Azure` or `.Redis` restores another's dependency, and the core
restores none of them. Mix and match by referencing whichever packages you need.

| Package | Contents | Depends on |
|---|---|---|
| `SnapshotStore` | interfaces, `SnapshotStore<T>`, `JsonGzipSnapshotSerializer<T>`, telemetry | `Microsoft.Extensions.Logging.Abstractions`, `OpenTelemetry.Api` |
| `SnapshotStore.Table` | `TableRecordSource<T>(TableClient)` — **source side only**, no snapshot storage. One row per record (RowKey = your `Key`, any characters — encoded transparently), a queryable timestamp column for "changed since". A property caps at 64 KB, an entity at 1 MB: many small-to-medium records, not a few huge ones | core, `Azure.Data.Tables` |
| `SnapshotStore.Azure` | `AzureBlobSnapshotStore(BlobContainerClient)` — **snapshot side only**, ETag-conditional writes. No `IRecordSource<T>`; pair it with `SnapshotStore.Table` or your own source | core, `Azure.Storage.Blobs` |
| `SnapshotStore.Redis` | `RedisSnapshotStore(IDatabase)` (Lua CAS, byte-size cap) **and** `RedisRecordSource<T>` (sorted-set index + hash, keyset-paginated so a tie band larger than one page is never skipped) — **both sides**, one store | core, `StackExchange.Redis` |

The caller owns credentials/connections in every package — a `TableClient`, `BlobContainerClient` or
`IConnectionMultiplexer` passed in; none of them read config or build a client for you.

## Behaviour spec

- **Load.** Read the stored snapshot (if any); on any read/decode failure or an incompatible format version,
  treat it as absent — log, don't fail. Ask the source for records `Timestamp >= highWater` (or everything if
  there is no snapshot); the bound is **inclusive**, so the merge must be (and is) idempotent by key. Merge by
  `Key`, a later record replacing an earlier one; advance the high-water mark to the largest timestamp seen.
  Write back only when `MinChangesToSave` (default 100) or more records changed.
- **Concurrency.** Writes are compare-and-swap on the blob's version: the first write only if none exists,
  later ones only if the version read at the start of the load is still current. A slow writer can never
  replace a newer snapshot with an older one.
- **Failure semantics.**

  | Failure | Behaviour |
  |---|---|
  | Snapshot read fails, is corrupt, or an old format version | Log, rebuild from source |
  | Snapshot write fails | Log, return the data; next load re-reads the delta |
  | Another writer saved first (version mismatch) | Keep theirs, `Saved = false` |
  | **Source fails** | Exception propagates — never masked by stale cache |

## Telemetry

Same shape as [RedisEvents](https://github.com/bklooste/RedisEvents): one static `ActivitySource` and
`Meter`, both named `SnapshotStore`. Without wiring, spans and metrics are produced and dropped at zero cost
(`StartActivity` returns null; every attribute is guarded by `IsAllDataRequested`).

```csharp
tracing.AddSnapshotStore();   // TracerProviderBuilder
metrics.AddSnapshotStore();   // MeterProviderBuilder
```

**Spans** — `snapshots.load` with `snapshots.cache.read`, `snapshots.source.read`, `snapshots.cache.write`
children. A source failure marks the load span failed; an absorbed cache failure marks only its child.

| Attribute | On | Meaning |
|---|---|---|
| `snapshots.name` | all | Snapshot name — unbounded, so **span-only**, never a metric tag |
| `snapshots.record_type` | load | Record type name |
| `snapshots.cache` | load, cache.read | `hit` \| `miss` \| `incompatible` \| `error` |
| `snapshots.records` / `snapshots.changes` / `snapshots.saved` | load | Result size, records merged, whether written |
| `snapshots.since_unix_ms` | source.read | High-water mark the read started from; absent for a full read |
| `snapshots.pages` / `snapshots.changes` | source.read | Pages returned, records merged |
| `snapshots.save_result` / `snapshots.bytes` | cache.write | `saved` \| `conflict` \| `error`; compressed size |

**Metrics** — tags are `record_type` plus a closed set of outcomes; never the snapshot name.

| Instrument | Type | Tags |
|---|---|---|
| `snapshots.loads` | counter | `record_type`, `cache` |
| `snapshots.changes` | counter | `record_type` |
| `snapshots.saves` | counter | `record_type`, `result` |
| `snapshots.cache.errors` | counter | `record_type`, `operation` (read / write) |
| `snapshots.source.errors` | counter | `record_type` |
| `snapshots.load.duration` | histogram, ms | `record_type`, `cache` |
| `snapshots.source.duration` | histogram, ms | `record_type` |
| `snapshots.records` | histogram | `record_type` |
| `snapshots.snapshot.bytes` | histogram, By | `record_type` |

No gauges — the store holds no state between loads.

## Development

```bash
dotnet build SnapshotStore.slnx
dotnet test SnapshotStore.slnx --filter "TestType!=PerfTest"   # Redis/Table tests need Docker (real Redis/Azurite)
```

Versions come from [Nerdbank.GitVersioning](https://github.com/dotnet/Nerdbank.GitVersioning)
(`version.json`); every push to `main` that passes tests publishes a new patch version to NuGet. See
[CHANGELOG.md](CHANGELOG.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

**Status:** `0.x` — the API may still move. Spans/metrics attribute names are more likely to be stable than
anything else.

## Licence

[MIT](LICENSE).
