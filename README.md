# SnapshotStore

[![CI](https://github.com/bklooste/SnapshotStore/actions/workflows/ci.yml/badge.svg)](https://github.com/bklooste/SnapshotStore/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/SnapshotStore.svg)](https://www.nuget.org/packages/SnapshotStore)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Incremental read-through snapshot cache. `LoadAsync(name, source)` starts from a stored snapshot, asks the
source only for records changed since its high-water mark, merges by key, and writes the result back when
enough changed. The snapshot is an optimisation, never a source of truth: cache failures are logged and
absorbed, source failures propagate.

Built for the case where a source is slow or rate-limited but the delta since last time is cheap: a paged
Table/API read that only supports "changed since", or a Redis stream you want as a rolling materialised set.

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

```bash
dotnet add package SnapshotStore
dotnet add package SnapshotStore.Azure    # or SnapshotStore.Redis, or both
```

```csharp
var store = new SnapshotStore<Order>(
    new AzureBlobSnapshotStore(container),               // SnapshotStore.Azure
    new JsonGzipSnapshotSerializer<Order>(OrderJson.Default.Order));

Snapshot<Order> snap = await store.LoadAsync($"{tenant}/{accountId}/Order", new OrderSource(client, tenant, accountId), ct);
```

Or backed by Redis instead of Blob (`SnapshotStore.Redis`):

```csharp
var store = new SnapshotStore<Order>(
    new RedisSnapshotStore(database, keyPrefix: "myservice:"),   // ISnapshotBlobStore
    new JsonGzipSnapshotSerializer<Order>(OrderJson.Default.Order));

// And/or a Redis-backed IRecordSource<T> — a producer WriteAsync()s records, this reads them "since":
var source = new RedisRecordSource<Order>(database, name: "orders", OrderJson.Default.Order, keyPrefix: "myservice:");
await source.WriteAsync(order, ct);
```

A runnable version of the Redis example is in [samples/SnapshotStore.Sample](samples/SnapshotStore.Sample).

## Storage backings

Two things are pluggable, independently: where the **snapshot** is kept (`ISnapshotBlobStore`) and where the
**source data** comes from (`IRecordSource<T>`). Each backing is its own package that pulls in only its own
client — `SnapshotStore.Azure` never restores `StackExchange.Redis`, `SnapshotStore.Redis` never restores
`Azure.*`, and the core restores neither. Mix them by referencing both packages, by choice.

| Package | Contents | Depends on |
|---|---|---|
| `SnapshotStore` | interfaces, `SnapshotStore<T>`, `JsonGzipSnapshotSerializer<T>`, telemetry | `Microsoft.Extensions.Logging.Abstractions`, `OpenTelemetry.Api` |
| `SnapshotStore.Azure` | `AzureBlobSnapshotStore(BlobContainerClient)` — ETag-conditional writes | core, `Azure.Storage.Blobs` |
| `SnapshotStore.Redis` | `RedisSnapshotStore(IDatabase)` (Lua CAS, byte-size cap); `RedisRecordSource<T>` (sorted-set index + hash, keyset-paginated so a tie band larger than one page is never skipped) | core, `StackExchange.Redis` |

The caller owns credentials/connections in both — a `BlobContainerClient` or an `IConnectionMultiplexer`
passed in; neither package reads config or builds a client for you.

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
dotnet test SnapshotStore.slnx --filter "TestType!=PerfTest"   # Redis tests need Docker
```

Versions come from [Nerdbank.GitVersioning](https://github.com/dotnet/Nerdbank.GitVersioning)
(`version.json`); every push to `main` that passes tests publishes a new patch version to NuGet. See
[CHANGELOG.md](CHANGELOG.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

**Status:** `0.x` — the API may still move. Spans/metrics attribute names are more likely to be stable than
anything else.

## Licence

[MIT](LICENSE).
