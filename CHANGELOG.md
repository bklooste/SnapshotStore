# Changelog

Every push to `main` publishes a new patch version automatically (see `version.json` — height-based,
not manually tagged), so not every version number gets its own entry here. This file tracks what
actually changed.

## Unreleased

- **New:** `SnapshotStore.Table` — an `IRecordSource<T>` over Azure Table Storage, the common source side
  of the Table+Blob two-level shape (`SnapshotStore.Azure` is the snapshot side). Any record `Key` round-trips
  even with characters Table forbids in a `RowKey`.
- Quick start and the sample now lead with Table+Blob (the common case) instead of Redis.

## 0.1.2 (2026-09-22)

- Initial public release: core (`SnapshotStore`), Azure Blob backing (`SnapshotStore.Azure`), Redis
  backing (`SnapshotStore.Redis`) — extracted from an in-house library, MIT licensed.
