# Changelog

Every push to `main` publishes a new patch version automatically (see `version.json` — height-based,
not manually tagged), so not every version number gets its own entry here. This file tracks what
actually changed.

## Unreleased

- Initial public release: core (`SnapshotStore`), Azure Blob backing (`SnapshotStore.Azure`), Redis
  backing (`SnapshotStore.Redis`) — extracted from an in-house library, MIT licensed.
