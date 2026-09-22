# Contributing

Issues and pull requests are welcome.

- `dotnet build SnapshotStore.slnx && dotnet test SnapshotStore.slnx` must pass. The Redis test project needs
  Docker (Testcontainers spins up a real `redis:8-alpine`); everything else is offline.
- Keep the dependency boundary: `SnapshotStore.Azure` must never restore `StackExchange.Redis`,
  `SnapshotStore.Redis` must never restore `Azure.*`, and the core (`SnapshotStore`) must restore neither.
  CI checks this from `project.assets.json`.
- A new backing package follows the same shape: implement `ISnapshotBlobStore` and/or `IRecordSource<T>`,
  reference only the core, ship your own tests.
- Record notable changes in `CHANGELOG.md`. Every push to `main` publishes, so keep `main` releasable.
