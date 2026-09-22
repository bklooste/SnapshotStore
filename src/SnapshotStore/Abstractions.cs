namespace SnapshotStore;

/// <summary>A record that can be merged into a snapshot.</summary>
public interface ISnapshotRecord
{
    /// <summary>Identity within a snapshot. A later record with the same key replaces the earlier one.</summary>
    string Key { get; }

    /// <summary>
    /// When the record last changed at the source. The largest value seen becomes the snapshot's
    /// high-water mark, so this must be monotonic per source (a server-assigned modification time).
    /// </summary>
    DateTimeOffset Timestamp { get; }
}

/// <summary>Where records come from — the system of record the snapshot is a cache of.</summary>
/// <typeparam name="T">The record type.</typeparam>
public interface IRecordSource<T> where T : ISnapshotRecord
{
    /// <summary>
    /// Streams records changed at or after <paramref name="since"/>, in pages. The bound is
    /// <b>inclusive</b>: the record that set the previous high-water mark is returned again, so the merge
    /// must be (and is) idempotent by key. <see langword="null"/> means everything.
    /// </summary>
    /// <param name="since">Inclusive lower bound on <see cref="ISnapshotRecord.Timestamp"/>, or null for all records.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>Pages of records; order within and across pages is not significant.</returns>
    IAsyncEnumerable<IReadOnlyList<T>> ReadSinceAsync(DateTimeOffset? since, CancellationToken cancellationToken);
}

/// <summary>Byte storage for snapshots. One named blob per snapshot.</summary>
public interface ISnapshotBlobStore
{
    /// <summary>Opens a snapshot for reading, or returns null when it does not exist.</summary>
    /// <param name="name">Snapshot blob name.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The stream and its version token, or null.</returns>
    Task<SnapshotBlob?> TryReadAsync(string name, CancellationToken cancellationToken);

    /// <summary>
    /// Writes a snapshot. When <paramref name="ifMatchVersion"/> is set the write happens only if the stored
    /// version still equals it; when null it happens only if no blob exists. That makes the write a
    /// compare-and-swap, so two writers cannot silently replace a newer snapshot with an older one.
    /// </summary>
    /// <param name="name">Snapshot blob name.</param>
    /// <param name="content">The serialized snapshot.</param>
    /// <param name="ifMatchVersion">The version token returned by <see cref="TryReadAsync"/>, or null if none existed.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>True when written; false when another writer got there first.</returns>
    Task<bool> TryWriteAsync(string name, Stream content, string? ifMatchVersion, CancellationToken cancellationToken);
}

/// <summary>A snapshot's bytes plus the version token to hand back on write.</summary>
/// <param name="Content">Readable stream; disposing the blob disposes it.</param>
/// <param name="Version">Opaque version (an ETag for Azure).</param>
public sealed record SnapshotBlob(Stream Content, string? Version) : IAsyncDisposable
{
    /// <inheritdoc />
    public ValueTask DisposeAsync() => this.Content.DisposeAsync();
}

/// <summary>Turns a snapshot into bytes and back.</summary>
/// <typeparam name="T">The record type.</typeparam>
public interface ISnapshotSerializer<T> where T : ISnapshotRecord
{
    /// <summary>Writes a snapshot to <paramref name="destination"/>.</summary>
    /// <param name="destination">Target stream; left open.</param>
    /// <param name="snapshot">What to write.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task WriteAsync(Stream destination, SnapshotData<T> snapshot, CancellationToken cancellationToken);

    /// <summary>
    /// Reads a snapshot. Returns null for bytes written by an incompatible format version, which the store
    /// treats as a cache miss. Throws for corrupt bytes.
    /// </summary>
    /// <param name="source">Source stream; left open.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The snapshot, or null if the format version is not the current one.</returns>
    Task<SnapshotData<T>?> ReadAsync(Stream source, CancellationToken cancellationToken);
}

/// <summary>The persisted content of a snapshot.</summary>
/// <param name="HighWater">Largest <see cref="ISnapshotRecord.Timestamp"/> merged so far.</param>
/// <param name="Records">All records, one per key.</param>
public sealed record SnapshotData<T>(DateTimeOffset? HighWater, IReadOnlyCollection<T> Records) where T : ISnapshotRecord;
