using System.Diagnostics;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using SnapshotStore.Diagnostics;

namespace SnapshotStore;

/// <summary>Tuning for <see cref="SnapshotStore{T}"/>.</summary>
public sealed class SnapshotStoreOptions
{
    /// <summary>
    /// A merge that applied fewer records than this is not written back; the next load re-reads that small
    /// delta instead. Avoids a blob write per page view. Zero writes after every merge that changed anything.
    /// </summary>
    public int MinChangesToSave { get; set; } = 100;
}

/// <summary>The result of a load.</summary>
/// <param name="Records">Every record, one per key. Owned by the caller; the store keeps no reference.</param>
/// <param name="HighWater">Largest timestamp seen; the next load reads from here.</param>
/// <param name="FromCache">True when a stored snapshot was the starting point.</param>
/// <param name="ChangesApplied">Records merged from the source on top of the snapshot.</param>
/// <param name="Saved">True when this load wrote a new snapshot back.</param>
public sealed record Snapshot<T>(
    Dictionary<string, T> Records,
    DateTimeOffset? HighWater,
    bool FromCache,
    int ChangesApplied,
    bool Saved) where T : ISnapshotRecord;

/// <summary>
/// Read-through snapshot cache. <see cref="LoadAsync"/> starts from the stored snapshot (if any), asks the
/// <see cref="IRecordSource{T}"/> only for what changed since the snapshot's high-water mark, merges by
/// key, and writes the result back when enough changed.
/// </summary>
/// <remarks>
/// The snapshot is an optimisation, never a source of truth: any failure reading, decoding or writing it is
/// logged and the load proceeds from the source. A failure of the source itself is not swallowed.
/// </remarks>
/// <typeparam name="T">The record type.</typeparam>
public sealed class SnapshotStore<T> where T : ISnapshotRecord
{
    private readonly ISnapshotBlobStore blobs;
    private readonly ISnapshotSerializer<T> serializer;
    private readonly SnapshotStoreOptions options;
    private readonly ILogger logger;

    private static readonly string RecordType = typeof(T).Name;

    /// <summary>Creates a store.</summary>
    /// <param name="blobs">Where snapshots are kept.</param>
    /// <param name="serializer">Snapshot encoding.</param>
    /// <param name="options">Tuning; defaults if null.</param>
    /// <param name="logger">Diagnostics; none if null.</param>
    public SnapshotStore(
        ISnapshotBlobStore blobs,
        ISnapshotSerializer<T> serializer,
        SnapshotStoreOptions? options = null,
        ILogger<SnapshotStore<T>>? logger = null)
    {
        this.blobs = blobs;
        this.serializer = serializer;
        this.options = options ?? new SnapshotStoreOptions();
        this.logger = logger ?? NullLogger<SnapshotStore<T>>.Instance;
    }

    /// <summary>Loads the current full set of records for <paramref name="name"/>.</summary>
    /// <param name="name">Snapshot identity, e.g. <c>tenant/account/Order</c>. Becomes the blob name.</param>
    /// <param name="source">Where changes since the snapshot come from.</param>
    /// <param name="cancellationToken">Cancels the load.</param>
    /// <returns>The merged records and how they were obtained.</returns>
    public async Task<Snapshot<T>> LoadAsync(string name, IRecordSource<T> source, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(source);

        var started = Stopwatch.GetTimestamp();
        using var load = SnapshotStoreSpans.StartLoad(name, RecordType);

        var records = new Dictionary<string, T>();
        DateTimeOffset? highWater = null;
        string? version = null;

        var cache = await this.TryLoadCacheAsync(name, cancellationToken).ConfigureAwait(false);
        SnapshotStoreSpans.Tag(load, SnapshotStoreSpans.CacheKey, cache.Outcome);
        if (cache.Data is { } data)
        {
            version = cache.Version;
            highWater = data.HighWater;
            foreach (var record in data.Records)
                records[record.Key] = record;
        }

        var changes = 0;
        var pages = 0;
        var sourceStarted = Stopwatch.GetTimestamp();
        try
        {
            using var read = SnapshotStoreSpans.StartClient(SnapshotStoreSpans.SourceReadSpan, name);
            SnapshotStoreSpans.Tag(read, SnapshotStoreSpans.SinceKey, highWater?.ToUnixTimeMilliseconds());
            try
            {
                await foreach (var page in source.ReadSinceAsync(highWater, cancellationToken).ConfigureAwait(false))
                {
                    pages++;
                    foreach (var record in page)
                    {
                        if (record is null)
                            continue;

                        records[record.Key] = record;
                        changes++;

                        if (highWater is null || record.Timestamp > highWater)
                            highWater = record.Timestamp;
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                SnapshotStoreSpans.Failed(read, ex);
                throw;
            }

            SnapshotStoreSpans.Tag(read, SnapshotStoreSpans.PagesKey, pages);
            SnapshotStoreSpans.Tag(read, SnapshotStoreSpans.ChangesKey, changes);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            SnapshotStoreDiagnostics.SourceErrors.Add(1, new KeyValuePair<string, object?>("record_type", RecordType));
            SnapshotStoreSpans.Failed(load, ex);
            throw;
        }
        finally
        {
            SnapshotStoreDiagnostics.SourceDuration.Record(SnapshotStoreDiagnostics.ElapsedMs(sourceStarted), new KeyValuePair<string, object?>("record_type", RecordType));
        }

        var saved = false;
        if (changes > 0 && changes >= this.options.MinChangesToSave)
            saved = await this.TrySaveAsync(name, new SnapshotData<T>(highWater, records.Values), version, cancellationToken).ConfigureAwait(false);

        var tags = new TagList { { "record_type", RecordType }, { "cache", cache.Outcome } };
        SnapshotStoreDiagnostics.Loads.Add(1, tags);
        SnapshotStoreDiagnostics.LoadDuration.Record(SnapshotStoreDiagnostics.ElapsedMs(started), tags);
        SnapshotStoreDiagnostics.Records.Record(records.Count, new KeyValuePair<string, object?>("record_type", RecordType));
        if (changes > 0)
            SnapshotStoreDiagnostics.Changes.Add(changes, new KeyValuePair<string, object?>("record_type", RecordType));

        SnapshotStoreSpans.Tag(load, SnapshotStoreSpans.RecordsKey, records.Count);
        SnapshotStoreSpans.Tag(load, SnapshotStoreSpans.ChangesKey, changes);
        SnapshotStoreSpans.Tag(load, SnapshotStoreSpans.SavedKey, saved);

        return new Snapshot<T>(records, highWater, cache.Data is not null, changes, saved);
    }

    private async Task<CacheLookup> TryLoadCacheAsync(string name, CancellationToken cancellationToken)
    {
        using var span = SnapshotStoreSpans.StartClient(SnapshotStoreSpans.CacheReadSpan, name);
        try
        {
            await using var blob = await this.blobs.TryReadAsync(name, cancellationToken).ConfigureAwait(false);
            if (blob is null)
            {
                SnapshotStoreSpans.Tag(span, SnapshotStoreSpans.CacheKey, "miss");
                return CacheLookup.Miss;
            }

            var data = await this.serializer.ReadAsync(blob.Content, cancellationToken).ConfigureAwait(false);
            if (data is null)
            {
                this.logger.LogInformation("Snapshot {Name} has an incompatible format version; rebuilding from source.", name);
                SnapshotStoreSpans.Tag(span, SnapshotStoreSpans.CacheKey, "incompatible");
                return CacheLookup.Incompatible;
            }

            SnapshotStoreSpans.Tag(span, SnapshotStoreSpans.CacheKey, "hit");
            return new CacheLookup(data, blob.Version, "hit");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            this.logger.LogWarning(ex, "Snapshot {Name} could not be read; rebuilding from source.", name);
            SnapshotStoreSpans.Failed(span, ex);
            SnapshotStoreSpans.Tag(span, SnapshotStoreSpans.CacheKey, "error");
            SnapshotStoreDiagnostics.CacheErrors.Add(1, new TagList { { "record_type", RecordType }, { "operation", "read" } });
            return CacheLookup.Error;
        }
    }

    private async Task<bool> TrySaveAsync(string name, SnapshotData<T> data, string? version, CancellationToken cancellationToken)
    {
        using var span = SnapshotStoreSpans.StartClient(SnapshotStoreSpans.CacheWriteSpan, name);
        var result = "error";
        try
        {
            await using var buffer = new MemoryStream();
            await this.serializer.WriteAsync(buffer, data, cancellationToken).ConfigureAwait(false);
            buffer.Position = 0;
            SnapshotStoreSpans.Tag(span, SnapshotStoreSpans.BytesKey, buffer.Length);
            SnapshotStoreDiagnostics.SnapshotBytes.Record(buffer.Length, new KeyValuePair<string, object?>("record_type", RecordType));

            var written = await this.blobs.TryWriteAsync(name, buffer, version, cancellationToken).ConfigureAwait(false);
            result = written ? "saved" : "conflict";
            if (!written)
                this.logger.LogInformation("Snapshot {Name} was updated by another writer; keeping theirs.", name);
            return written;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            this.logger.LogWarning(ex, "Snapshot {Name} could not be saved; the next load will re-read the delta.", name);
            SnapshotStoreSpans.Failed(span, ex);
            SnapshotStoreDiagnostics.CacheErrors.Add(1, new TagList { { "record_type", RecordType }, { "operation", "write" } });
            return false;
        }
        finally
        {
            SnapshotStoreSpans.Tag(span, SnapshotStoreSpans.SaveResultKey, result);
            SnapshotStoreDiagnostics.Saves.Add(1, new TagList { { "record_type", RecordType }, { "result", result } });
        }
    }

    private readonly record struct CacheLookup(SnapshotData<T>? Data, string? Version, string Outcome)
    {
        internal static readonly CacheLookup Miss = new(null, null, "miss");
        internal static readonly CacheLookup Incompatible = new(null, null, "incompatible");
        internal static readonly CacheLookup Error = new(null, null, "error");
    }
}
