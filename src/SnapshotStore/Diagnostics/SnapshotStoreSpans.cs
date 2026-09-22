using System.Diagnostics;

namespace SnapshotStore.Diagnostics;

/// <summary>
/// The store's spans and the attribute names they use: <c>snapshots.load</c> with children
/// <c>snapshots.cache.read</c>, <c>snapshots.source.read</c> and <c>snapshots.cache.write</c>.
/// </summary>
/// <remarks>
/// <b>Cost when tracing is off.</b> <see cref="ActivitySource.StartActivity(string, ActivityKind)"/> returns
/// <see langword="null"/> when nothing listens: no span, no tags, no allocation. Every tag is guarded by
/// <see cref="Activity.IsAllDataRequested"/>, so a span that is created but sampled out costs nothing beyond
/// its construction. The child spans nest under <c>snapshots.load</c> through <see cref="Activity.Current"/>.
/// </remarks>
internal static class SnapshotStoreSpans
{
    internal const string LoadSpan = "snapshots.load";
    internal const string CacheReadSpan = "snapshots.cache.read";
    internal const string SourceReadSpan = "snapshots.source.read";
    internal const string CacheWriteSpan = "snapshots.cache.write";

    /// <summary>Attribute: the snapshot name. Unbounded, so span-only.</summary>
    internal const string NameKey = "snapshots.name";

    /// <summary>Attribute: the record type.</summary>
    internal const string RecordTypeKey = "snapshots.record_type";

    /// <summary>Attribute: how the cache lookup ended: hit, miss, incompatible or error.</summary>
    internal const string CacheKey = "snapshots.cache";

    /// <summary>Attribute: records in the resulting snapshot.</summary>
    internal const string RecordsKey = "snapshots.records";

    /// <summary>Attribute: records merged from the source.</summary>
    internal const string ChangesKey = "snapshots.changes";

    /// <summary>Attribute: pages the source returned.</summary>
    internal const string PagesKey = "snapshots.pages";

    /// <summary>Attribute: whether a new snapshot was written.</summary>
    internal const string SavedKey = "snapshots.saved";

    /// <summary>Attribute: how a write ended: saved, conflict or error.</summary>
    internal const string SaveResultKey = "snapshots.save_result";

    /// <summary>Attribute: compressed bytes written.</summary>
    internal const string BytesKey = "snapshots.bytes";

    /// <summary>Attribute: the high-water mark the source read started from, Unix milliseconds; absent for a full read.</summary>
    internal const string SinceKey = "snapshots.since_unix_ms";

    /// <summary>Starts the span for one load.</summary>
    internal static Activity? StartLoad(string name, string recordType)
    {
        var activity = SnapshotStoreDiagnostics.Source.StartActivity(LoadSpan, ActivityKind.Internal);
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag(NameKey, name);
            activity.SetTag(RecordTypeKey, recordType);
        }

        return activity;
    }

    /// <summary>Starts a child span for a call out to storage or the source.</summary>
    internal static Activity? StartClient(string spanName, string name)
    {
        var activity = SnapshotStoreDiagnostics.Source.StartActivity(spanName, ActivityKind.Client);
        if (activity is { IsAllDataRequested: true })
            activity.SetTag(NameKey, name);

        return activity;
    }

    /// <summary>Sets an attribute on a span that may be <see langword="null"/> or sampled out.</summary>
    internal static void Tag(Activity? activity, string key, object? value)
    {
        if (activity is { IsAllDataRequested: true })
            activity.SetTag(key, value);
    }

    /// <summary>
    /// Marks a span as failed by an unexpected error and records the exception. Safe on a
    /// <see langword="null"/> span.
    /// </summary>
    internal static void Failed(Activity? activity, Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        activity?.AddException(ex);
        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
    }
}
