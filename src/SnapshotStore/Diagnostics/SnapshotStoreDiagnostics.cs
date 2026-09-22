using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace SnapshotStore.Diagnostics;

/// <summary>
/// The single <see cref="ActivitySource"/> and <see cref="Meter"/> for the snapshot store.
/// </summary>
/// <remarks>
/// <para>
/// <b>Cardinality.</b> Metric tags are limited to the record type and a small closed set of outcomes. The
/// snapshot <i>name</i> (<c>tenant/account/Order</c>) is per-account and unbounded, so it appears on spans
/// only, never on a metric.
/// </para>
/// <para>
/// There are no observable gauges: the store keeps no per-snapshot state between loads, so there is
/// nothing to observe. "How stale is a snapshot" is <c>snapshots.load.duration</c> and
/// <c>snapshots.changes</c> read together.
/// </para>
/// </remarks>
internal static class SnapshotStoreDiagnostics
{
    /// <summary>
    /// Source and meter name. Register with <c>AddSource</c>/<c>AddMeter</c>, or use
    /// <c>AddSnapshotStore()</c>.
    /// </summary>
    internal const string SourceName = "SnapshotStore";

    private const string Version = "1.0.0";

    /// <summary>ActivitySource for OpenTelemetry tracing.</summary>
    internal static readonly ActivitySource Source = new(SourceName, Version);

    /// <summary>Meter for OpenTelemetry metrics.</summary>
    internal static readonly Meter Meter = new(SourceName, Version);

    /// <summary>Counter: loads completed. Tags: record_type, cache (hit | miss | incompatible | error).</summary>
    internal static readonly Counter<long> Loads =
        Meter.CreateCounter<long>("snapshots.loads", "loads", "Snapshot loads completed");

    /// <summary>Counter: records merged from the source on top of a snapshot. Tags: record_type.</summary>
    internal static readonly Counter<long> Changes =
        Meter.CreateCounter<long>("snapshots.changes", "records", "Records merged from the source");

    /// <summary>Counter: snapshot write attempts. Tags: record_type, result (saved | conflict | error).</summary>
    internal static readonly Counter<long> Saves =
        Meter.CreateCounter<long>("snapshots.saves", "saves", "Snapshot write attempts");

    /// <summary>Counter: cache failures that were absorbed. Tags: record_type, operation (read | write).</summary>
    internal static readonly Counter<long> CacheErrors =
        Meter.CreateCounter<long>("snapshots.cache.errors", "errors", "Snapshot cache failures that fell back to the source");

    /// <summary>Counter: source failures, which propagate to the caller. Tags: record_type.</summary>
    internal static readonly Counter<long> SourceErrors =
        Meter.CreateCounter<long>("snapshots.source.errors", "errors", "Record source failures");

    /// <summary>Histogram: end-to-end load duration in milliseconds. Tags: record_type, cache.</summary>
    internal static readonly Histogram<double> LoadDuration =
        Meter.CreateHistogram<double>("snapshots.load.duration", "ms", "Duration of a snapshot load");

    /// <summary>Histogram: time spent reading the record source in milliseconds. Tags: record_type.</summary>
    internal static readonly Histogram<double> SourceDuration =
        Meter.CreateHistogram<double>("snapshots.source.duration", "ms", "Duration of reading the record source");

    /// <summary>Histogram: records in the loaded snapshot. Tags: record_type.</summary>
    internal static readonly Histogram<long> Records =
        Meter.CreateHistogram<long>("snapshots.records", "records", "Records in a loaded snapshot");

    /// <summary>Histogram: compressed size of a written snapshot in bytes. Tags: record_type.</summary>
    internal static readonly Histogram<long> SnapshotBytes =
        Meter.CreateHistogram<long>("snapshots.snapshot.bytes", "By", "Compressed size of a written snapshot");

    /// <summary>Elapsed milliseconds since <paramref name="startTimestamp"/>.</summary>
    internal static double ElapsedMs(long startTimestamp) => Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds;
}
