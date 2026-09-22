using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

using SnapshotStore.Diagnostics;

namespace SnapshotStore;

/// <summary>
/// One-call OpenTelemetry wiring for the snapshot store, in the shape of RedisEvents'
/// <c>AddRedisEvents</c>: a service adds one call next to it for the tracer provider and one for the meter
/// provider.
///
/// <para>
/// The <see cref="System.Diagnostics.ActivitySource"/> and <see cref="System.Diagnostics.Metrics.Meter"/> are
/// both named <c>SnapshotStore</c> and are static, so nothing here constructs anything — it only
/// tells the provider to listen. Without these calls the spans and metrics are produced and dropped, which
/// is the usual reason a service reports no snapshot telemetry at all.
/// </para>
/// </summary>
public static class SnapshotStoreOpenTelemetryExtensions
{
    /// <summary>
    /// The name of both the <see cref="System.Diagnostics.ActivitySource"/> and the
    /// <see cref="System.Diagnostics.Metrics.Meter"/>. Exposed for services that wire providers by name from
    /// configuration rather than by calling the methods below.
    /// </summary>
    public const string TelemetrySourceName = SnapshotStoreDiagnostics.SourceName;

    /// <summary>
    /// Emits the store's spans — <c>snapshots.load</c> and its <c>snapshots.cache.read</c>,
    /// <c>snapshots.source.read</c> and <c>snapshots.cache.write</c> children — into this tracer provider.
    /// </summary>
    /// <param name="builder">The tracer provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static TracerProviderBuilder AddSnapshotStore(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.AddSource(TelemetrySourceName);
    }

    /// <summary>
    /// Emits the store's metrics — load, change, save and error counters and the duration, size and record
    /// histograms — into this meter provider.
    /// </summary>
    /// <param name="builder">The meter provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static MeterProviderBuilder AddSnapshotStore(this MeterProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.AddMeter(TelemetrySourceName);
    }
}
