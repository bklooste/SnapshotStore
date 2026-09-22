using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace SnapshotStore.Tests;

/// <summary>
/// The store's spans and metrics. Listeners are scoped to the store's own source/meter name, and the source
/// and meter are process-wide statics, so these run in one collection to keep concurrent tests from
/// contaminating each other's measurements.
/// </summary>
[Collection("telemetry")]
public class TelemetryTests
{
    private static readonly DateTimeOffset T0 = new(2026, 9, 1, 0, 0, 0, TimeSpan.Zero);
    private static Row R(string key, int minutes) => new(key, T0.AddMinutes(minutes), "v");

    private static SnapshotStore<Row> Store(MemoryBlobs blobs, int minChanges = 1) =>
        new(blobs, new JsonGzipSnapshotSerializer<Row>(TestJson.Default.Row), new SnapshotStoreOptions { MinChangesToSave = minChanges });

    private sealed class Capture : IDisposable
    {
        private readonly ActivityListener spans;
        private readonly MeterListener meters = new();
        private readonly string marker = Guid.NewGuid().ToString("N");

        public List<Activity> Activities { get; } = [];
        public List<(string Name, double Value, Dictionary<string, object?> Tags)> Measurements { get; } = [];
        public string Name(string suffix) => $"{this.marker}/{suffix}";

        public Capture()
        {
            this.spans = new ActivityListener
            {
                ShouldListenTo = s => s.Name == SnapshotStoreOpenTelemetryExtensions.TelemetrySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = a => { lock (this.Activities) this.Activities.Add(a); },
            };
            ActivitySource.AddActivityListener(this.spans);

            this.meters.InstrumentPublished = (i, l) =>
            {
                if (i.Meter.Name == SnapshotStoreOpenTelemetryExtensions.TelemetrySourceName)
                    l.EnableMeasurementEvents(i);
            };
            this.meters.SetMeasurementEventCallback<long>((i, v, t, _) => this.Add(i, v, t));
            this.meters.SetMeasurementEventCallback<double>((i, v, t, _) => this.Add(i, v, t));
            this.meters.Start();
        }

        private void Add(Instrument i, double v, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var d = new Dictionary<string, object?>();
            foreach (var t in tags) d[t.Key] = t.Value;
            lock (this.Measurements) this.Measurements.Add((i.Name, v, d));
        }

        public double Sum(string metric, string? tag = null, object? value = null)
        {
            lock (this.Measurements)
                return this.Measurements.Where(m => m.Name == metric && (tag is null || Equals(m.Tags.GetValueOrDefault(tag), value))).Sum(m => m.Value);
        }

        public int Count(string metric)
        {
            lock (this.Measurements) return this.Measurements.Count(m => m.Name == metric);
        }

        public Activity Span(string name, string snapshot)
        {
            lock (this.Activities)
                return this.Activities.Single(a => a.OperationName == name && (string?)a.GetTagItem("snapshots.name") == snapshot);
        }

        public void Dispose()
        {
            this.spans.Dispose();
            this.meters.Dispose();
        }
    }

    [Fact]
    public async Task A_cold_load_emits_a_load_span_with_cache_source_and_write_children()
    {
        using var t = new Capture();
        var name = t.Name("cold");

        await Store(new MemoryBlobs()).LoadAsync(name, new ListSource(R("a", 1), R("b", 2), R("c", 3)));

        var load = t.Span("snapshots.load", name);
        Assert.Equal("Row", load.GetTagItem("snapshots.record_type"));
        Assert.Equal("miss", load.GetTagItem("snapshots.cache"));
        Assert.Equal(3, load.GetTagItem("snapshots.records"));
        Assert.Equal(3, load.GetTagItem("snapshots.changes"));
        Assert.Equal(true, load.GetTagItem("snapshots.saved"));

        Assert.Equal("miss", t.Span("snapshots.cache.read", name).GetTagItem("snapshots.cache"));
        var read = t.Span("snapshots.source.read", name);
        Assert.Equal(2, read.GetTagItem("snapshots.pages"));
        Assert.Null(read.GetTagItem("snapshots.since_unix_ms"));
        var write = t.Span("snapshots.cache.write", name);
        Assert.Equal("saved", write.GetTagItem("snapshots.save_result"));
        Assert.True((long)write.GetTagItem("snapshots.bytes")! > 0);

        // Children nest under the load span.
        Assert.Equal(load.Id, t.Span("snapshots.source.read", name).ParentId);
        Assert.Equal(load.Id, write.ParentId);
    }

    [Fact]
    public async Task A_warm_load_records_a_hit_and_reads_the_source_from_the_high_water_mark()
    {
        using var t = new Capture();
        var name = t.Name("warm");
        var blobs = new MemoryBlobs();
        var store = Store(blobs);
        var source = new ListSource(R("a", 1), R("b", 2));
        await store.LoadAsync(name, source);
        source.Rows.Add(R("c", 5));

        await store.LoadAsync(name, source);

        var loads = t.Activities.Where(a => a.OperationName == "snapshots.load" && (string?)a.GetTagItem("snapshots.name") == name).ToList();
        Assert.Equal(["hit", "miss"], loads.Select(a => (string)a.GetTagItem("snapshots.cache")!).Order());
        var warmRead = t.Activities.Where(a => a.OperationName == "snapshots.source.read" && a.GetTagItem("snapshots.since_unix_ms") is not null).Single();
        Assert.Equal(T0.AddMinutes(2).ToUnixTimeMilliseconds(), warmRead.GetTagItem("snapshots.since_unix_ms"));
    }

    [Fact]
    public async Task Metrics_count_loads_changes_and_saves_by_outcome()
    {
        using var t = new Capture();
        var store = Store(new MemoryBlobs());

        await store.LoadAsync(t.Name("m"), new ListSource(R("a", 1), R("b", 2)));

        Assert.Equal(1, t.Sum("snapshots.loads", "cache", "miss"));
        Assert.Equal(2, t.Sum("snapshots.changes"));
        Assert.Equal(1, t.Sum("snapshots.saves", "result", "saved"));
        Assert.Equal(1, t.Count("snapshots.load.duration"));
        Assert.Equal(1, t.Count("snapshots.source.duration"));
        Assert.Equal(2, t.Sum("snapshots.records"));
        Assert.True(t.Sum("snapshots.snapshot.bytes") > 0);
        Assert.All(t.Measurements.Where(m => m.Name.StartsWith("snapshots.")), m => Assert.Equal("Row", m.Tags["record_type"]));
    }

    [Fact]
    public async Task Metric_tags_never_carry_the_unbounded_snapshot_name()
    {
        using var t = new Capture();

        await Store(new MemoryBlobs()).LoadAsync(t.Name("card"), new ListSource(R("a", 1)));

        Assert.All(t.Measurements, m => Assert.DoesNotContain("snapshots.name", m.Tags.Keys));
        Assert.All(t.Measurements, m => Assert.DoesNotContain("name", m.Tags.Keys));
    }

    [Fact]
    public async Task An_absorbed_cache_failure_is_counted_and_marks_the_child_span_but_not_the_load()
    {
        using var t = new Capture();
        var name = t.Name("cachefail");
        var blobs = new MemoryBlobs { FailReads = _ => true };

        await Store(blobs).LoadAsync(name, new ListSource(R("a", 1)));

        Assert.Equal(1, t.Sum("snapshots.cache.errors", "operation", "read"));
        Assert.Equal(1, t.Sum("snapshots.loads", "cache", "error"));
        Assert.Equal(ActivityStatusCode.Error, t.Span("snapshots.cache.read", name).Status);
        Assert.NotEqual(ActivityStatusCode.Error, t.Span("snapshots.load", name).Status);
    }

    [Fact]
    public async Task A_source_failure_fails_the_load_span_and_is_counted()
    {
        using var t = new Capture();
        var name = t.Name("srcfail");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Store(new MemoryBlobs()).LoadAsync(name, new ListSource { Throws = true }));

        Assert.Equal(1, t.Sum("snapshots.source.errors"));
        Assert.Equal(ActivityStatusCode.Error, t.Span("snapshots.load", name).Status);
        Assert.Equal(ActivityStatusCode.Error, t.Span("snapshots.source.read", name).Status);
        Assert.Equal(0, t.Count("snapshots.loads"));
    }

    [Fact]
    public async Task A_lost_write_race_is_a_conflict_not_an_error()
    {
        using var t = new Capture();
        var name = t.Name("race");
        var blobs = new MemoryBlobs();
        var store = Store(blobs);
        await store.LoadAsync(name, new ListSource(R("a", 1)));
        blobs.AfterRead = n => blobs.Store[n] = ([9], blobs.Store[n].Version + 1);

        await store.LoadAsync(name, new ListSource(R("a", 1), R("b", 2)));

        Assert.Equal(1, t.Sum("snapshots.saves", "result", "conflict"));
        Assert.Equal(0, t.Sum("snapshots.cache.errors"));
    }

    [Fact]
    public void The_source_name_is_exposed_for_wiring_by_name()
    {
        Assert.Equal("SnapshotStore", SnapshotStoreOpenTelemetryExtensions.TelemetrySourceName);
    }
}
