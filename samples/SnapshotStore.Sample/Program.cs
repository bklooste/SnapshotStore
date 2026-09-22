// Minimal runnable example: an in-memory source (stands in for a paged API/Table you only read "changed
// since"), a Redis-backed snapshot store, and telemetry wired to the console exporter.
//
//   docker run --rm -p 6379:6379 redis:8-alpine   # in another terminal
//   dotnet run --project samples/SnapshotStore.Sample

using System.Text.Json.Serialization;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using SnapshotStore;
using SnapshotStore.Redis;
using StackExchange.Redis;

using var tracerProvider = Sdk.CreateTracerProviderBuilder().AddSnapshotStore().AddConsoleExporter().Build();
using var meterProvider = Sdk.CreateMeterProviderBuilder().AddSnapshotStore().AddConsoleExporter().Build();

var redis = await ConnectionMultiplexer.ConnectAsync("localhost:6379");
var database = redis.GetDatabase();

var store = new SnapshotStore<Order>(
    new RedisSnapshotStore(database, keyPrefix: "sample:"),
    new JsonGzipSnapshotSerializer<Order>(OrderJson.Default.Order),
    new SnapshotStoreOptions { MinChangesToSave = 1 });

var source = new InMemorySource();
source.Add(new Order("order-1", DateTimeOffset.UtcNow, "placed"));
source.Add(new Order("order-2", DateTimeOffset.UtcNow, "placed"));

var first = await store.LoadAsync("demo/tenant/Order", source);
Console.WriteLine($"First load: {first.Records.Count} records, fromCache={first.FromCache}, saved={first.Saved}");

source.Add(new Order("order-1", DateTimeOffset.UtcNow, "shipped")); // same key, later record replaces it
var second = await store.LoadAsync("demo/tenant/Order", source);
Console.WriteLine($"Second load: {second.Records.Count} records, fromCache={second.FromCache}, order-1={second.Records["order-1"].Status}");

public sealed record Order(string Key, DateTimeOffset Timestamp, string Status) : ISnapshotRecord;

[JsonSerializable(typeof(Order))]
internal partial class OrderJson : JsonSerializerContext;

/// <summary>Stands in for a real source (a paged Table/API read). Real sources are typically I/O, not memory.</summary>
internal sealed class InMemorySource : IRecordSource<Order>
{
    private readonly List<Order> orders = [];

    public void Add(Order order) => orders.Add(order);

    public async IAsyncEnumerable<IReadOnlyList<Order>> ReadSinceAsync(DateTimeOffset? since, [System.Runtime.CompilerServices.EnumeratorCancellation] System.Threading.CancellationToken ct)
    {
        await Task.Yield();
        yield return orders.Where(o => since is null || o.Timestamp >= since).ToList();
    }
}
