// The common shape this library is built for: Table Storage as the system of record, Blob Storage as the
// cached snapshot on top of it — two levels, not one. Redis is shown too, as the single-storage alternative.
//
//   docker run --rm -p 10000:10000 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite
//   dotnet run --project samples/SnapshotStore.Sample

using System.Text.Json.Serialization;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using SnapshotStore;
using SnapshotStore.Azure;
using SnapshotStore.Table;

using var tracerProvider = Sdk.CreateTracerProviderBuilder().AddSnapshotStore().AddConsoleExporter().Build();
using var meterProvider = Sdk.CreateMeterProviderBuilder().AddSnapshotStore().AddConsoleExporter().Build();

const string azuriteConnectionString =
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

await RunTableAndBlobAsync();
await RunRedisAsync();

// ── Table (source) + Blob (snapshot) — the common two-level shape ──────────────────────────────────────
async Task RunTableAndBlobAsync()
{
    var tableClient = new TableServiceClient(azuriteConnectionString).GetTableClient("orders");
    await tableClient.CreateIfNotExistsAsync();
    var container = new BlobServiceClient(azuriteConnectionString).GetBlobContainerClient("snapshots");
    await container.CreateIfNotExistsAsync();

    // Table is the system of record: a producer publishes rows here as they change.
    var source = new TableRecordSource<Order>(tableClient, OrderJson.Default.Order);
    await source.WriteAsync(new Order("order-1", DateTimeOffset.UtcNow, "placed"), default);
    await source.WriteAsync(new Order("order-2", DateTimeOffset.UtcNow, "placed"), default);

    // Blob holds the materialised snapshot: LoadAsync reads only what changed in Table since last time.
    var store = new SnapshotStore<Order>(
        new AzureBlobSnapshotStore(container),
        new JsonGzipSnapshotSerializer<Order>(OrderJson.Default.Order),
        new SnapshotStoreOptions { MinChangesToSave = 1 });

    var first = await store.LoadAsync("demo/tenant/Order", source);
    Console.WriteLine($"[Table+Blob] First load: {first.Records.Count} records, fromCache={first.FromCache}, saved={first.Saved}");

    await source.WriteAsync(new Order("order-1", DateTimeOffset.UtcNow, "shipped"), default); // same key, later record replaces it
    var second = await store.LoadAsync("demo/tenant/Order", source);
    Console.WriteLine($"[Table+Blob] Second load: {second.Records.Count} records, fromCache={second.FromCache}, order-1={second.Records["order-1"].Status}");
}

// ── Redis, as an alternative: one store for both the snapshot and the source side ──────────────────────
async Task RunRedisAsync()
{
    var redis = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync("localhost:6379");
    var database = redis.GetDatabase();

    var store = new SnapshotStore<Order>(
        new SnapshotStore.Redis.RedisSnapshotStore(database, keyPrefix: "sample:"),
        new JsonGzipSnapshotSerializer<Order>(OrderJson.Default.Order),
        new SnapshotStoreOptions { MinChangesToSave = 1 });

    var source = new SnapshotStore.Redis.RedisRecordSource<Order>(database, name: "orders", OrderJson.Default.Order, keyPrefix: "sample:");
    await source.WriteAsync(new Order("order-1", DateTimeOffset.UtcNow, "placed"), default);

    var snap = await store.LoadAsync("demo/tenant/Order-redis", source);
    Console.WriteLine($"[Redis] Load: {snap.Records.Count} records, fromCache={snap.FromCache}, saved={snap.Saved}");
}

public sealed record Order(string Key, DateTimeOffset Timestamp, string Status) : ISnapshotRecord;

[JsonSerializable(typeof(Order))]
internal partial class OrderJson : JsonSerializerContext;
