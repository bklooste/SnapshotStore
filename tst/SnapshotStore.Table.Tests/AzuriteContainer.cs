using Azure.Data.Tables;
using Azure.Storage.Blobs;

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace SnapshotStore.Table.Tests;

[CollectionDefinition(Name)]
public sealed class AzuriteCollection : ICollectionFixture<AzuriteContainer>
{
    public const string Name = "azurite";
}

/// <summary>One real Azurite (Table + Blob) for the whole test run. Well-known emulator account/key — not a secret.</summary>
public sealed class AzuriteContainer : IAsyncLifetime
{
    private const string ConnectionStringTemplate =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "TableEndpoint=http://127.0.0.1:{0}/devstoreaccount1;BlobEndpoint=http://127.0.0.1:{1}/devstoreaccount1;";

    private readonly IContainer container = new ContainerBuilder("mcr.microsoft.com/azure-storage/azurite:3.28.0")
        .WithCommand("azurite", "--tableHost", "0.0.0.0", "--blobHost", "0.0.0.0", "--skipApiVersionCheck")
        .WithPortBinding(10002, assignRandomHostPort: true)
        .WithPortBinding(10000, assignRandomHostPort: true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilExternalTcpPortIsAvailable(10002))
        .Build();

    private TableServiceClient tableService = null!;
    private BlobServiceClient blobService = null!;

    public async ValueTask InitializeAsync()
    {
        await container.StartAsync();
        var connectionString = string.Format(ConnectionStringTemplate,
            container.GetMappedPublicPort(10002), container.GetMappedPublicPort(10000));
        tableService = new TableServiceClient(connectionString);
        blobService = new BlobServiceClient(connectionString);
    }

    public async ValueTask DisposeAsync() => await container.DisposeAsync();

    /// <summary>A fresh table for isolation (table names must be alphanumeric, no per-test FLUSHDB equivalent).</summary>
    public async Task<TableClient> NewTableAsync()
    {
        // Table names: alphanumeric only, 3-63 chars, must start with a letter — a bare GUID hex is enough
        // for isolation; a test-name suffix risks the 63-char cap on longer method names.
        var client = tableService.GetTableClient("t" + Guid.NewGuid().ToString("N"));
        await client.CreateIfNotExistsAsync();
        return client;
    }

    /// <summary>A fresh container for isolation.</summary>
    public async Task<BlobContainerClient> NewBlobContainerAsync()
    {
        var client = blobService.GetBlobContainerClient("c" + Guid.NewGuid().ToString("N"));
        await client.CreateIfNotExistsAsync();
        return client;
    }
}
