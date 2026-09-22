using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

using StackExchange.Redis;

namespace SnapshotStore.Redis.Tests;

[CollectionDefinition(Name)]
public sealed class RedisCollection : ICollectionFixture<RedisContainer>
{
    public const string Name = "redis";
}

/// <summary>One real Redis for the whole test run; each test gets its own key prefix (see <see cref="Prefix"/>) so tests never collide.</summary>
public sealed class RedisContainer : IAsyncLifetime
{
    private readonly IContainer container = new ContainerBuilder("redis:8-alpine")
        .WithPortBinding(6379, assignRandomHostPort: true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilExternalTcpPortIsAvailable(6379))
        .Build();

    private ConnectionMultiplexer connection = null!;

    public IDatabase Database { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await this.container.StartAsync();
        this.connection = await ConnectionMultiplexer.ConnectAsync($"{this.container.Hostname}:{this.container.GetMappedPublicPort(6379)}");
        this.Database = this.connection.GetDatabase();
    }

    public async ValueTask DisposeAsync()
    {
        await this.connection.DisposeAsync();
        await this.container.DisposeAsync();
    }

    /// <summary>A fresh, unique key prefix — cheap isolation without needing a per-test FLUSHDB.</summary>
    public static string Prefix([System.Runtime.CompilerServices.CallerMemberName] string test = "") => $"{test}:{Guid.NewGuid():N}:";
}
