namespace SnapshotStore.Redis.Tests;

[Collection(RedisCollection.Name)]
public class RedisSnapshotStoreTests(RedisContainer redis)
{
    private RedisSnapshotStore Store() => new(redis.Database, RedisContainer.Prefix());

    [Fact]
    public async Task Reading_a_snapshot_that_was_never_written_is_a_miss()
    {
        Assert.Null(await this.Store().TryReadAsync("s", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task First_write_succeeds_with_no_expected_version_and_can_be_read_back()
    {
        var store = this.Store();
        var written = await store.TryWriteAsync("s", new MemoryStream("hello"u8.ToArray()), ifMatchVersion: null, TestContext.Current.CancellationToken);
        Assert.True(written);

        await using var blob = await store.TryReadAsync("s", TestContext.Current.CancellationToken);
        Assert.NotNull(blob);
        using var reader = new StreamReader(blob.Content);
        Assert.Equal("hello", await reader.ReadToEndAsync(TestContext.Current.CancellationToken));
        Assert.NotNull(blob.Version);
    }

    [Fact]
    public async Task A_second_no_expected_version_write_is_refused_once_something_exists()
    {
        var store = this.Store();
        await store.TryWriteAsync("s", new MemoryStream("a"u8.ToArray()), null, TestContext.Current.CancellationToken);

        var second = await store.TryWriteAsync("s", new MemoryStream("b"u8.ToArray()), ifMatchVersion: null, TestContext.Current.CancellationToken);

        Assert.False(second);
        await using var blob = await store.TryReadAsync("s", TestContext.Current.CancellationToken);
        using var reader = new StreamReader(blob!.Content);
        Assert.Equal("a", await reader.ReadToEndAsync(TestContext.Current.CancellationToken)); // untouched
    }

    [Fact]
    public async Task Writing_with_the_current_version_succeeds_and_advances_it()
    {
        var store = this.Store();
        await store.TryWriteAsync("s", new MemoryStream("a"u8.ToArray()), null, TestContext.Current.CancellationToken);
        await using var first = await store.TryReadAsync("s", TestContext.Current.CancellationToken);

        var written = await store.TryWriteAsync("s", new MemoryStream("b"u8.ToArray()), first!.Version, TestContext.Current.CancellationToken);
        Assert.True(written);

        await using var second = await store.TryReadAsync("s", TestContext.Current.CancellationToken);
        Assert.NotEqual(first.Version, second!.Version);
        using var reader = new StreamReader(second.Content);
        Assert.Equal("b", await reader.ReadToEndAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Writing_with_a_stale_version_is_refused_a_losing_writer_never_overwrites_the_winner()
    {
        var store = this.Store();
        await store.TryWriteAsync("s", new MemoryStream("a"u8.ToArray()), null, TestContext.Current.CancellationToken);
        await using var stale = await store.TryReadAsync("s", TestContext.Current.CancellationToken);

        // A concurrent writer updates first.
        await using var current = await store.TryReadAsync("s", TestContext.Current.CancellationToken);
        await store.TryWriteAsync("s", new MemoryStream("winner"u8.ToArray()), current!.Version, TestContext.Current.CancellationToken);

        var losingWrite = await store.TryWriteAsync("s", new MemoryStream("loser"u8.ToArray()), stale!.Version, TestContext.Current.CancellationToken);

        Assert.False(losingWrite);
        await using var final = await store.TryReadAsync("s", TestContext.Current.CancellationToken);
        using var reader = new StreamReader(final!.Content);
        Assert.Equal("winner", await reader.ReadToEndAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Writing_with_an_expected_version_when_nothing_exists_is_refused()
    {
        var written = await this.Store().TryWriteAsync("s", new MemoryStream("a"u8.ToArray()), ifMatchVersion: "1", TestContext.Current.CancellationToken);
        Assert.False(written);
    }

    [Fact]
    public async Task Different_snapshot_names_do_not_collide()
    {
        var store = this.Store();
        await store.TryWriteAsync("a", new MemoryStream("x"u8.ToArray()), null, TestContext.Current.CancellationToken);
        await store.TryWriteAsync("b", new MemoryStream("y"u8.ToArray()), null, TestContext.Current.CancellationToken);

        await using var a = await store.TryReadAsync("a", TestContext.Current.CancellationToken);
        await using var b = await store.TryReadAsync("b", TestContext.Current.CancellationToken);
        using var ar = new StreamReader(a!.Content);
        using var br = new StreamReader(b!.Content);
        Assert.Equal("x", await ar.ReadToEndAsync(TestContext.Current.CancellationToken));
        Assert.Equal("y", await br.ReadToEndAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Two_stores_with_different_prefixes_do_not_collide_on_the_same_name()
    {
        var prefix = RedisContainer.Prefix();
        var storeA = new RedisSnapshotStore(redis.Database, prefix + "a:");
        var storeB = new RedisSnapshotStore(redis.Database, prefix + "b:");
        await storeA.TryWriteAsync("s", new MemoryStream("a-data"u8.ToArray()), null, TestContext.Current.CancellationToken);

        Assert.Null(await storeB.TryReadAsync("s", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task A_snapshot_over_the_byte_cap_is_refused_like_a_lost_race_not_written_and_existing_data_survives()
    {
        var store = new RedisSnapshotStore(redis.Database, RedisContainer.Prefix(), maxSnapshotBytes: 10);
        await store.TryWriteAsync("s", new MemoryStream("small"u8.ToArray()), null, TestContext.Current.CancellationToken); // 5 bytes, under the cap

        var refused = await store.TryWriteAsync("s", new MemoryStream(new byte[50]), ifMatchVersion: (await store.TryReadAsync("s", TestContext.Current.CancellationToken))!.Version, TestContext.Current.CancellationToken);

        Assert.False(refused);
        await using var blob = await store.TryReadAsync("s", TestContext.Current.CancellationToken);
        using var reader = new StreamReader(blob!.Content);
        Assert.Equal("small", await reader.ReadToEndAsync(TestContext.Current.CancellationToken)); // untouched
    }

    [Fact]
    public async Task A_zero_cap_disables_the_limit()
    {
        var store = new RedisSnapshotStore(redis.Database, RedisContainer.Prefix(), maxSnapshotBytes: 0);
        var written = await store.TryWriteAsync("s", new MemoryStream(new byte[10_000]), null, TestContext.Current.CancellationToken);
        Assert.True(written);
    }

    /// <summary>End-to-end: the full SnapshotStore&lt;T&gt; behaviour over a real Redis backing, not a fake.</summary>
    [Fact]
    public async Task SnapshotStore_round_trips_through_real_redis()
    {
        var blobs = this.Store();
        var t0 = DateTimeOffset.UtcNow;
        var source = new StaticSource([new TestRow("a", t0, "1"), new TestRow("b", t0.AddSeconds(1), "1")]);
        var store = new SnapshotStore<TestRow>(blobs, new JsonGzipSnapshotSerializer<TestRow>(TestJson.Default.TestRow), new SnapshotStoreOptions { MinChangesToSave = 1 });

        var first = await store.LoadAsync("scope", source, TestContext.Current.CancellationToken);
        Assert.Equal(2, first.Records.Count);
        Assert.True(first.Saved);

        source.Rows.Add(new TestRow("c", t0.AddSeconds(2), "1"));
        var second = await store.LoadAsync("scope", source, TestContext.Current.CancellationToken);
        Assert.True(second.FromCache);
        Assert.Equal(3, second.Records.Count);
    }

    private sealed class StaticSource(List<TestRow> rows) : IRecordSource<TestRow>
    {
        public List<TestRow> Rows { get; } = rows;

        public async IAsyncEnumerable<IReadOnlyList<TestRow>> ReadSinceAsync(DateTimeOffset? since, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.Yield();
            yield return this.Rows.Where(r => since is null || r.Timestamp >= since).ToList();
        }
    }
}
