namespace SnapshotStore.Table.Tests;

[Collection(AzuriteCollection.Name)]
public class TableRecordSourceTests(AzuriteContainer azurite)
{
    private static readonly DateTimeOffset T0 = new(2026, 9, 1, 0, 0, 0, TimeSpan.Zero);

    private async Task<TableRecordSource<TestRow>> Source(int pageSizeHint = 1000) =>
        new(await azurite.NewTableAsync(), TestJson.Default.TestRow, pageSizeHint: pageSizeHint);

    private static async Task<List<TestRow>> Drain(IAsyncEnumerable<IReadOnlyList<TestRow>> pages, CancellationToken ct)
    {
        var all = new List<TestRow>();
        await foreach (var page in pages.WithCancellation(ct))
            all.AddRange(page);
        return all;
    }

    [Fact]
    public async Task With_no_records_reading_since_null_yields_nothing()
    {
        var rows = await Drain((await Source()).ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Written_records_are_read_back_by_key()
    {
        var source = await Source();
        await source.WriteAsync(new TestRow("a", T0, "1"), TestContext.Current.CancellationToken);
        await source.WriteAsync(new TestRow("b", T0.AddSeconds(1), "1"), TestContext.Current.CancellationToken);

        var rows = await Drain(source.ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Equal(["a", "b"], rows.Select(r => r.Key).Order());
    }

    [Fact]
    public async Task Reading_since_a_timestamp_is_inclusive_and_excludes_earlier_records()
    {
        var source = await Source();
        await source.WriteAsync(new TestRow("old", T0, "1"), TestContext.Current.CancellationToken);
        await source.WriteAsync(new TestRow("boundary", T0.AddSeconds(5), "1"), TestContext.Current.CancellationToken);
        await source.WriteAsync(new TestRow("new", T0.AddSeconds(10), "1"), TestContext.Current.CancellationToken);

        var rows = await Drain(source.ReadSinceAsync(T0.AddSeconds(5), TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Equal(["boundary", "new"], rows.Select(r => r.Key).Order());
    }

    [Fact]
    public async Task Rewriting_the_same_key_replaces_its_value_and_timestamp()
    {
        var source = await Source();
        await source.WriteAsync(new TestRow("a", T0, "old"), TestContext.Current.CancellationToken);
        await source.WriteAsync(new TestRow("a", T0.AddSeconds(1), "new"), TestContext.Current.CancellationToken);

        var rows = await Drain(source.ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        var row = Assert.Single(rows);
        Assert.Equal("new", row.Value);
        Assert.Equal(T0.AddSeconds(1), row.Timestamp);
    }

    [Fact]
    public async Task Pagination_across_many_records_returns_every_one_exactly_once()
    {
        var source = await Source(pageSizeHint: 7); // not a divisor of the count below
        const int count = 53;
        for (var i = 0; i < count; i++)
            await source.WriteAsync(new TestRow($"r{i:D3}", T0.AddMilliseconds(i), "v"), TestContext.Current.CancellationToken);

        var seen = new HashSet<string>();
        var pages = 0;
        await foreach (var page in source.ReadSinceAsync(null, TestContext.Current.CancellationToken))
        {
            pages++;
            foreach (var r in page)
                Assert.True(seen.Add(r.Key), $"{r.Key} was returned twice");
        }

        Assert.Equal(count, seen.Count);
        Assert.True(pages > 1, "expected more than one page");
    }

    /// <summary>Every record shares the exact same Timestamp — proves the query filter's tie handling has no gap (Table's own continuation-token paging, not hand-rolled).</summary>
    [Fact]
    public async Task Many_records_sharing_the_exact_same_timestamp_are_all_returned()
    {
        var source = await Source(pageSizeHint: 5);
        const int count = 23;
        for (var i = 0; i < count; i++)
            await source.WriteAsync(new TestRow($"tie{i:D3}", T0, "v"), TestContext.Current.CancellationToken); // all at T0

        var rows = await Drain(source.ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Equal(count, rows.Select(r => r.Key).Distinct().Count());
    }

    [Theory]
    [InlineData("has/slash")]
    [InlineData("has\\backslash")]
    [InlineData("has#hash")]
    [InlineData("has?question")]
    [InlineData("a very ordinary key with spaces")]
    public async Task Keys_with_characters_illegal_in_a_table_row_key_still_round_trip(string key)
    {
        var source = await Source();
        await source.WriteAsync(new TestRow(key, T0, "v"), TestContext.Current.CancellationToken);

        var rows = await Drain(source.ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Equal(key, Assert.Single(rows).Key);
    }

    [Fact]
    public async Task Two_sources_over_different_tables_do_not_collide()
    {
        var a = await Source();
        var b = await Source();
        await a.WriteAsync(new TestRow("k", T0, "a-value"), TestContext.Current.CancellationToken);

        var rows = await Drain(b.ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Empty(rows);
    }

    /// <summary>The two-level shape this backing exists for: Table as the source, Blob as the cached snapshot.</summary>
    [Fact]
    public async Task End_to_end_with_a_real_blob_snapshot_on_top()
    {
        var table = await azurite.NewTableAsync();
        var source = new TableRecordSource<TestRow>(table, TestJson.Default.TestRow);
        var blobs = new SnapshotStore.Azure.AzureBlobSnapshotStore(await azurite.NewBlobContainerAsync());
        var store = new SnapshotStore<TestRow>(blobs, new JsonGzipSnapshotSerializer<TestRow>(TestJson.Default.TestRow),
            new SnapshotStoreOptions { MinChangesToSave = 1 });

        await source.WriteAsync(new TestRow("a", T0, "1"), TestContext.Current.CancellationToken);
        await source.WriteAsync(new TestRow("b", T0.AddSeconds(1), "1"), TestContext.Current.CancellationToken);

        var first = await store.LoadAsync("scope", source, TestContext.Current.CancellationToken);
        Assert.Equal(2, first.Records.Count);
        Assert.True(first.Saved);
        Assert.False(first.FromCache);

        await source.WriteAsync(new TestRow("c", T0.AddSeconds(2), "1"), TestContext.Current.CancellationToken);
        var second = await store.LoadAsync("scope", source, TestContext.Current.CancellationToken);

        Assert.True(second.FromCache);
        Assert.Equal(3, second.Records.Count);
    }
}
