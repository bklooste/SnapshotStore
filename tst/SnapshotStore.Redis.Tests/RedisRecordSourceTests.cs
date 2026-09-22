namespace SnapshotStore.Redis.Tests;

[Collection(RedisCollection.Name)]
public class RedisRecordSourceTests(RedisContainer redis)
{
    private static readonly DateTimeOffset T0 = new(2026, 9, 1, 0, 0, 0, TimeSpan.Zero);

    private RedisRecordSource<TestRow> Source(int pageSize = 500) =>
        new(redis.Database, "src", TestJson.Default.TestRow, RedisContainer.Prefix(), pageSize);

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
        var rows = await Drain(this.Source().ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Written_records_are_read_back_by_key()
    {
        var source = this.Source();
        await source.WriteAsync(new TestRow("a", T0, "1"), TestContext.Current.CancellationToken);
        await source.WriteAsync(new TestRow("b", T0.AddSeconds(1), "1"), TestContext.Current.CancellationToken);

        var rows = await Drain(source.ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Equal(["a", "b"], rows.Select(r => r.Key).Order());
    }

    [Fact]
    public async Task Reading_since_a_timestamp_is_inclusive_and_excludes_earlier_records()
    {
        var source = this.Source();
        await source.WriteAsync(new TestRow("old", T0, "1"), TestContext.Current.CancellationToken);
        await source.WriteAsync(new TestRow("boundary", T0.AddSeconds(5), "1"), TestContext.Current.CancellationToken);
        await source.WriteAsync(new TestRow("new", T0.AddSeconds(10), "1"), TestContext.Current.CancellationToken);

        var rows = await Drain(source.ReadSinceAsync(T0.AddSeconds(5), TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Equal(["boundary", "new"], rows.Select(r => r.Key).Order());
    }

    [Fact]
    public async Task Rewriting_the_same_key_replaces_its_value_and_timestamp()
    {
        var source = this.Source();
        await source.WriteAsync(new TestRow("a", T0, "old"), TestContext.Current.CancellationToken);
        await source.WriteAsync(new TestRow("a", T0.AddSeconds(1), "new"), TestContext.Current.CancellationToken);

        var rows = await Drain(source.ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        var row = Assert.Single(rows);
        Assert.Equal("new", row.Value);
        Assert.Equal(T0.AddSeconds(1), row.Timestamp);
    }

    [Fact]
    public async Task Pagination_across_many_distinct_timestamps_returns_every_record_exactly_once()
    {
        var source = this.Source(pageSize: 7); // deliberately not a divisor of the count below
        const int count = 53;
        for (var i = 0; i < count; i++)
            await source.WriteAsync(new TestRow($"r{i:D3}", T0.AddMilliseconds(i), "v"), TestContext.Current.CancellationToken);

        var pages = new List<int>();
        var seen = new HashSet<string>();
        await foreach (var page in source.ReadSinceAsync(null, TestContext.Current.CancellationToken))
        {
            pages.Add(page.Count);
            foreach (var r in page)
                Assert.True(seen.Add(r.Key), $"{r.Key} was returned twice");
        }

        Assert.Equal(count, seen.Count);
        Assert.True(pages.Count > 1, "expected more than one page");
    }

    /// <summary>
    /// The pathological case: every record shares the exact same timestamp, and there are more of them than
    /// fit in one page. Proves the tie-band keyset pagination in ReadSinceAsync never skips a record.
    /// </summary>
    [Fact]
    public async Task A_tie_band_larger_than_one_page_is_still_read_completely_and_without_duplicates()
    {
        var source = this.Source(pageSize: 5);
        const int count = 23;
        for (var i = 0; i < count; i++)
            await source.WriteAsync(new TestRow($"tie{i:D3}", T0, "v"), TestContext.Current.CancellationToken); // all at T0

        var rows = await Drain(source.ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Equal(count, rows.Select(r => r.Key).Distinct().Count());
        Assert.Equal(count, rows.Count);
    }

    /// <summary>A tie band that starts exactly at the `since` bound must still be read in full (the inclusive-bound contract).</summary>
    [Fact]
    public async Task A_tie_band_exactly_at_the_since_bound_is_read_in_full()
    {
        var source = this.Source(pageSize: 3);
        for (var i = 0; i < 10; i++)
            await source.WriteAsync(new TestRow($"tie{i:D2}", T0.AddSeconds(5), "v"), TestContext.Current.CancellationToken);
        await source.WriteAsync(new TestRow("before", T0, "v"), TestContext.Current.CancellationToken);

        var rows = await Drain(source.ReadSinceAsync(T0.AddSeconds(5), TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Equal(10, rows.Count);
        Assert.DoesNotContain(rows, r => r.Key == "before");
    }

    [Fact]
    public async Task Two_sources_with_different_names_under_the_same_prefix_do_not_collide()
    {
        var prefix = RedisContainer.Prefix();
        var a = new RedisRecordSource<TestRow>(redis.Database, "a", TestJson.Default.TestRow, prefix);
        var b = new RedisRecordSource<TestRow>(redis.Database, "b", TestJson.Default.TestRow, prefix);
        await a.WriteAsync(new TestRow("k", T0, "a-value"), TestContext.Current.CancellationToken);

        var rows = await Drain(b.ReadSinceAsync(null, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Empty(rows);
    }
}
