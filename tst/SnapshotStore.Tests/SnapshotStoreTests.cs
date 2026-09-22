using System.Text.Json.Serialization;

namespace SnapshotStore.Tests;

public record Row(string Key, DateTimeOffset Timestamp, string Value) : ISnapshotRecord;

[JsonSerializable(typeof(Row))]
internal partial class TestJson : JsonSerializerContext;

internal sealed class MemoryBlobs : ISnapshotBlobStore
{
    public Dictionary<string, (byte[] Bytes, int Version)> Store { get; } = [];
    public Func<string, bool> FailReads { get; set; } = _ => false;
    public bool FailWrites { get; set; }
    public Action<string>? AfterRead { get; set; }
    public int Writes { get; private set; }

    public Task<SnapshotBlob?> TryReadAsync(string name, CancellationToken cancellationToken)
    {
        if (this.FailReads(name)) throw new IOException("read failed");
        var found = this.Store.TryGetValue(name, out var e);
        var blob = found ? new SnapshotBlob(new MemoryStream(e.Bytes), e.Version.ToString()) : null;
        if (found) this.AfterRead?.Invoke(name);
        return Task.FromResult<SnapshotBlob?>(blob);
    }

    public Task<bool> TryWriteAsync(string name, Stream content, string? ifMatchVersion, CancellationToken cancellationToken)
    {
        if (this.FailWrites) throw new IOException("write failed");
        var exists = this.Store.TryGetValue(name, out var e);
        if (ifMatchVersion is null ? exists : !exists || e.Version.ToString() != ifMatchVersion)
            return Task.FromResult(false);

        using var ms = new MemoryStream();
        content.CopyTo(ms);
        this.Store[name] = (ms.ToArray(), exists ? e.Version + 1 : 1);
        this.Writes++;
        return Task.FromResult(true);
    }
}

internal sealed class ListSource(params Row[] rows) : IRecordSource<Row>
{
    public List<DateTimeOffset?> Asked { get; } = [];
    public List<Row> Rows { get; } = [.. rows];
    public bool Throws { get; set; }

    public async IAsyncEnumerable<IReadOnlyList<Row>> ReadSinceAsync(DateTimeOffset? since, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        this.Asked.Add(since);
        if (this.Throws) throw new InvalidOperationException("source down");
        await Task.Yield();
        var matching = this.Rows.Where(r => since is null || r.Timestamp >= since).ToList();
        foreach (var page in matching.Chunk(2))
            yield return page;
    }
}

[Collection("telemetry")]
public class SnapshotStoreTests
{
    private static readonly DateTimeOffset T0 = new(2026, 9, 1, 0, 0, 0, TimeSpan.Zero);
    private static Row R(string key, int minutes, string value = "v") => new(key, T0.AddMinutes(minutes), value);

    private static (SnapshotStore<Row> Store, MemoryBlobs Blobs) Make(int minChanges = 1, MemoryBlobs? blobs = null)
    {
        blobs ??= new MemoryBlobs();
        return (new SnapshotStore<Row>(blobs, new JsonGzipSnapshotSerializer<Row>(TestJson.Default.Row), new SnapshotStoreOptions { MinChangesToSave = minChanges }), blobs);
    }

    [Fact]
    public async Task First_load_reads_everything_and_saves()
    {
        var (store, blobs) = Make();
        var source = new ListSource(R("a", 1), R("b", 2), R("c", 3));

        var snap = await store.LoadAsync("s", source);

        Assert.False(snap.FromCache);
        Assert.Equal(3, snap.Records.Count);
        Assert.Equal(T0.AddMinutes(3), snap.HighWater);
        Assert.True(snap.Saved);
        Assert.Null(source.Asked[0]);
        Assert.Single(blobs.Store);
    }

    [Fact]
    public async Task Second_load_asks_the_source_only_from_the_high_water_mark()
    {
        var (store, _) = Make();
        var source = new ListSource(R("a", 1), R("b", 2));
        await store.LoadAsync("s", source);

        source.Rows.Add(R("c", 5));
        var snap = await store.LoadAsync("s", source);

        Assert.True(snap.FromCache);
        Assert.Equal(T0.AddMinutes(2), source.Asked[1]);
        Assert.Equal(["a", "b", "c"], snap.Records.Keys.Order());
    }

    [Fact]
    public async Task A_later_record_with_the_same_key_replaces_the_cached_one_and_the_inclusive_bound_is_idempotent()
    {
        var (store, _) = Make();
        var source = new ListSource(R("a", 1, "old"), R("b", 2, "b"));
        await store.LoadAsync("s", source);

        source.Rows[0] = R("a", 9, "new");
        var snap = await store.LoadAsync("s", source);

        Assert.Equal("new", snap.Records["a"].Value);
        Assert.Equal(2, snap.Records.Count);
        // "b" sat exactly on the previous high-water mark, so it is re-read and merged again without duplication.
        Assert.Equal(2, snap.ChangesApplied);
    }

    [Fact]
    public async Task Small_deltas_are_not_written_back()
    {
        var (store, blobs) = Make(minChanges: 100);
        var source = new ListSource(R("a", 1));

        var snap = await store.LoadAsync("s", source);

        Assert.False(snap.Saved);
        Assert.Empty(blobs.Store);
    }

    [Fact]
    public async Task An_unchanged_source_does_not_rewrite_the_snapshot()
    {
        var (store, blobs) = Make(minChanges: 0);
        var source = new ListSource();
        await store.LoadAsync("s", source);

        Assert.Equal(0, blobs.Writes);
    }

    [Fact]
    public async Task A_cache_read_failure_falls_back_to_the_source()
    {
        var (store, blobs) = Make();
        blobs.FailReads = _ => true;
        var source = new ListSource(R("a", 1));

        var snap = await store.LoadAsync("s", source);

        Assert.False(snap.FromCache);
        Assert.Single(snap.Records);
    }

    [Fact]
    public async Task Corrupt_cache_bytes_fall_back_to_the_source()
    {
        var (store, blobs) = Make();
        blobs.Store["s"] = ([1, 2, 3, 4], 1);

        var snap = await store.LoadAsync("s", new ListSource(R("a", 1)));

        Assert.False(snap.FromCache);
        Assert.Single(snap.Records);
    }

    [Fact]
    public async Task A_save_failure_still_returns_the_data()
    {
        var (store, blobs) = Make();
        blobs.FailWrites = true;

        var snap = await store.LoadAsync("s", new ListSource(R("a", 1)));

        Assert.False(snap.Saved);
        Assert.Single(snap.Records);
    }

    [Fact]
    public async Task A_source_failure_is_not_swallowed()
    {
        var (store, _) = Make();

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAsync("s", new ListSource { Throws = true }));
    }

    [Fact]
    public async Task A_writer_that_lost_a_race_does_not_overwrite_the_winner()
    {
        var (store, blobs) = Make();
        await store.LoadAsync("s", new ListSource(R("a", 1)));
        var winnerBytes = new byte[] { 9, 9, 9 };

        // Between this load reading the snapshot and writing it back, another writer saves theirs.
        blobs.AfterRead = name => blobs.Store[name] = (winnerBytes, blobs.Store[name].Version + 1);
        var snap = await store.LoadAsync("s", new ListSource(R("a", 1), R("b", 2)));

        Assert.False(snap.Saved);
        Assert.Equal(2, snap.Records.Count);
        Assert.Equal(winnerBytes, blobs.Store["s"].Bytes);
    }

    [Fact]
    public async Task An_incompatible_format_version_is_a_cache_miss()
    {
        var (store, blobs) = Make();
        using var ms = new MemoryStream();
        await using (var gz = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionLevel.Fastest, true))
            await gz.WriteAsync("{\"format\":999,\"records\":[]}"u8.ToArray());
        blobs.Store["s"] = (ms.ToArray(), 1);

        var snap = await store.LoadAsync("s", new ListSource(R("a", 1)));

        Assert.False(snap.FromCache);
    }
}
