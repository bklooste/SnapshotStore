using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

using StackExchange.Redis;

namespace SnapshotStore.Redis;

/// <summary>
/// <see cref="IRecordSource{T}"/> over Redis: a sorted set indexes every record's key by
/// <see cref="ISnapshotRecord.Timestamp"/> (so a "changed since" read is a range query), and a hash holds
/// each record's JSON. <see cref="WriteAsync"/> is how a producer publishes into it.
/// </summary>
/// <remarks>
/// Ties (two records with the exact same millisecond timestamp) are handled by keyset pagination within the
/// tie band, so a full scan never skips a record — see the "same-score wall" handling in
/// <see cref="ReadSinceAsync"/>. Records are never deleted from the index by this class; a source that
/// deletes must emit a tombstone record instead, per <c>ISnapshotRecord</c>'s contract.
/// </remarks>
/// <typeparam name="T">The record type.</typeparam>
/// <param name="database">The database records live in; the caller owns its connection.</param>
/// <param name="name">This source's identity. Becomes the Redis key prefix (index + hash).</param>
/// <param name="recordTypeInfo">Metadata for <typeparamref name="T"/>; take it from a source-generated context to stay AOT-safe.</param>
/// <param name="keyPrefix">Prepended to <paramref name="name"/>, same purpose as <see cref="RedisSnapshotStore"/>'s.</param>
/// <param name="pageSize">Records fetched per round trip.</param>
public sealed class RedisRecordSource<T>(IDatabase database, string name, JsonTypeInfo<T> recordTypeInfo, string? keyPrefix = null, int pageSize = 500)
    : IRecordSource<T> where T : ISnapshotRecord
{
    private readonly RedisKey indexKey = (RedisKey)(keyPrefix + name + ":index");
    private readonly RedisKey hashKey = (RedisKey)(keyPrefix + name + ":records");

    /// <summary>Publishes (or replaces) one record. Safe to call concurrently from multiple producers.</summary>
    public async Task WriteAsync(T record, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(record);
        var json = JsonSerializer.SerializeToUtf8Bytes(record, recordTypeInfo);
        var batch = database.CreateBatch();
        var t1 = batch.HashSetAsync(this.hashKey, record.Key, json);
        var t2 = batch.SortedSetAddAsync(this.indexKey, record.Key, record.Timestamp.ToUnixTimeMilliseconds());
        batch.Execute();
        await Task.WhenAll(t1, t2).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IReadOnlyList<T>> ReadSinceAsync(DateTimeOffset? since, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        double cursorScore = since?.ToUnixTimeMilliseconds() ?? double.NegativeInfinity;
        var skipAtCursor = 0L;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entries = await database.SortedSetRangeByScoreWithScoresAsync(
                this.indexKey, cursorScore, double.PositiveInfinity, Exclude.None, Order.Ascending, skipAtCursor, pageSize).ConfigureAwait(false);
            if (entries.Length == 0)
                yield break;

            var values = await database.HashGetAsync(this.hashKey, [.. entries.Select(e => e.Element)]).ConfigureAwait(false);
            var page = new List<T>(entries.Length);
            for (var i = 0; i < entries.Length; i++)
            {
                // A record can be absent from the hash if it was written to the index but the hash write of the
                // same WriteAsync call has not landed yet (the two are not atomic across a crash) — skip it now;
                // it reappears once the write completes, since its score already qualifies for a later read.
                if (!values[i].IsNull)
                    page.Add(JsonSerializer.Deserialize((byte[])values[i]!, recordTypeInfo)!);
            }

            yield return page;

            if (entries.Length < pageSize)
                yield break;

            var lastScore = entries[^1].Score;
            if (lastScore == cursorScore)
            {
                // Still inside the same tie band: extend how much of it we have already emitted.
                skipAtCursor += entries.Length;
            }
            else
            {
                cursorScore = lastScore;
                skipAtCursor = entries.Count(e => e.Score == lastScore);
            }
        }
    }
}
