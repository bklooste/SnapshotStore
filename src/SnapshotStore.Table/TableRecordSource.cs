using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

using Azure.Data.Tables;

namespace SnapshotStore.Table;

/// <summary>
/// <see cref="IRecordSource{T}"/> over Azure Table Storage — the common two-level shape this library was
/// built for: Table as the system of record, <c>AzureBlobSnapshotStore</c> (SnapshotStore.Azure) as the
/// cached snapshot on top of it.
/// </summary>
/// <remarks>
/// <para>
/// Records are held one per row (<c>RowKey</c> = the record's <see cref="ISnapshotRecord.Key"/>, URL-encoded
/// so any key is a valid RowKey — Table forbids <c>/ \ # ?</c> and control characters in keys), under a
/// single fixed <c>PartitionKey</c> by default. The JSON payload lives in one string property; each record's
/// own <see cref="ISnapshotRecord.Timestamp"/> is stored again in a queryable <c>long</c> property and is
/// what "changed since" filters on — deliberately not Table's own service-assigned system Timestamp, which
/// would let the query bound and the record's reported timestamp disagree.
/// </para>
/// <para>
/// Table's continuation-token paging already guarantees a stable filtered query returns every matching row
/// exactly once, so — unlike the Redis record source — no extra keyset-pagination bookkeeping is needed here.
/// </para>
/// <para>
/// Table Storage's own limits apply: a property (the JSON payload included) is capped at 64 KB, an entity at
/// 1 MB total. This backing suits many small-to-medium records, not a few huge ones — that shape wants the
/// core's <c>ISnapshotBlobStore</c> side (Blob) directly instead.
/// </para>
/// </remarks>
/// <typeparam name="T">The record type.</typeparam>
/// <param name="table">The table records live in; the caller owns its client/credentials.</param>
/// <param name="recordTypeInfo">Metadata for <typeparamref name="T"/>; take it from a source-generated context to stay AOT-safe.</param>
/// <param name="partitionKey">Every record's <c>PartitionKey</c>. Use a separate <see cref="TableRecordSource{T}"/> per logical partition if you want more than one.</param>
/// <param name="pageSizeHint">Rows requested per page from the service.</param>
public sealed class TableRecordSource<T>(TableClient table, JsonTypeInfo<T> recordTypeInfo, string partitionKey = "records", int pageSizeHint = 1000)
    : IRecordSource<T> where T : ISnapshotRecord
{
    private const string DataProperty = "Data";
    private const string TimestampProperty = "RecordTimestampTicks";

    /// <summary>Publishes (or replaces) one record. Safe to call concurrently from multiple producers.</summary>
    public async Task WriteAsync(T record, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(record);
        var entity = new TableEntity(partitionKey, EncodeRowKey(record.Key))
        {
            [DataProperty] = JsonSerializer.Serialize(record, recordTypeInfo),
            [TimestampProperty] = record.Timestamp.UtcTicks,
        };
        await table.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IReadOnlyList<T>> ReadSinceAsync(DateTimeOffset? since, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // The property name is written literally, NOT as an interpolation hole: CreateQueryFilter quotes and
        // escapes every {…} hole as a VALUE. Put a column name in one and it becomes a string-literal
        // comparison ('RecordTimestampTicks' ge 123) that matches nothing usefully — found by testing against
        // a real Azurite, not by reading the code; the generated filter text looks plausible either way.
        var filter = since is null
            ? TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey}")
            : TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey} and RecordTimestampTicks ge {since.Value.UtcTicks}");

        var pages = table.QueryAsync<TableEntity>(filter, maxPerPage: pageSizeHint, cancellationToken: cancellationToken).AsPages();
        await foreach (var page in pages.WithCancellation(cancellationToken))
        {
            var records = new List<T>(page.Values.Count);
            foreach (var entity in page.Values)
                if (entity.TryGetValue(DataProperty, out var json) && json is string s)
                    records.Add(JsonSerializer.Deserialize(s, recordTypeInfo)!);
            yield return records;
        }
    }

    // Table RowKey forbids '/', '\', '#', '?' and control characters — URL-encode so any Key is valid.
    private static string EncodeRowKey(string key) => Uri.EscapeDataString(key);
}
