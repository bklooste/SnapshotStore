using System.IO.Compression;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace SnapshotStore;

/// <summary>
/// Gzip-compressed JSON snapshots: <c>{"format":1,"highWaterUnixMs":123,"records":[...]}</c>. The format
/// version is written first and checked before any record is parsed, so a future layout change makes old
/// snapshots read as a cache miss rather than as corrupt data.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class JsonGzipSnapshotSerializer<T> : ISnapshotSerializer<T> where T : ISnapshotRecord
{
    /// <summary>The layout version this serializer writes and accepts.</summary>
    public const int FormatVersion = 1;

    private readonly JsonTypeInfo<T> recordTypeInfo;

    /// <summary>Creates a serializer.</summary>
    /// <param name="recordTypeInfo">Metadata for <typeparamref name="T"/>; take it from a source-generated context to stay AOT-safe.</param>
    public JsonGzipSnapshotSerializer(JsonTypeInfo<T> recordTypeInfo)
    {
        ArgumentNullException.ThrowIfNull(recordTypeInfo);
        this.recordTypeInfo = recordTypeInfo;
    }

    /// <inheritdoc />
    public async Task WriteAsync(Stream destination, SnapshotData<T> snapshot, CancellationToken cancellationToken)
    {
        await using var gzip = new GZipStream(destination, CompressionLevel.Optimal, leaveOpen: true);
        await using var writer = new Utf8JsonWriter(gzip);

        writer.WriteStartObject();
        writer.WriteNumber("format", FormatVersion);
        if (snapshot.HighWater is { } highWater)
            writer.WriteNumber("highWaterUnixMs", highWater.ToUnixTimeMilliseconds());
        writer.WriteStartArray("records");
        foreach (var record in snapshot.Records)
        {
            cancellationToken.ThrowIfCancellationRequested();
            JsonSerializer.Serialize(writer, record, this.recordTypeInfo);
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    /// <inheritdoc />
    public async Task<SnapshotData<T>?> ReadAsync(Stream source, CancellationToken cancellationToken)
    {
        await using var gzip = new GZipStream(source, CompressionMode.Decompress, leaveOpen: true);
        using var buffer = new MemoryStream();
        await gzip.CopyToAsync(buffer, cancellationToken).ConfigureAwait(false);

        return this.Parse(buffer.GetBuffer().AsSpan(0, (int)buffer.Length));
    }

    private SnapshotData<T>? Parse(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Snapshot is not a JSON object.");

        var formatChecked = false;
        DateTimeOffset? highWater = null;
        List<T>? records = null;

        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            var property = reader.GetString();
            reader.Read();

            switch (property)
            {
                case "format":
                    if (reader.GetInt32() != FormatVersion)
                        return null;
                    formatChecked = true;
                    break;

                case "highWaterUnixMs":
                    highWater = DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64());
                    break;

                case "records":
                    if (!formatChecked)
                        throw new JsonException("Snapshot has records before its format version.");
                    records = [];
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    {
                        var record = JsonSerializer.Deserialize(ref reader, this.recordTypeInfo)
                            ?? throw new JsonException("Snapshot contains a null record.");
                        records.Add(record);
                    }

                    break;

                default:
                    reader.Skip();
                    break;
            }
        }

        if (!formatChecked || records is null)
            throw new JsonException("Snapshot is missing its format version or records.");

        return new SnapshotData<T>(highWater, records);
    }
}
