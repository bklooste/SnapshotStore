using System.Text.Json.Serialization;

namespace SnapshotStore.Table.Tests;

public record TestRow(string Key, DateTimeOffset Timestamp, string Value) : ISnapshotRecord;

[JsonSerializable(typeof(TestRow))]
internal partial class TestJson : JsonSerializerContext;
