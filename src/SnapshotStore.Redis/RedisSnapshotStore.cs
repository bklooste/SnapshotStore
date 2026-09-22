using StackExchange.Redis;

namespace SnapshotStore.Redis;

/// <summary>
/// <see cref="ISnapshotBlobStore"/> over a Redis hash: one hash per snapshot, holding the compressed bytes
/// plus a version counter beside them. Writes are conditional on that counter via a Lua script, so — like
/// <c>AzureBlobSnapshotStore</c>'s ETag — a slow writer cannot overwrite a newer snapshot with an older one.
/// </summary>
/// <param name="database">The database snapshots live in; the caller owns its connection.</param>
/// <param name="keyPrefix">Prepended to every snapshot's hash key. Give each environment/service its own so
/// several can share one Redis without colliding (the lesson of RedisEvents' key-namespacing fix).</param>
/// <param name="maxSnapshotBytes">
/// A snapshot larger than this is not written — <see cref="TryWriteAsync"/> returns false, exactly as if
/// another writer had won the race, so <c>SnapshotStore&lt;T&gt;</c> just keeps re-reading the delta on every
/// load instead of growing one Redis value without bound. Zero or negative disables the cap. Default 4 MiB.
/// </param>
public sealed class RedisSnapshotStore(IDatabase database, string? keyPrefix = null, long maxSnapshotBytes = 4 * 1024 * 1024) : ISnapshotBlobStore
{
    private const string ValueField = "value";
    private const string VersionField = "version";

    // Atomic compare-and-swap over the pair of fields. expectedVersion == "" means "write only if the hash
    // does not exist yet" (mirrors ISnapshotBlobStore.TryWriteAsync's ifMatchVersion == null). Returns 1 on
    // a successful write, 0 when the expected version no longer matches (or did not exist).
    //
    // Every reference is `@name`, not a literal KEYS[n]/ARGV[n]: LuaScript.Prepare assigns each @name to
    // KEYS or ARGV for us, based on whether the .NET parameter passed for it is a RedisKey or a RedisValue
    // (mixing the two styles makes the KEYS/ARGV arrays StackExchange.Redis builds disagree with what the
    // script text expects, and Redis rejects the call).
    private static readonly LuaScript CasScript = LuaScript.Prepare(
        """
        local exists = redis.call('HEXISTS', @snapshotKey, 'version')
        if @expectedVersion == '' then
            if exists == 1 then return 0 end
            redis.call('HSET', @snapshotKey, 'value', @value, 'version', 1)
            return 1
        end
        if exists == 0 then return 0 end
        local current = redis.call('HGET', @snapshotKey, 'version')
        if current ~= @expectedVersion then return 0 end
        redis.call('HSET', @snapshotKey, 'value', @value, 'version', tostring(tonumber(current) + 1))
        return 1
        """);

    private RedisKey Key(string name) => (RedisKey)(keyPrefix + name);

    /// <inheritdoc />
    public async Task<SnapshotBlob?> TryReadAsync(string name, CancellationToken cancellationToken)
    {
        var values = await database.HashGetAsync(this.Key(name), [ValueField, VersionField]).ConfigureAwait(false);
        if (values[0].IsNull || values[1].IsNull)
            return null;

        return new SnapshotBlob(new MemoryStream((byte[])values[0]!), (string)values[1]!);
    }

    /// <inheritdoc />
    public async Task<bool> TryWriteAsync(string name, Stream content, string? ifMatchVersion, CancellationToken cancellationToken)
    {
        await using var buffer = new MemoryStream();
        await content.CopyToAsync(buffer, cancellationToken).ConfigureAwait(false);

        if (maxSnapshotBytes > 0 && buffer.Length > maxSnapshotBytes)
            return false;

        var result = await database.ScriptEvaluateAsync(CasScript, new { snapshotKey = this.Key(name), value = (RedisValue)buffer.ToArray(), expectedVersion = (RedisValue)(ifMatchVersion ?? "") })
            .ConfigureAwait(false);
        return (int)result == 1;
    }
}
