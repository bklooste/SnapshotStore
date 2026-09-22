using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace SnapshotStore.Azure;

/// <summary>
/// <see cref="ISnapshotBlobStore"/> over an Azure blob container. Writes are conditional on the blob's ETag,
/// so a slow writer cannot overwrite a newer snapshot with an older one.
/// </summary>
/// <param name="container">The container snapshots live in; the caller owns its credentials.</param>
public sealed class AzureBlobSnapshotStore(BlobContainerClient container) : ISnapshotBlobStore
{
    /// <inheritdoc />
    public async Task<SnapshotBlob?> TryReadAsync(string name, CancellationToken cancellationToken)
    {
        try
        {
            var result = await container.GetBlobClient(name).DownloadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            return new SnapshotBlob(result.Value.Content, result.Value.Details.ETag.ToString());
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public async Task<bool> TryWriteAsync(string name, Stream content, string? ifMatchVersion, CancellationToken cancellationToken)
    {
        var condition = ifMatchVersion is null
            ? new BlobRequestConditions { IfNoneMatch = ETag.All }
            : new BlobRequestConditions { IfMatch = new ETag(ifMatchVersion) };

        try
        {
            await container.GetBlockBlobClient(name)
                .UploadAsync(content, new BlobUploadOptions { Conditions = condition }, cancellationToken)
                .ConfigureAwait(false);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status is 409 or 412)
        {
            return false;
        }
    }
}
