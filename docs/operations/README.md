# Orbit Storage Azure: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

The package README does not name `ORBIT_*` variables. Use its typed constructors and application configuration, and confirm exact runtime inputs in the implementation before deployment.

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-storage-azure` is an optional Azure Blob Storage adapter for the provider-neutral
`orbit-storage` capability. It lives outside Orbit Core and is installed only by applications that
select this provider.

```bash
pip install orbit-storage orbit-storage-azure
# Add the Core lifecycle plugin only when using it:
pip install 'orbit-storage-azure[core]'
```

```python
from orbit import Application, ApplicationConfig
from orbit_storage_azure import AzureBlobConfig, AzureBlobObjectStore
from orbit_storage_azure.plugin import AzureBlobStorePlugin

application = Application(ApplicationConfig(name="orders"))
store = AzureBlobObjectStore(
    AzureBlobConfig(
        account_url="https://example.blob.core.windows.net",
        container="orders-data",
    )
)
application.plugins.register(AzureBlobStorePlugin(store))
```

The adapter uses Azure's async Blob SDK, authenticates through `DefaultAzureCredential`, and owns
the SDK credential/client lifecycle. Configure workload identity, managed identity, or another
supported Azure Identity source outside the application; no key, SAS token, or connection string is
embedded in Core. The plugin registers the adapter through `orbit-storage`'s shared dependency key.
Shutdown rejects new operations immediately. If either owned resource fails to close, later
`aclose()` calls retry only resources whose close has not succeeded; successfully closed resources
are not closed again.

The client uses Azure Storage `ExponentialRetry` with jitter. Configure `retry_total` from 0 to 10
retries; the initial request is additional. Backoff and jitter are bounded to 180 seconds total. Retried uploads
may have ambiguous outcomes after transport failures, so use stable object keys and verify object
metadata after timeouts when the caller needs to reconcile a write. The official
[Azure Storage retry guide](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-retry-policy-python)
describes retry configuration and defaults.

Uploads accept bytes or an async iterable and use the SDK's asynchronous block upload. Download
responses are streamed and must be closed with `async with` or `aclose()`. The adapter requests the
download with the ETag returned by its metadata lookup, so a concurrent update fails rather than
pairing new bytes with stale metadata. Azure container listing uses bounded page sizes and opaque
continuation tokens. Container provisioning, IAM/RBAC, versioning, soft-delete, retention,
redundancy, and encryption policy remain the application's or cloud operator's responsibility.

Uploads and open download streams share the `max_concurrent_transfers` semaphore. The product of
`max_block_size_bytes`, `max_concurrent_transfers`, and `max_transfer_concurrency` cannot exceed
64 MiB, bounding the SDK's block-transfer buffers. This excludes source chunks allocated by the
caller and buffers held by downstream consumers. Always close streamed downloads to return their
transfer slot.

Unit tests use a fake SDK surface. No Azure account, Azurite emulator, or live cloud validation is
claimed. The [Azure async SDK guide](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-python-get-started)
describes async client ownership; the [SDK API](https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.aio?view=azure-python)
documents asynchronous upload, list, and chunked download operations.

## Status

This package is alpha; its API is not stable and it is not yet published. It supports Python
3.11 through 3.14. Licensed under Apache-2.0.

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
