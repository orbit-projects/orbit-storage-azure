# Orbit Storage Azure: architecture and boundaries

## Responsibility

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

The adapter configures Azure Storage's `ExponentialRetry` policy with randomized jitter. Retry
count and timing are bounded in `AzureBlobConfig`; retries apply only to failures the SDK classifies
as transient and can extend request latency. The provider SDK owns HTTP retry execution.

Uploads accept bytes or an async iterable and use the SDK's asynchronous block upload. Download
responses are streamed and must be closed with `async with` or `aclose()`. The adapter requests the
download with the ETag returned by its metadata lookup, so a concurrent update fails rather than
pairing new bytes with stale metadata. Azure container listing uses bounded page sizes and opaque
continuation tokens. Container provisioning, IAM/RBAC, versioning, soft-delete, retention,
redundancy, and encryption policy remain the application's or cloud operator's responsibility.

The adapter limits simultaneous uploads and open download streams with one shared transfer
semaphore. `max_block_size_bytes × max_concurrent_transfers × max_transfer_concurrency` is capped
at 64 MiB to bound SDK block buffering; caller-owned source chunks and downstream consumer buffers
are outside that budget. Close each download stream promptly so it releases its transfer slot.

Unit tests use a fake SDK surface. No Azure account, Azurite emulator, or live cloud validation is
claimed. The [Azure async SDK guide](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-python-get-started)
describes async client ownership; the [SDK API](https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.aio?view=azure-python)
documents asynchronous upload, list, and chunked download operations.

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `azure-identity>=1.26,<2`
- `azure-storage-blob[aio]>=12.31,<13`
- `orbit-storage>=0.1.0a1,<0.2`
- Optional `core` extra: `orbit-core>=0.1.0a1,<0.2` for `AzureBlobStorePlugin`.
- Development dependencies: Core for lifecycle tests, plus pytest, pytest-asyncio, Ruff, and mypy.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_storage_azure/__init__.py`
- `src/orbit_storage_azure/config.py`
- `src/orbit_storage_azure/plugin.py`
- `src/orbit_storage_azure/store.py`

## Public contract and scope

## Status

This package is alpha; its API is not stable and it is not yet published. It supports Python
3.11 through 3.14. Licensed under Apache-2.0.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
