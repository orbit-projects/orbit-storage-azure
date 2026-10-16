# Orbit Storage Azure

`orbit-storage-azure` is an optional Azure Blob Storage adapter for the provider-neutral
`orbit-storage` capability. It lives outside Orbit Core and is installed only by applications that
select this provider.
`AzureBlobObjectStore` works directly with `orbit-storage`; `AzureBlobStorePlugin` is an optional
Core integration from `orbit_storage_azure.plugin`.

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
It also contributes a read-only **Check Azure container access** action to Orbit CLI. The action
lists at most one blob without displaying names; the configured identity therefore needs permission
to list blobs in the selected container.

The Blob client uses Azure Storage's exponential retry policy with randomized jitter for retryable
transient and throttling responses. `retry_total` defaults to 3 retries (in addition to the initial
request) and is bounded from 0 to 10; initial backoff, growth, and jitter have explicit finite
bounds and a combined maximum backoff of 180 seconds. These settings apply to SDK requests and can
increase operation latency. See Microsoft's
[Azure Storage retry policy guidance](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-retry-policy-python).

Uploads accept bytes or an async iterable and use the SDK's asynchronous block upload. Download
responses are streamed and must be closed with `async with` or `aclose()`. The adapter requests the
download with the ETag returned by its metadata lookup, so a concurrent update fails rather than
pairing new bytes with stale metadata. Azure container listing uses bounded page sizes and opaque
continuation tokens. Container provisioning, IAM/RBAC, versioning, soft-delete, retention,
redundancy, and encryption policy remain the application's or cloud operator's responsibility.

Unit tests use a fake SDK surface. No Azure account, Azurite emulator, or live cloud validation is
claimed. The [Azure async SDK guide](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-python-get-started)
describes async client ownership; the [SDK API](https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.aio?view=azure-python)
documents asynchronous upload, list, and chunked download operations.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e ../orbit-core
python -m pip install -e ../orbit-storage
python -m pip install -e '.[dev]'
pytest
ruff check src tests
mypy
```

## Status

This package is alpha; its API is not stable and it is not yet published. It supports Python
3.11 through 3.14. Licensed under Apache-2.0.

