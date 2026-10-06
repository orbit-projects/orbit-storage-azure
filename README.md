# Orbit Storage Azure

`orbit-storage-azure` is an optional Azure Blob Storage adapter for the provider-neutral
`orbit-storage` capability. It lives outside Orbit Core and is installed only by applications that
select this provider.

```bash
pip install orbit-core orbit-storage orbit-storage-azure
```

```python
from orbit import Application, ApplicationConfig
from orbit_storage_azure import AzureBlobConfig, AzureBlobObjectStore, AzureBlobStorePlugin

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

This package is pre-alpha; its API is not stable and it is not yet published. It supports Python
3.11 through 3.14. Licensed under Apache-2.0.

