# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Fake-SDK tests for Azure mapping, async streams, and Orbit lifecycle ownership."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from azure.core import MatchConditions
from orbit import Application, ApplicationConfig
from orbit_storage import (
    OBJECT_STORE_DEPENDENCY_KEY,
    StorageConfigurationError,
    StorageOperationError,
)

from orbit_storage_azure import AzureBlobConfig, AzureBlobObjectStore, AzureBlobStorePlugin


class FakeResponseError(Exception):
    """Azure-shaped HTTP failure with a status code for not-found mapping."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}")


class FakeDownloader:
    """Async chunks result with deterministic cleanup state."""

    def __init__(self, content: bytes) -> None:
        self.content = content
        self.closed = False

    def chunks(self) -> AsyncIterator[bytes]:
        async def iterate() -> AsyncIterator[bytes]:
            if self.content:
                yield self.content[:2]
                if self.content[2:]:
                    yield self.content[2:]

        return iterate()


class FakeBlob:
    """Individual blob handle that records conditional download options."""

    def __init__(self, container: FakeContainer, name: str) -> None:
        self.container = container
        self.name = name

    async def upload_blob(self, data: Any, **kwargs: Any) -> Any:
        return await self.container.upload_blob(name=self.name, data=data, **kwargs)

    async def get_blob_properties(self, **kwargs: Any) -> Any:
        if self.name not in self.container.objects:
            raise FakeResponseError(404)
        return self.container.objects[self.name]

    async def download_blob(self, **kwargs: Any) -> FakeDownloader:
        if self.name not in self.container.objects:
            raise FakeResponseError(404)
        self.container.download_options = kwargs
        return FakeDownloader(self.container.payloads[self.name])

    async def exists(self, **kwargs: Any) -> bool:
        return self.name in self.container.objects


class FakePageIterator:
    """One-page Azure listing iterator with continuation-token behavior."""

    def __init__(self, container: FakeContainer, params: dict[str, Any]) -> None:
        self.container = container
        self.params = params
        self.continuation_token: str | None = None
        self._start_token: str | None = None
        self._fetched = False

    def by_page(self, continuation_token: str | None = None) -> FakePageIterator:
        self._start_token = continuation_token
        return self

    def __aiter__(self) -> FakePageIterator:
        return self

    async def __anext__(self) -> AsyncIterator[Any]:
        if self._fetched:
            raise StopAsyncIteration
        self._fetched = True
        names = sorted(
            name
            for name in self.container.objects
            if name.startswith(self.params["name_starts_with"])
        )
        start = int(self._start_token or "0")
        page = names[start : start + self.params["results_per_page"]]
        next_offset = start + len(page)
        self.continuation_token = str(next_offset) if next_offset < len(names) else None

        async def items() -> AsyncIterator[Any]:
            for name in page:
                yield self.container.objects[name]

        return items()


class FakeContainer:
    """Container-like in-memory implementation of the async Blob API."""

    def __init__(self) -> None:
        self.objects: dict[str, Any] = {}
        self.payloads: dict[str, bytes] = {}
        self.download_options: dict[str, Any] | None = None
        self.last_list_params: dict[str, Any] | None = None

    def get_blob_client(self, blob: str) -> FakeBlob:
        return FakeBlob(self, blob)

    async def upload_blob(self, name: str, data: Any, **kwargs: Any) -> dict[str, Any]:
        if isinstance(data, bytes):
            payload = data
        else:
            chunks = bytearray()
            async for chunk in data:
                chunks.extend(chunk)
            payload = bytes(chunks)
        self.payloads[name] = payload
        settings = kwargs["content_settings"]
        properties = SimpleNamespace(
            name=name,
            size=len(payload),
            etag='"azure-etag"',
            last_modified=datetime.now(UTC),
            metadata=kwargs["metadata"],
            content_settings=settings,
        )
        self.objects[name] = properties
        return {"etag": properties.etag, "last_modified": properties.last_modified}

    def list_blobs(self, **kwargs: Any) -> FakePageIterator:
        self.last_list_params = kwargs
        return FakePageIterator(self, kwargs)

    async def delete_blob(self, blob: str, **kwargs: Any) -> None:
        if blob not in self.objects:
            raise FakeResponseError(404)
        del self.objects[blob]
        del self.payloads[blob]


class FakeCredential:
    """Minimal closeable token credential used without external Azure accounts."""

    def __init__(self) -> None:
        self.closed = False

    async def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        return SimpleNamespace(token="fake-token", expires_on=4_000_000_000)

    async def close(self) -> None:
        self.closed = True


class FakeService:
    """Service client wrapper with container and shutdown behavior."""

    def __init__(self, container: FakeContainer) -> None:
        self.container = container
        self.closed = False
        self.container_name: str | None = None

    def get_container_client(self, container: str) -> FakeContainer:
        self.container_name = container
        return self.container

    async def close(self) -> None:
        self.closed = True


def make_store(
    container: FakeContainer, **config: Any
) -> tuple[AzureBlobObjectStore, FakeCredential, FakeService]:
    """Build a test adapter with fake async identity and service clients."""
    settings = {"account_url": "https://storage.example.test", "container": "orbit-test", **config}
    credential = FakeCredential()
    service = FakeService(container)
    store = AzureBlobObjectStore(
        AzureBlobConfig(**settings),
        credential_factory=lambda: credential,
        service_client_factory=lambda *args, **kwargs: service,
    )
    return store, credential, service


def test_config_rejects_credentials_in_endpoint_and_invalid_container() -> None:
    with pytest.raises(StorageConfigurationError, match="secure URL"):
        AzureBlobConfig(account_url="https://user:secret@example.test", container="valid-name")
    with pytest.raises(StorageConfigurationError, match="container"):
        AzureBlobConfig(account_url="https://storage.example.test", container="Bad_Name")
    with pytest.raises(StorageConfigurationError, match="secure URL"):
        AzureBlobConfig(
            account_url="http://127.0.0.1:10000", container="valid", allow_insecure_http=False
        )
    with pytest.raises(StorageConfigurationError, match="secure URL"):
        AzureBlobConfig(
            account_url="http://storage.example.test", container="valid", allow_insecure_http=True
        )
    assert AzureBlobConfig(
        account_url="http://127.0.0.1:10000", container="valid", allow_insecure_http=True
    ).allow_insecure_http


@pytest.mark.asyncio
async def test_put_get_stream_and_delete_follow_storage_contract() -> None:
    container = FakeContainer()
    store, credential, service = make_store(container)
    info = await store.put(
        "reports/today.json", b"hello", content_type="application/json", metadata={"team": "ops"}
    )
    assert info.size_bytes == 5
    assert info.etag == '"azure-etag"'
    assert dict(info.metadata) == {"team": "ops"}
    stored = await store.get("reports/today.json")
    assert stored is not None
    async with stored:
        assert b"".join([chunk async for chunk in stored.body]) == b"hello"
    assert container.download_options is not None
    assert container.download_options["etag"] == '"azure-etag"'
    assert container.download_options["match_condition"] is MatchConditions.IfNotModified
    assert await store.delete("reports/today.json")
    assert not await store.delete("reports/today.json")
    assert await store.get("missing") is None
    await store.aclose()
    assert service.closed and credential.closed


@pytest.mark.asyncio
async def test_async_upload_streams_without_buffering_the_full_object() -> None:
    container = FakeContainer()
    store, _, _ = make_store(container)

    async def chunks() -> AsyncIterator[bytes]:
        yield b"first"
        yield b"-second"

    info = await store.put("streamed", chunks())
    assert info.size_bytes == 12
    assert container.payloads["streamed"] == b"first-second"
    await store.aclose()


@pytest.mark.asyncio
async def test_invalid_stream_chunk_is_rejected() -> None:
    container = FakeContainer()
    store, _, _ = make_store(container)

    async def chunks() -> AsyncIterator[Any]:
        yield "not-bytes"

    with pytest.raises(StorageConfigurationError, match="byte chunks"):
        await store.put("invalid", chunks())
    assert container.objects == {}
    await store.aclose()


@pytest.mark.asyncio
async def test_list_page_uses_prefix_and_opaque_token() -> None:
    container = FakeContainer()
    store, _, _ = make_store(container)
    await store.put("docs/a", b"a")
    await store.put("docs/b", b"b")
    first = await store.list_page("docs/", limit=1)
    assert [item.key for item in first.items] == ["docs/a"]
    assert first.continuation_token == "1"
    second = await store.list_page("docs/", limit=1, continuation_token="1")
    assert [item.key for item in second.items] == ["docs/b"]
    assert container.last_list_params is not None
    assert container.last_list_params["results_per_page"] == 1
    await store.aclose()


@pytest.mark.asyncio
async def test_plugin_closes_service_and_credential_resources() -> None:
    container = FakeContainer()
    store, credential, service = make_store(container)
    app = Application(ApplicationConfig(name="azure-plugin-test"))
    app.plugins.register(AzureBlobStorePlugin(store))
    await app.startup()
    await store.put("registered", b"value")
    assert await app.container.aresolve(OBJECT_STORE_DEPENDENCY_KEY) is store
    await app.stop()
    assert service.closed and credential.closed


@pytest.mark.asyncio
async def test_provider_error_is_sanitized() -> None:
    class BrokenContainer(FakeContainer):
        async def upload_blob(self, name: str, data: Any, **kwargs: Any) -> Any:
            raise RuntimeError("account key: deliberately-secret-value")

    store, _, _ = make_store(BrokenContainer())
    with pytest.raises(StorageOperationError) as error:
        await store.put("safe-name", b"payload")
    assert "deliberately-secret-value" not in str(error.value)
    await store.aclose()


@pytest.mark.asyncio
async def test_real_azure_sdk_client_constructs_and_closes_without_network() -> None:
    credential = FakeCredential()
    store = AzureBlobObjectStore(
        AzureBlobConfig(
            account_url="https://storage.example.test",
            container="orbit-test",
        ),
        credential_factory=lambda: credential,
    )
    await store._get_container()
    await store.aclose()
    assert credential.closed
