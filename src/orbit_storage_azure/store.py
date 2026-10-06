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
"""Async Azure Blob Storage implementation of Orbit Storage's object contract."""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterable, AsyncIterator, Callable, Mapping
from contextlib import suppress
from typing import Any, Protocol, cast

from azure.core import MatchConditions
from azure.core.credentials_async import AsyncTokenCredential
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob import ContentSettings
from azure.storage.blob.aio import BlobServiceClient
from orbit_storage import (
    ListObjectsRequest,
    ObjectInfo,
    ObjectPage,
    ObjectStore,
    StorageConfigurationError,
    StorageOperationError,
    StoredObject,
    validate_object_key,
)

from orbit_storage_azure.config import AzureBlobConfig


class _Downloader(Protocol):
    """Subset of Azure's async download result used by the body reader."""

    def chunks(self) -> AsyncIterator[bytes]:
        """Return a lazy async iterator over bounded response chunks."""


class _Container(Protocol):
    """Narrow container client surface used by the adapter."""

    def get_blob_client(self, blob: str) -> _Blob:
        """Return a client for the named blob."""
        ...

    async def upload_blob(self, name: str, data: Any, **kwargs: Any) -> Any:
        """Upload a stream under the named blob."""
        ...

    def list_blobs(self, **kwargs: Any) -> Any:
        """Return the provider's paginated blob iterator."""
        ...

    async def delete_blob(self, blob: str, **kwargs: Any) -> None:
        """Delete a blob, raising when the provider reports an error."""
        ...


class _Blob(Protocol):
    """Narrow blob client surface used by the adapter."""

    async def upload_blob(self, data: Any, **kwargs: Any) -> Any:
        """Upload a stream as this blob."""
        ...

    async def get_blob_properties(self, **kwargs: Any) -> Any:
        """Fetch blob metadata and the current entity tag."""
        ...

    async def download_blob(self, **kwargs: Any) -> _Downloader:
        """Start a lazy asynchronous blob download."""
        ...

    async def exists(self, **kwargs: Any) -> bool:
        """Return whether the blob currently exists."""
        ...


class _ServiceClient(Protocol):
    """Narrow service client surface retained for the adapter lifetime."""

    def get_container_client(self, container: str) -> _Container:
        """Return a client for one container."""
        ...

    async def close(self) -> None:
        """Release this SDK client's asynchronous resources."""
        ...


class _CountingBody:
    """Validate yielded bytes and count the total while Azure consumes an async upload."""

    def __init__(self, source: AsyncIterable[bytes]) -> None:
        """Wrap a caller-provided stream without buffering its contents."""
        self._iterator = aiter(source)
        self.size = 0

    def __aiter__(self) -> _CountingBody:
        """Return this counted upload stream."""
        return self

    async def __anext__(self) -> bytes:
        """Return the next non-empty byte block and update the size counter."""
        while True:
            chunk = await anext(self._iterator)
            if not isinstance(chunk, bytes):
                raise StorageConfigurationError("Azure upload streams must yield byte chunks.")
            if chunk:
                self.size += len(chunk)
                return chunk


class _AzureBodyReader:
    """Adapt Azure's async chunk iterator to Orbit's explicitly closeable body reader."""

    def __init__(self, downloader: _Downloader) -> None:
        """Take ownership of the SDK's lazy chunk iterator."""
        self._iterator = downloader.chunks().__aiter__()
        self._closed = False

    def __aiter__(self) -> _AzureBodyReader:
        """Return this asynchronous body reader."""
        return self

    async def __anext__(self) -> bytes:
        """Return the next SDK chunk and release any closeable iterator at EOF."""
        if self._closed:
            raise StopAsyncIteration
        try:
            chunk = await anext(self._iterator)
        except StopAsyncIteration:
            await self.aclose()
            raise
        except asyncio.CancelledError:
            raise
        except Exception:
            await self.aclose()
            raise StorageOperationError("Azure blob download failed while reading.") from None
        if not isinstance(chunk, bytes):
            await self.aclose()
            raise StorageOperationError("Azure returned a non-byte blob chunk.")
        return chunk

    async def aclose(self) -> None:
        """Close the SDK iterator when supported; the service client remains store-owned."""
        if not self._closed:
            close = getattr(self._iterator, "aclose", None)
            if close is not None:
                try:
                    await close()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise StorageOperationError("Azure download stream close failed.") from None
            self._closed = True


class AzureBlobObjectStore(ObjectStore):
    """Lifecycle-managed Azure adapter with async uploads and streamed downloads."""

    def __init__(
        self,
        config: AzureBlobConfig,
        *,
        credential_factory: Callable[[], AsyncTokenCredential] | None = None,
        service_client_factory: Callable[..., _ServiceClient] | None = None,
    ) -> None:
        """Bind options and lazily construct an SDK client with workload identity by default."""
        if not isinstance(config, AzureBlobConfig):
            raise TypeError("AzureBlobObjectStore requires an AzureBlobConfig instance.")
        self.config = config
        self._credential_factory = (
            DefaultAzureCredential if credential_factory is None else credential_factory
        )
        self._service_client_factory = cast(
            Callable[..., _ServiceClient],
            BlobServiceClient if service_client_factory is None else service_client_factory,
        )
        self._credential: AsyncTokenCredential | None = None
        self._service: _ServiceClient | None = None
        self._container: _Container | None = None
        self._client_lock = asyncio.Lock()
        self._transfer_slots = asyncio.Semaphore(config.max_concurrent_transfers)
        self._closed = False

    async def _get_container(self) -> _Container:
        """Create one credential and service client lazily, then retain their container handle."""
        async with self._client_lock:
            if self._closed:
                raise StorageOperationError("Azure blob store is closed.")
            if self._container is None:
                credential: AsyncTokenCredential | None = None
                service: _ServiceClient | None = None
                try:
                    credential = self._credential_factory()
                    service = self._service_client_factory(
                        self.config.account_url,
                        credential=credential,
                        max_block_size=self.config.max_block_size_bytes,
                        max_single_get_size=self.config.max_block_size_bytes,
                        max_chunk_get_size=self.config.max_block_size_bytes,
                    )
                    container = service.get_container_client(self.config.container)
                except asyncio.CancelledError:
                    if service is not None:
                        with suppress(Exception):
                            await service.close()
                    if credential is not None:
                        with suppress(Exception):
                            await credential.close()
                    raise
                except Exception:
                    if service is not None:
                        with suppress(Exception):
                            await service.close()
                    if credential is not None:
                        with suppress(Exception):
                            await credential.close()
                    raise StorageOperationError("Azure client initialization failed.") from None
                self._credential = credential
                self._service = service
                self._container = container
            return self._container

    def _key(self, key: str) -> str:
        """Validate opaque storage keys before sending a provider request."""
        try:
            return validate_object_key(key)
        except ValueError:
            raise StorageConfigurationError("Azure blob key is invalid.") from None

    @staticmethod
    def _status(error: Exception) -> int | None:
        """Extract only the HTTP response status from an Azure SDK exception."""
        status = getattr(error, "status_code", None)
        return status if isinstance(status, int) and not isinstance(status, bool) else None

    @classmethod
    def _not_found(cls, error: Exception) -> bool:
        """Recognize an explicit HTTP 404 as a missing object/container response."""
        return cls._status(error) == 404

    @staticmethod
    def _value(source: object, name: str, default: Any = None) -> Any:
        """Read a property from SDK model objects and mapping-shaped test responses."""
        if isinstance(source, Mapping):
            return source.get(name, default)
        return getattr(source, name, default)

    @classmethod
    def _info(cls, key: str, properties: object) -> ObjectInfo:
        """Normalize Azure properties through the provider-neutral object model."""
        try:
            content_settings = cls._value(properties, "content_settings")
            content_type = cls._value(content_settings, "content_type")
            modified = cls._value(properties, "last_modified")
            metadata = cls._value(properties, "metadata", {}) or {}
            if not isinstance(metadata, Mapping):
                raise ValueError
            return ObjectInfo(
                key=key,
                size_bytes=cls._value(properties, "size"),
                content_type=content_type,
                etag=cls._value(properties, "etag"),
                modified_at=modified,
                metadata=metadata,
            )
        except Exception:
            raise StorageOperationError("Azure returned invalid blob metadata.") from None

    async def put(
        self,
        key: str,
        body: bytes | AsyncIterable[bytes],
        *,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> ObjectInfo:
        """Upload bytes or an async iterable using Azure's bounded block transfer."""
        blob_name = self._key(key)
        try:
            validated = ObjectInfo(
                key=blob_name,
                size_bytes=0,
                content_type=content_type,
                metadata=metadata or {},
            )
        except Exception:
            raise StorageConfigurationError("Azure upload metadata is invalid.") from None
        if isinstance(body, bytes):
            source: bytes | _CountingBody = body
            known_size = len(body)
            counted_source = None
        elif hasattr(body, "__aiter__"):
            counted_source = _CountingBody(body)
            source = counted_source
            known_size = -1
        else:
            raise StorageConfigurationError(
                "Azure upload body must be bytes or an async byte stream."
            )

        async with self._transfer_slots:
            container = await self._get_container()
            blob = container.get_blob_client(blob_name)
            try:
                properties = await blob.upload_blob(
                    data=source,
                    overwrite=True,
                    metadata=dict(validated.metadata),
                    content_settings=ContentSettings(content_type=validated.content_type),
                    max_concurrency=self.config.max_transfer_concurrency,
                    timeout=math.ceil(self.config.request_timeout_seconds),
                )
            except asyncio.CancelledError:
                raise
            except StorageConfigurationError:
                raise
            except Exception:
                raise StorageOperationError("Azure blob upload failed.") from None
        if known_size < 0:
            assert counted_source is not None
            known_size = counted_source.size
        if isinstance(properties, Mapping):
            response = dict(properties)
        else:
            response = {
                "etag": getattr(properties, "etag", None),
                "last_modified": getattr(properties, "last_modified", None),
            }
        response["size"] = known_size
        response["content_settings"] = {"content_type": validated.content_type}
        response["metadata"] = validated.metadata
        return self._info(blob_name, response)

    async def get(self, key: str) -> StoredObject | None:
        """Fetch metadata, then open an ETag-pinned chunk stream for that exact version."""
        blob_name = self._key(key)
        container = await self._get_container()
        blob = container.get_blob_client(blob_name)
        try:
            properties = await blob.get_blob_properties(
                timeout=math.ceil(self.config.request_timeout_seconds)
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._not_found(error):
                return None
            raise StorageOperationError("Azure blob metadata request failed.") from None
        info = self._info(blob_name, properties)
        etag = self._value(properties, "etag")
        if not isinstance(etag, str) or not etag:
            raise StorageOperationError("Azure returned invalid blob ETag metadata.")
        try:
            downloader = await blob.download_blob(
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
                max_concurrency=self.config.max_transfer_concurrency,
                timeout=math.ceil(self.config.request_timeout_seconds),
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._not_found(error):
                return None
            raise StorageOperationError("Azure blob download could not be opened.") from None
        return StoredObject(info=info, body=_AzureBodyReader(downloader))

    async def delete(self, key: str) -> bool:
        """Delete an object, returning false only if the preceding existence check missed."""
        blob_name = self._key(key)
        container = await self._get_container()
        blob = container.get_blob_client(blob_name)
        try:
            exists = await blob.exists(timeout=math.ceil(self.config.request_timeout_seconds))
        except asyncio.CancelledError:
            raise
        except Exception:
            raise StorageOperationError("Azure blob existence check failed.") from None
        if not exists:
            return False
        try:
            await container.delete_blob(
                blob_name,
                timeout=math.ceil(self.config.request_timeout_seconds),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise StorageOperationError("Azure blob delete failed.") from None
        return True

    async def list_page(
        self,
        prefix: str = "",
        *,
        limit: int = 100,
        continuation_token: str | None = None,
    ) -> ObjectPage:
        """Read one bounded container page and return Azure's opaque continuation token."""
        try:
            request = ListObjectsRequest(
                prefix=prefix, limit=limit, continuation_token=continuation_token
            )
        except Exception:
            raise StorageConfigurationError("Azure blob list request is invalid.") from None
        container = await self._get_container()
        try:
            listing = container.list_blobs(
                name_starts_with=request.prefix,
                results_per_page=request.limit,
                timeout=math.ceil(self.config.request_timeout_seconds),
            )
            pages = listing.by_page(continuation_token=request.continuation_token)
            current = await anext(pages)
            items: list[ObjectInfo] = []
            async for properties in current:
                name = self._value(properties, "name")
                if not isinstance(name, str):
                    raise ValueError("Azure list item has no blob name.")
                items.append(self._info(name, properties))
            token = pages.continuation_token
            if token is not None and not isinstance(token, str):
                raise ValueError("Azure continuation token is invalid.")
            return ObjectPage(items=tuple(items), continuation_token=token)
        except StopAsyncIteration:
            return ObjectPage(items=(), continuation_token=None)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise StorageOperationError(
                "Azure blob listing failed or returned invalid data."
            ) from None

    async def aclose(self) -> None:
        """Close the owned service client and Azure credential exactly once."""
        async with self._client_lock:
            if self._closed:
                return
            self._closed = True
            try:
                try:
                    if self._service is not None:
                        await self._service.close()
                finally:
                    if self._credential is not None:
                        await self._credential.close()
            except asyncio.CancelledError:
                self._closed = False
                raise
            except Exception:
                raise StorageOperationError("Azure storage client shutdown failed.") from None


__all__ = ["AzureBlobObjectStore"]
