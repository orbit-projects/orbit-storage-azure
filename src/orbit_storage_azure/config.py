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
"""Validated Azure account, container, endpoint, and transfer settings."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

from orbit_storage import StorageConfigurationError

_CONTAINER_NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\Z")


@dataclass(frozen=True, slots=True)
class AzureBlobConfig:
    """Azure Blob Storage settings; secrets are supplied by Azure Identity, not stored here."""

    account_url: str
    container: str
    allow_insecure_http: bool = False
    request_timeout_seconds: float = 30.0
    max_block_size_bytes: int = 4 * 1024 * 1024
    max_concurrent_transfers: int = 4
    max_transfer_concurrency: int = 2

    def __post_init__(self) -> None:
        """Reject credential-bearing URLs and unsafe or unbounded transfer options."""
        self._validate_account_url()
        if (
            not isinstance(self.container, str)
            or not 3 <= len(self.container) <= 63
            or _CONTAINER_NAME.fullmatch(self.container) is None
        ):
            raise StorageConfigurationError(
                "Azure container must be 3-63 lowercase letters, digits, or internal hyphens."
            )
        if not isinstance(self.allow_insecure_http, bool):
            raise StorageConfigurationError("allow_insecure_http must be a boolean.")
        try:
            valid_timeout = (
                isinstance(self.request_timeout_seconds, (int, float))
                and not isinstance(self.request_timeout_seconds, bool)
                and math.isfinite(self.request_timeout_seconds)
                and 0 < self.request_timeout_seconds <= 300
            )
        except OverflowError:
            valid_timeout = False
        if not valid_timeout:
            raise StorageConfigurationError(
                "request_timeout_seconds must be finite and between 0 and 300."
            )
        if (
            isinstance(self.max_block_size_bytes, bool)
            or not isinstance(self.max_block_size_bytes, int)
            or not 1 <= self.max_block_size_bytes <= 100 * 1024 * 1024
        ):
            raise StorageConfigurationError(
                "max_block_size_bytes must be between 1 byte and 100 MiB."
            )
        for name, value in (
            ("max_concurrent_transfers", self.max_concurrent_transfers),
            ("max_transfer_concurrency", self.max_transfer_concurrency),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 64:
                raise StorageConfigurationError(f"{name} must be between 1 and 64.")

    def _validate_account_url(self) -> None:
        """Require HTTPS unless explicitly using a local HTTP emulator endpoint."""
        try:
            parsed = urlsplit(self.account_url)
            host = parsed.hostname
            port = parsed.port
            valid = (
                isinstance(self.account_url, str)
                and bool(host)
                and parsed.scheme in {"https", "http"}
                and parsed.username is None
                and parsed.password is None
                and not parsed.query
                and not parsed.fragment
                and (port is None or 1 <= port <= 65535)
                and (
                    parsed.scheme == "https"
                    or (
                        self.allow_insecure_http
                        and host is not None
                        and host.lower() in {"localhost", "127.0.0.1", "::1", "azurite"}
                    )
                )
            )
        except (AttributeError, TypeError, ValueError):
            valid = False
        if not valid:
            raise StorageConfigurationError(
                "Azure account_url must be a valid secure URL without embedded credentials."
            )


__all__ = ["AzureBlobConfig"]
