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
"""Optional Core plugin that registers and owns the Azure object-store adapter."""

from orbit import Application
from orbit.plugins import Plugin, PluginMetadata
from orbit_storage import OBJECT_STORE_DEPENDENCY_KEY

from orbit_storage_azure.store import AzureBlobObjectStore


class AzureBlobStorePlugin(Plugin):
    """Register the provider under the shared storage key and close it on shutdown."""

    metadata = PluginMetadata(
        name="orbit-storage-azure",
        version="0.1.0a1",
        capabilities=frozenset({"storage.azure-blob"}),
    )

    def __init__(self, store: AzureBlobObjectStore) -> None:
        """Take lifecycle ownership of an explicitly configured Azure adapter."""
        if not isinstance(store, AzureBlobObjectStore):
            raise TypeError("AzureBlobStorePlugin requires an AzureBlobObjectStore instance.")
        self.store = store

    def setup(self, application: Application) -> None:
        """Expose the provider through Orbit Storage's stable dependency key."""
        if not isinstance(application, Application):
            raise TypeError("AzureBlobStorePlugin requires an Orbit Application.")
        application.container.register_instance(OBJECT_STORE_DEPENDENCY_KEY, self.store)

    async def deactivate(self) -> None:
        """Close the Azure SDK client and identity credential during shutdown."""
        await self.store.aclose()


__all__ = ["AzureBlobStorePlugin"]
