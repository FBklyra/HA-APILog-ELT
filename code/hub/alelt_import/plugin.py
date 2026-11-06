"""ImportPlugin SPI: discover a legacy platform read-only and map it to a MigrationPlan.

Plugins are vendor packages registered in config/extensions.yaml (`import_plugins: {name: 'module:Class'}`);
the core ships only the generic file-based `yaml_manifest` plugin. Applying a plan is core logic (hub), never
plugin code, so a plugin cannot write anywhere.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from alelt_common.util import load

from .models import CredentialField, InventoryModel, MigrationPlan, PluginInfo

if TYPE_CHECKING:
    from .files import Bundle

log = logging.getLogger("alelt_import")
REPO = Path(__file__).resolve().parents[2]


class DiscoveryError(RuntimeError):
    """Discovery failed (bad credentials, platform unreachable). Message must not contain secrets."""


class ImportPlugin(ABC):
    name: ClassVar[str]
    display_name: ClassVar[str]
    description: ClassVar[str] = ""
    credential_fields: ClassVar[list[CredentialField]] = []
    output_profile: ClassVar[str | None] = None       # profile matching the legacy landing contract
    role_map: ClassVar[dict[str, list[str]]] = {}     # legacy role name -> ALELT roles
    accepts_files: ClassVar[bool] = False              # offline import from exported files
    file_hint: ClassVar[str] = ""                      # what to export / drop (shown in the console)
    live: ClassVar[bool] = True                         # supports discover(credentials)

    def info(self) -> PluginInfo:
        return PluginInfo(name=self.name, display_name=self.display_name, description=self.description,
                          credential_fields=list(self.credential_fields), accepts_files=self.accepts_files,
                          file_hint=self.file_hint, live=self.live)

    def endpoints(self, credentials: Mapping[str, str]) -> list[str]:
        """Hosts/URLs discover() would contact (checked against the production deny list first)."""
        return []

    def discover_files(self, bundle: Bundle) -> InventoryModel:
        """Offline inventory from exported files (no network)."""
        raise DiscoveryError(f"{self.display_name} does not support file import")

    @abstractmethod
    def discover(self, credentials: Mapping[str, str]) -> InventoryModel:
        """Read-only inventory. Credentials are used for this call only and must not be retained."""

    def map(self, inventory: InventoryModel, sources: Mapping[str, str] | None = None) -> MigrationPlan:
        """Default: the core mapper (override to adjust, then call super())."""
        from .plan import build_plan

        return build_plan(inventory, sources=sources, output_profile=self.output_profile, role_map=self.role_map)


def plugins(repo_root: str | Path = REPO) -> dict[str, ImportPlugin]:
    """Core plugins + extension plugins from config/extensions.yaml; a broken extension is logged and skipped."""
    from alelt_schema.config import load_extensions

    from .manifest import ManifestPlugin

    out: dict[str, ImportPlugin] = {ManifestPlugin.name: ManifestPlugin()}
    for ext in load_extensions(repo_root):
        for name, ref in sorted(ext.import_plugins.items()):
            try:
                p = load(ref)()
                if not isinstance(p, ImportPlugin):
                    raise TypeError(f"{ref} is not an ImportPlugin")
                out[name] = p
            except Exception as e:  # noqa: BLE001
                log.warning("import plugin %s unavailable: %s", ref, e, extra={"component": "alelt_import"})
    return out
