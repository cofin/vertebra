"""TypeBridge: caching and version hashing for generated frontend type assets."""

import hashlib
import time
from pathlib import Path

import msgspec

__all__ = ("TypeAssets", "TypeBridge")


class TypeAssets(msgspec.Struct, frozen=True, gc=False):
    """Container for generated type assets."""

    version: str = ""
    ts_declarations: str = ""
    zod_schemas: str = ""
    routes: str = ""
    manifest: str = ""
    timestamp: float = msgspec.field(default_factory=time.time)


class TypeBridge:
    """Manage delivery of generated types, Zod schemas, and route manifests."""

    def __init__(self, codegen_dir: Path | str) -> None:
        self.codegen_dir = Path(codegen_dir)
        self._cache: TypeAssets | None = None

    @property
    def version(self) -> str:
        """Return the current assets version hash."""
        return self.get_assets().version

    def get_assets(self) -> TypeAssets:
        """Return the current type assets, rebuilding the cache if needed."""
        if self._cache is not None:
            return self._cache

        ts_path = self.codegen_dir / "types.d.ts"
        zod_path = self.codegen_dir / "schemas.zod.ts"
        routes_path = self.codegen_dir / "routes.ts"
        manifest_path = self.codegen_dir / "manifest.json"

        ts_content = ts_path.read_text() if ts_path.exists() else ""
        zod_content = zod_path.read_text() if zod_path.exists() else ""
        routes_content = routes_path.read_text() if routes_path.exists() else ""
        manifest_content = manifest_path.read_text() if manifest_path.exists() else "{}"

        combined = ts_content + zod_content + routes_content + manifest_content
        version = hashlib.sha256(combined.encode()).hexdigest()[:12]

        self._cache = TypeAssets(
            version=version,
            ts_declarations=ts_content,
            zod_schemas=zod_content,
            routes=routes_content,
            manifest=manifest_content,
        )
        return self._cache

    def invalidate(self) -> None:
        """Clear the asset cache to force a reload on next access."""
        self._cache = None
