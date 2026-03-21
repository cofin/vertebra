import hashlib
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TypeAssets:
    """Container for generated type assets."""

    version: str = ""
    ts_declarations: str = ""
    zod_schemas: str = ""
    routes: str = ""
    manifest: str = ""
    timestamp: float = field(default_factory=time.time)


class TypeBridge:
    """Manages the delivery of generated types to the frontend.

    Provides caching, ETag versioning, and invalidation for TypeScript
    declarations and route manifests.
    """

    def __init__(self, codegen_dir: Path | str):
        self.codegen_dir = Path(codegen_dir)
        self._cache: TypeAssets | None = None

    def get_assets(self) -> TypeAssets:
        """Return the current type assets, rebuilding cache if needed."""
        if self._cache is not None:
            return self._cache

        # 1. Load files from codegen dir
        ts_path = self.codegen_dir / "types.d.ts"
        zod_path = self.codegen_dir / "schemas.zod.ts"
        routes_path = self.codegen_dir / "routes.ts"
        manifest_path = self.codegen_dir / "manifest.json"

        ts_content = ts_path.read_text() if ts_path.exists() else ""
        zod_content = zod_path.read_text() if zod_path.exists() else ""
        routes_content = routes_path.read_text() if routes_path.exists() else ""
        manifest_content = manifest_path.read_text() if manifest_path.exists() else "{}"

        # 2. Generate version hash from contents
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

    @property
    def version(self) -> str:
        """Return the current assets version hash."""
        return self.get_assets().version
