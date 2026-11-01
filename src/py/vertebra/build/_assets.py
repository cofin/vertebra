"""Cycle-safe Vite/Rolldown asset manifest resolution, SRI/CSP tag rendering, and 103 Early Hints."""

from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Any

import msgspec

from vertebra.utils.serializers import decode_json

__all__ = (
    "PROTECTED_METADATA_FILENAMES",
    "AssetManifest",
    "EarlyHintsSpec",
    "ManifestEntry",
    "ResolvedAssetTags",
    "escape_html_json",
    "is_fingerprinted_asset",
    "is_protected_build_metadata_path",
)

PROTECTED_METADATA_FILENAMES: frozenset[str] = frozenset(
    {
        "manifest.json",
        ".vite/manifest.json",
        "ssr-manifest.json",
        ".vite/ssr-manifest.json",
        "hot",
        ".vertebra/importmap.json",
        ".litestar.json",
    }
)

_FINGERPRINTED_RE = re.compile(r"-[A-Za-z0-9_-]{8,}\.[A-Za-z0-9]+$")


def escape_html_json(raw_json: str) -> str:
    """Escape HTML-sensitive characters in JSON strings embedded inside <script> tags."""
    return (
        raw_json.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def is_protected_build_metadata_path(path: str) -> bool:
    """Return True when path targets a protected build metadata or dev-server state file."""
    cleaned = path.strip("/").replace("\\", "/")
    if cleaned in PROTECTED_METADATA_FILENAMES:
        return True
    return any(cleaned.endswith(f"/{suffix}") for suffix in PROTECTED_METADATA_FILENAMES)


def is_fingerprinted_asset(path: str) -> bool:
    """Return True when path has a content-hash fingerprint before its extension."""
    return bool(_FINGERPRINTED_RE.search(path))


class ManifestEntry(
    msgspec.Struct,
    frozen=True,
    gc=False,
    rename={"is_entry": "isEntry", "is_dynamic_entry": "isDynamicEntry", "dynamic_imports": "dynamicImports"},
):
    """Single entry in a Vite or Rolldown build manifest.json."""

    file: str
    src: str | None = None
    is_entry: bool = False
    is_dynamic_entry: bool = False
    imports: tuple[str, ...] = ()
    dynamic_imports: tuple[str, ...] = ()
    css: tuple[str, ...] = ()
    assets: tuple[str, ...] = ()
    integrity: str | None = None


class EarlyHintsSpec(msgspec.Struct, frozen=True, gc=False):
    """HTTP 103 Early Hints Link header and ASGI http.response.early_hint specification."""

    links: tuple[str, ...] = ()

    def to_link_header(self) -> str:
        """Join all preload links into a single HTTP Link header value."""
        return ", ".join(self.links)

    def to_asgi_event(self) -> dict[str, Any]:
        """Build a spec-compliant ASGI http.response.early_hint event dictionary."""
        return {
            "type": "http.response.early_hint",
            "links": [link.encode("latin-1") for link in self.links],
        }


class ResolvedAssetTags(msgspec.Struct, frozen=True, gc=False):
    """Resolved stylesheet, modulepreload, and module script tags for an entrypoint."""

    entry_file: str
    css_files: tuple[str, ...] = ()
    preload_files: tuple[str, ...] = ()
    dynamic_preload_files: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    early_hints: EarlyHintsSpec = msgspec.field(default_factory=EarlyHintsSpec)

    def to_html(self) -> str:
        """Join all resolved HTML tags with newlines."""
        return "\n".join(self.tags)


class AssetManifest(msgspec.Struct, frozen=True, gc=False):
    """Parsed Vite/Rolldown asset manifest with cycle-safe recursive dependency resolution."""

    entries: dict[str, ManifestEntry] = msgspec.field(default_factory=dict)

    @classmethod
    def from_bytes(cls, data: bytes | str) -> AssetManifest:
        """Decode a JSON manifest payload into an AssetManifest."""
        entries: dict[str, ManifestEntry] = decode_json(data, type=dict[str, ManifestEntry])
        return cls(entries=entries)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> AssetManifest:
        """Convert a dictionary of manifest entries into an AssetManifest."""
        entries: dict[str, ManifestEntry] = {}
        for key, val in raw.items():
            if isinstance(val, ManifestEntry):
                entries[key] = val
            elif isinstance(val, dict):
                entries[key] = msgspec.convert(val, type=ManifestEntry)
        return cls(entries=entries)

    @classmethod
    def from_file(cls, path: Path | str) -> AssetManifest:
        """Load an AssetManifest from disk, probing .vite/manifest.json when given a directory."""
        target = Path(path)
        if target.is_dir():
            v5 = target / ".vite" / "manifest.json"
            target = v5 if v5.is_file() else (target / "manifest.json")
        if not target.is_file():
            return cls()
        return cls.from_bytes(target.read_bytes())

    def resolve_entry(
        self,
        entry_name: str,
        *,
        asset_url: str = "/static/",
        csp_nonce: str | None = None,
        preload_dynamic: bool = False,
    ) -> ResolvedAssetTags:
        """Resolve an entrypoint with cycle-safe recursive imports and CSS collection."""
        root = self.entries.get(entry_name)
        if root is None:
            raise KeyError(f"Asset manifest entry not found: {entry_name!r}")

        prefix = asset_url.rstrip("/")
        visited_imports: set[str] = {entry_name}
        css_seen: dict[str, str | None] = {}
        preload_seen: dict[str, str | None] = {}
        dynamic_seen: dict[str, str | None] = {}

        for css_path in root.css:
            css_seen.setdefault(css_path, None)

        def _walk_static(chunk_key: str) -> None:
            if chunk_key in visited_imports:
                return
            visited_imports.add(chunk_key)
            chunk = self.entries.get(chunk_key)
            if chunk is None:
                return
            preload_seen.setdefault(chunk.file, chunk.integrity)
            for css_path in chunk.css:
                css_seen.setdefault(css_path, None)
            for sub_key in chunk.imports:
                _walk_static(sub_key)
            if preload_dynamic:
                for dyn_key in chunk.dynamic_imports:
                    _walk_dynamic(dyn_key)

        visited_dynamic: set[str] = set()

        def _walk_dynamic(chunk_key: str) -> None:
            if chunk_key in visited_dynamic or chunk_key in visited_imports:
                return
            visited_dynamic.add(chunk_key)
            chunk = self.entries.get(chunk_key)
            if chunk is None:
                return
            dynamic_seen.setdefault(chunk.file, chunk.integrity)
            for css_path in chunk.css:
                css_seen.setdefault(css_path, None)
            for sub_key in chunk.imports:
                _walk_dynamic(sub_key)

        for imp_key in root.imports:
            _walk_static(imp_key)
        if preload_dynamic:
            for dyn_key in root.dynamic_imports:
                _walk_dynamic(dyn_key)

        nonce_attr = f' nonce="{html.escape(csp_nonce, quote=True)}"' if csp_nonce else ""
        tags: list[str] = []
        links: list[str] = []

        entry_href = f"{prefix}/{root.file.lstrip('/')}"
        links.append(f"<{entry_href}>; rel=modulepreload; as=script")

        for css_file in css_seen:
            href = f"{prefix}/{css_file.lstrip('/')}"
            tags.append(f'<link rel="stylesheet" href="{html.escape(href, quote=True)}"{nonce_attr}>')
            links.append(f"<{href}>; rel=preload; as=style")

        for preload_file, preload_sri in preload_seen.items():
            href = f"{prefix}/{preload_file.lstrip('/')}"
            sri_attr = (
                f' integrity="{html.escape(preload_sri, quote=True)}" crossorigin="anonymous"' if preload_sri else ""
            )
            tags.append(f'<link rel="modulepreload" href="{html.escape(href, quote=True)}"{sri_attr}{nonce_attr}>')
            links.append(f"<{href}>; rel=modulepreload; as=script")

        for dyn_file, dyn_sri in dynamic_seen.items():
            href = f"{prefix}/{dyn_file.lstrip('/')}"
            sri_attr = f' integrity="{html.escape(dyn_sri, quote=True)}" crossorigin="anonymous"' if dyn_sri else ""
            tags.append(f'<link rel="modulepreload" href="{html.escape(href, quote=True)}"{sri_attr}{nonce_attr}>')

        root_sri = (
            f' integrity="{html.escape(root.integrity, quote=True)}" crossorigin="anonymous"' if root.integrity else ""
        )
        tags.append(
            f'<script type="module" src="{html.escape(entry_href, quote=True)}"{root_sri}{nonce_attr}></script>'
        )

        return ResolvedAssetTags(
            entry_file=entry_href,
            css_files=tuple(f"{prefix}/{c.lstrip('/')}" for c in css_seen),
            preload_files=tuple(f"{prefix}/{p.lstrip('/')}" for p in preload_seen),
            dynamic_preload_files=tuple(f"{prefix}/{d.lstrip('/')}" for d in dynamic_seen),
            tags=tuple(tags),
            early_hints=EarlyHintsSpec(links=tuple(links)),
        )
