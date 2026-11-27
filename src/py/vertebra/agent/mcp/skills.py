"""Progressive skill catalog and filesystem disclosure for Model Context Protocol.

This module provides SkillCatalog, FilesystemSkillSource, Skill, and SkillFile
to load and serve skill packages with SHA-256 integrity verification.
"""

import hashlib
import inspect
import mimetypes
import os
import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

import anyio
import anyio.to_thread
import msgspec

from vertebra.agent.mcp.models import MCPIcon, ResourceEntry, coerce_mcp_icons
from vertebra.agent.telemetry import AgentTelemetryRecorder
from vertebra.typing import VertebraError

SKILL_URI_PREFIX: str = "skill://"
SKILL_FILE_NAME: str = "SKILL.md"


class SkillIntegrityError(Exception):
    """Raised when a skill file fails SHA-256 digest verification."""


class SkillAccessError(VertebraError, PermissionError):
    """Raised when skill directory listing or path access violates policy."""


class SkillFile(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Immutable descriptor of a single file in a skill package."""

    path: str
    """Relative POSIX path of the file inside the skill directory."""
    digest: str
    """Expected `sha256:<hex>` integrity digest verified on read."""
    size_bytes: int
    """File size in bytes enforced against `max_bytes_per_skill`."""
    mime_type: str = "text/markdown"
    """Detected IANA media type of the skill file."""
    content_bytes: bytes | None = None
    """Optional in-memory file payload for synthetic skills."""
    real_path: str | None = None
    """Resolved filesystem path for on-disk skill files."""


class Skill(msgspec.Struct, frozen=True, kw_only=True):
    """Immutable representation of an MCP progressive skill package."""

    name: str
    """Unique skill slug used in `skill://<name>` URIs."""
    description: str
    """One-line summary advertised in the compact skill catalog."""
    body: str
    """Full Markdown instructions loaded from `SKILL.md`."""
    title: str | None = None
    """Optional human-readable display title for the skill."""
    tags: tuple[str, ...] = ()
    """Filterable discovery tags extracted from frontmatter or explicit builder options."""
    icons: tuple[MCPIcon, ...] = ()
    """Optional SEP-1865 icon descriptors advertised alongside the skill."""
    meta: dict[str, Any] = msgspec.field(default_factory=dict)
    """Optional ``_meta`` extension payload associated with the skill."""
    files: dict[str, SkillFile] = msgspec.field(default_factory=dict)
    """Map of relative file paths to `SkillFile` descriptors within the skill bundle."""
    frontmatter: dict[str, Any] = msgspec.field(default_factory=dict)
    """Parsed YAML frontmatter metadata from `SKILL.md`."""

    @classmethod
    def build(
        cls,
        name: str,
        description: str,
        *,
        body: str,
        title: str | None = None,
        tags: Sequence[str] = (),
        icons: Sequence[MCPIcon | Mapping[str, Any]] = (),
        meta: Mapping[str, Any] | None = None,
        files: Mapping[str, bytes | str] | None = None,
        frontmatter: Mapping[str, Any] | None = None,
    ) -> "Skill":
        """Construct an in-memory Skill, calculating SHA-256 digests for all files."""
        fm_dict = dict(frontmatter or {})
        raw_fm_title = fm_dict.get("title")
        resolved_title = title if title is not None else (str(raw_fm_title) if raw_fm_title is not None else None)
        resolved_tags = _extract_skill_tags(fm_dict, tags)
        resolved_icons = _extract_skill_icons(fm_dict, icons)
        resolved_meta = _extract_skill_meta(fm_dict, meta)
        built_files: dict[str, SkillFile] = {}
        if files:
            for rel_path, raw in files.items():
                content = raw.encode("utf-8") if isinstance(raw, str) else raw
                digest = f"sha256:{hashlib.sha256(content).hexdigest()}"
                mime, _ = mimetypes.guess_type(rel_path)
                built_files[rel_path] = SkillFile(
                    path=rel_path,
                    digest=digest,
                    size_bytes=len(content),
                    mime_type=mime or "application/octet-stream",
                    content_bytes=content,
                )
        return cls(
            name=name,
            description=description,
            body=body,
            title=resolved_title,
            tags=resolved_tags,
            icons=resolved_icons,
            meta=resolved_meta,
            files=built_files,
            frontmatter=fm_dict,
        )


class SkillSource(Protocol):
    """Protocol for sources providing skill packages."""

    def load_skills(self) -> Sequence[Skill]:
        """Load and return all discovered skills."""
        ...


def parse_skill_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    """Parse YAML-subset frontmatter delimited by triple-dash markers."""
    if not text.startswith("---"):
        return {}, text

    lines = text.splitlines(keepends=True)
    if not lines or lines[0].strip() != "---":
        return {}, text

    closing_idx = -1
    for idx in range(1, len(lines)):
        if lines[idx].strip() == "---":
            closing_idx = idx
            break

    if closing_idx == -1:
        return {}, text

    frontmatter_lines = lines[1:closing_idx]
    body = "".join(lines[closing_idx + 1 :])

    data: dict[str, Any] = {}
    current_key: str | None = None

    for line in frontmatter_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if stripped.startswith("- ") and current_key is not None:
            val = stripped[2:].strip().strip("\"'")
            if not isinstance(data.get(current_key), list):
                data[current_key] = []
            data[current_key].append(val)
            continue

        match = re.match(r"^([a-zA-Z0-9_\-]+)\s*:\s*(.*)$", stripped)
        if match:
            key, val = match.group(1), match.group(2).strip()
            if not val:
                data[key] = []
                current_key = key
            else:
                current_key = None
                if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                    data[key] = val[1:-1]
                elif val.lower() == "true":
                    data[key] = True
                elif val.lower() == "false":
                    data[key] = False
                elif val.isdigit():
                    data[key] = int(val)
                elif re.match(r"^-?\d+\.\d+$", val):
                    data[key] = float(val)
                elif val.startswith("[") and val.endswith("]"):
                    items = [it.strip().strip("\"'") for it in val[1:-1].split(",") if it.strip()]
                    data[key] = items
                else:
                    data[key] = val

    return data, body


class FilesystemSkillSource:
    """Discovers skills from directories on the local filesystem."""

    def __init__(
        self,
        base_paths: Sequence[str | Path],
        max_files_per_skill: int = 128,
        max_bytes_per_skill: int = 2_097_152,
    ) -> None:
        self.base_paths = [_resolve_skill_base_path(p) for p in base_paths]
        self.max_files_per_skill = max_files_per_skill
        self.max_bytes_per_skill = max_bytes_per_skill

    def load_skills(self) -> list[Skill]:
        """Scan directories and load skills with verified digests."""
        skills: list[Skill] = []
        for root in self.base_paths:
            if not root.is_dir():
                continue
            for skill_dir in root.iterdir():
                if not skill_dir.is_dir() or skill_dir.name.startswith("."):
                    continue
                skill_md = skill_dir / SKILL_FILE_NAME
                if not skill_md.is_file() or skill_md.is_symlink():
                    continue

                raw_text = skill_md.read_text(encoding="utf-8")
                frontmatter, body = parse_skill_frontmatter(raw_text)
                name = str(frontmatter.get("name") or skill_dir.name)
                description = str(frontmatter.get("description") or "")

                files: dict[str, SkillFile] = {}
                total_bytes = 0
                file_count = 0

                for path in skill_dir.rglob("*"):
                    if path.is_file() and not path.is_symlink():
                        rel = str(path.relative_to(skill_dir))
                        if ".." in rel:
                            continue
                        file_count += 1
                        if file_count > self.max_files_per_skill:
                            break
                        size = path.stat().st_size
                        total_bytes += size
                        if total_bytes > self.max_bytes_per_skill:
                            break
                        content = path.read_bytes()
                        digest = f"sha256:{hashlib.sha256(content).hexdigest()}"
                        mime, _ = mimetypes.guess_type(str(path))
                        files[rel] = SkillFile(
                            path=rel,
                            digest=digest,
                            size_bytes=size,
                            mime_type=mime or "application/octet-stream",
                            real_path=str(path.resolve()),
                        )

                raw_fm_title = frontmatter.get("title")
                skills.append(
                    Skill(
                        name=name,
                        description=description,
                        body=body,
                        title=str(raw_fm_title) if raw_fm_title is not None else None,
                        tags=_extract_skill_tags(frontmatter),
                        icons=_extract_skill_icons(frontmatter),
                        meta=_extract_skill_meta(frontmatter),
                        files=files,
                        frontmatter=frontmatter,
                    )
                )
        return skills


class MCPSkillsConfig(msgspec.Struct, frozen=True, kw_only=True):
    """Configuration for MCP progressive skill discovery."""

    paths: tuple[str | Path, ...] = (".agents/skills",)
    """Filesystem directories scanned for `<skill>/SKILL.md` packages."""
    sources: tuple[Any, ...] = ()
    """Additional `SkillSource` providers supplying programmatic skills."""
    directory_read: bool = True
    """Allow clients to list relative file paths inside `skill://<name>/` directories."""
    expose_as_prompts: bool = False
    """Project discovered skills into `prompts/list` and `prompts/get` when true."""
    list_changed: bool = True
    """Advertise and emit `notifications/skills/list_changed` when the catalog mutates."""
    max_files_per_skill: int = 128
    """Maximum number of files indexed per skill directory."""
    max_bytes_per_skill: int = 2_097_152
    """Maximum cumulative byte budget (`2 MiB` default) allowed per skill package."""

    def resolved_paths(self) -> tuple[Path, ...]:
        """Return configured skill search paths resolved against ``VERTEBRA_APP_ROOT`` when active."""
        return tuple(_resolve_skill_base_path(p) for p in self.paths)


class SkillCatalog:
    """Catalog of active skills providing progressive disclosure and digest verification."""

    def __init__(
        self,
        skills: Sequence[Skill] = (),
        telemetry: AgentTelemetryRecorder | None = None,
        *,
        sources: Sequence[SkillSource] = (),
        config: MCPSkillsConfig | None = None,
        directory_read: bool | None = None,
        expose_as_prompts: bool | None = None,
        list_changed: bool | None = None,
    ) -> None:
        self.telemetry = telemetry if telemetry is not None else AgentTelemetryRecorder()
        self.config = config
        self._sources: list[SkillSource] = list(sources)
        self.directory_read: bool = (
            directory_read if directory_read is not None else (config.directory_read if config is not None else True)
        )
        self.expose_as_prompts: bool = (
            expose_as_prompts
            if expose_as_prompts is not None
            else (config.expose_as_prompts if config is not None else False)
        )
        self.list_changed: bool = (
            list_changed if list_changed is not None else (config.list_changed if config is not None else True)
        )
        self._listeners: list[Callable[..., Any]] = []
        self._manual_skills: dict[str, Skill] = {s.name: s for s in skills}
        source_skills = self._load_from_sources() if (self._sources and not skills) else {}
        self._skills: dict[str, Skill] = {**source_skills, **self._manual_skills}

    @classmethod
    def coerce(
        cls,
        value: "MCPSkillsConfig | SkillCatalog | None",
        *,
        telemetry: AgentTelemetryRecorder | None = None,
    ) -> "SkillCatalog | None":
        """Normalize ``MCPSkillsConfig | SkillCatalog | None`` into a ``SkillCatalog`` or ``None``."""
        if value is None:
            return None
        if isinstance(value, cls):
            return value
        if isinstance(value, MCPSkillsConfig):
            return cls.from_config(value, telemetry=telemetry)
        raise TypeError(f"Expected MCPSkillsConfig, SkillCatalog, or None; got {type(value).__name__}")

    @classmethod
    def from_config(
        cls,
        config: MCPSkillsConfig,
        telemetry: AgentTelemetryRecorder | None = None,
    ) -> "SkillCatalog":
        """Instantiate a SkillCatalog from an MCPSkillsConfig."""
        sources: list[SkillSource] = [
            FilesystemSkillSource(
                base_paths=config.paths,
                max_files_per_skill=config.max_files_per_skill,
                max_bytes_per_skill=config.max_bytes_per_skill,
            )
        ]
        for src in config.sources:
            if hasattr(src, "load_skills") and callable(src.load_skills):
                sources.append(src)
        return cls(telemetry=telemetry, sources=sources, config=config)

    def add_listener(self, listener: Callable[..., Any]) -> None:
        """Register a listener invoked when the skill catalog changes."""
        self._listeners.append(listener)

    def register_skill(self, skill: Skill) -> None:
        """Register or replace a skill in the catalog and notify listeners if changed."""
        previous = self._skills.get(skill.name)
        self._manual_skills[skill.name] = skill
        self._skills[skill.name] = skill
        if previous != skill:
            self._notify()

    def unregister_skill(self, name: str) -> Skill | None:
        """Remove a skill by name from the catalog, notifying listeners if present."""
        self._manual_skills.pop(name, None)
        removed = self._skills.pop(name, None)
        if removed is not None:
            self._notify()
        return removed

    def refresh(self) -> bool:
        """Reload all configured skill sources and fire listeners strictly when contents changed."""
        if not self._sources:
            return False
        next_skills = {**self._load_from_sources(), **self._manual_skills}
        if next_skills == self._skills:
            return False
        self._skills = next_skills
        self._notify()
        return True

    def list_skills(
        self,
        *,
        tags: Sequence[str] | None = None,
        query: str | None = None,
    ) -> list[Skill]:
        """Return skills sorted by name, optionally filtered by tag intersection and query substring."""
        required_tags = {str(t).strip().lower() for t in tags if str(t).strip()} if tags is not None else set()
        query_norm = query.strip().lower() if query else ""
        matched: list[Skill] = []
        for key in sorted(self._skills.keys()):
            skill = self._skills[key]
            if required_tags:
                skill_tags = {t.lower() for t in _extract_skill_tags(skill.frontmatter, skill.tags)}
                if not required_tags.issubset(skill_tags):
                    continue
            if query_norm:
                title_text = (skill.title or "").lower()
                if (
                    query_norm not in skill.name.lower()
                    and query_norm not in title_text
                    and query_norm not in skill.description.lower()
                ):
                    continue
            matched.append(skill)
        return matched

    def get_skill(self, name_or_uri: str) -> Skill | None:
        """Retrieve a skill by slug name, ``skill://<name>``, or ``skill://<name>/SKILL.md``."""
        direct = self._skills.get(name_or_uri)
        if direct is not None:
            return direct
        if not name_or_uri.startswith(SKILL_URI_PREFIX):
            return None
        raw = name_or_uri.removeprefix(SKILL_URI_PREFIX).strip("/")
        if not raw:
            return None
        if "/" not in raw:
            return self._skills.get(raw)
        skill_name, subpath = raw.split("/", 1)
        if subpath != SKILL_FILE_NAME:
            return None
        return self._skills.get(skill_name)

    def get_file_descriptor(self, uri: str) -> tuple[Skill, SkillFile | None, str] | None:
        """Resolve a ``skill://`` URI to its ``(Skill, SkillFile | None, subpath)`` triple."""
        if not uri.startswith(SKILL_URI_PREFIX):
            return None
        raw = uri.removeprefix(SKILL_URI_PREFIX).strip("/")
        if not raw:
            return None
        parts = raw.split("/", 1)
        skill_name = parts[0]
        subpath = parts[1] if len(parts) > 1 else SKILL_FILE_NAME
        skill = self._skills.get(skill_name)
        if skill is None:
            return None
        return skill, skill.files.get(subpath), subpath

    def resource_entries(self) -> list[ResourceEntry]:
        """Project active skills and visible skill files into ``ResourceEntry`` descriptors."""
        entries: list[ResourceEntry] = []
        for skill in self.list_skills():
            skill_meta = dict(skill.meta) if skill.meta else None
            if not skill.files:
                entries.append(
                    ResourceEntry(
                        uri=f"{SKILL_URI_PREFIX}{skill.name}/{SKILL_FILE_NAME}",
                        name=skill.name,
                        title=skill.title,
                        description=skill.description,
                        mimeType="text/markdown",
                        icons=skill.icons,
                        meta=skill_meta,
                    )
                )
                continue
            for rel_path in sorted(skill.files.keys()):
                if not self.directory_read and rel_path != SKILL_FILE_NAME:
                    continue
                skill_file = skill.files[rel_path]
                is_root = rel_path == SKILL_FILE_NAME
                entries.append(
                    ResourceEntry(
                        uri=f"{SKILL_URI_PREFIX}{skill.name}/{rel_path}",
                        name=skill.name if is_root else f"{skill.name}/{rel_path}",
                        title=skill.title if is_root else None,
                        description=skill.description if is_root else "",
                        mimeType=skill_file.mime_type,
                        icons=skill.icons if is_root else (),
                        meta=skill_meta if is_root else None,
                    )
                )
        return entries

    def read_directory(self, uri: str) -> list[str]:
        """Return file relative paths belonging to the requested skill URI."""
        if not self.directory_read:
            raise SkillAccessError("Skill directory listing is disabled by MCPSkillsConfig.directory_read=False")
        if not uri.startswith(SKILL_URI_PREFIX):
            return []
        raw = uri.removeprefix(SKILL_URI_PREFIX).strip("/")
        if ".." in Path(raw).parts:
            raise SkillAccessError(f"Path traversal rejected in skill directory URI: {uri}")
        parts = raw.split("/", 1)
        skill_name = parts[0]
        skill = self._skills.get(skill_name)
        if skill is None:
            return []
        subpath = parts[1] if len(parts) > 1 else ""
        if not subpath:
            return sorted(skill.files.keys())
        prefix = subpath if subpath.endswith("/") else f"{subpath}/"
        return sorted(f for f in skill.files if f.startswith(prefix))

    async def read_file(self, uri: str) -> bytes:
        """Read and verify the SHA-256 digest of a skill file."""
        if not uri.startswith(SKILL_URI_PREFIX):
            raise KeyError(f"Invalid skill URI: {uri}")
        raw = uri.removeprefix(SKILL_URI_PREFIX)
        if ".." in Path(raw).parts:
            raise SkillAccessError(f"Path traversal rejected in skill file URI: {uri}")
        parts = raw.split("/", 1)
        skill_name = parts[0]
        subpath = parts[1] if len(parts) > 1 else SKILL_FILE_NAME

        skill = self._skills.get(skill_name)
        if skill is None:
            raise KeyError(f"Skill '{skill_name}' not found")

        skill_file = skill.files.get(subpath)
        if skill_file is None:
            if subpath == SKILL_FILE_NAME and skill.body:
                with self.telemetry.span_rpc("mcp", f"skill://{skill_name}/{subpath}"):
                    return skill.body.encode("utf-8")
            raise KeyError(f"File '{subpath}' not found in skill '{skill_name}'")

        if skill_file.content_bytes is not None:
            content = skill_file.content_bytes
        elif skill_file.real_path is not None:
            path_obj = Path(skill_file.real_path)
            content = await anyio.to_thread.run_sync(path_obj.read_bytes)
        else:
            raise FileNotFoundError(f"No storage found for file '{subpath}'")

        actual_digest = f"sha256:{hashlib.sha256(content).hexdigest()}"
        if actual_digest != skill_file.digest:
            raise SkillIntegrityError(f"Digest mismatch for '{uri}': expected {skill_file.digest}, got {actual_digest}")

        with self.telemetry.span_rpc("mcp", f"skill://{skill_name}/{subpath}"):
            return content

    def _load_from_sources(self) -> dict[str, Skill]:
        loaded: dict[str, Skill] = {}
        for src in self._sources:
            for skill in src.load_skills():
                loaded[skill.name] = skill
        return loaded

    @staticmethod
    def _listener_accepts_arg(listener: Callable[..., Any]) -> bool:
        try:
            sig = inspect.signature(listener)
        except (TypeError, ValueError):
            return False
        positional = [
            p
            for p in sig.parameters.values()
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        has_varargs = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
        return has_varargs or len(positional) >= 1

    def _notify(self) -> None:
        for listener in list(self._listeners):
            if self._listener_accepts_arg(listener):
                listener("skills")
            else:
                listener()


def _extract_skill_tags(frontmatter: Mapping[str, Any], explicit: Sequence[str] = ()) -> tuple[str, ...]:
    if explicit:
        return tuple(str(t) for t in explicit if str(t))
    raw = frontmatter.get("tags")
    if isinstance(raw, str):
        return (raw,) if raw else ()
    if isinstance(raw, Sequence) and not isinstance(raw, bytes):
        return tuple(str(t) for t in raw if str(t))
    return ()


def _extract_skill_icons(
    frontmatter: Mapping[str, Any],
    explicit: Sequence[MCPIcon | Mapping[str, Any]] = (),
) -> tuple[MCPIcon, ...]:
    if explicit:
        return coerce_mcp_icons(explicit)
    raw = frontmatter.get("icons")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return coerce_mcp_icons(raw)
    return ()


def _extract_skill_meta(frontmatter: Mapping[str, Any], explicit: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if explicit is not None:
        return dict(explicit)
    raw = frontmatter.get("meta")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _resolve_skill_base_path(raw_path: str | Path) -> Path:
    """Resolve relative skill directory paths against ``VERTEBRA_APP_ROOT`` when set."""
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    if app_root := os.environ.get("VERTEBRA_APP_ROOT", "").strip():
        return Path(app_root) / candidate
    return candidate


__all__ = [
    "FilesystemSkillSource",
    "MCPSkillsConfig",
    "SKILL_FILE_NAME",
    "SKILL_URI_PREFIX",
    "Skill",
    "SkillAccessError",
    "SkillCatalog",
    "SkillFile",
    "SkillIntegrityError",
    "SkillSource",
    "parse_skill_frontmatter",
]
