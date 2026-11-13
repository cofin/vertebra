"""Progressive skill catalog and filesystem disclosure for Model Context Protocol.

This module provides SkillCatalog, FilesystemSkillSource, Skill, and SkillFile
to load and serve skill packages with SHA-256 integrity verification.
"""

import hashlib
import mimetypes
import os
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

import anyio
import anyio.to_thread
import msgspec

from vertebra.agent.telemetry import AgentTelemetryRecorder

SKILL_URI_PREFIX: str = "skill://"
SKILL_FILE_NAME: str = "SKILL.md"


class SkillIntegrityError(Exception):
    """Raised when a skill file fails SHA-256 digest verification."""


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


class Skill(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Immutable representation of an MCP progressive skill package."""

    name: str
    """Unique skill slug used in `skill://<name>` URIs."""
    description: str
    """One-line summary advertised in the compact skill catalog."""
    body: str
    """Full Markdown instructions loaded from `SKILL.md`."""
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
        files: Mapping[str, bytes | str] | None = None,
        frontmatter: Mapping[str, Any] | None = None,
    ) -> "Skill":
        """Construct an in-memory Skill, calculating SHA-256 digests for all files."""
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
            files=built_files,
            frontmatter=dict(frontmatter or {}),
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


def _resolve_skill_base_path(raw_path: str | Path) -> Path:
    """Resolve relative skill directory paths against ``VERTEBRA_APP_ROOT`` when set."""
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    if app_root := os.environ.get("VERTEBRA_APP_ROOT", "").strip():
        return Path(app_root) / candidate
    return candidate


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

                skills.append(
                    Skill(
                        name=name,
                        description=description,
                        body=body,
                        files=files,
                        frontmatter=frontmatter,
                    )
                )
        return skills


class MCPSkillsConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for MCP progressive skill discovery."""

    paths: tuple[str | Path, ...] = (".agents/skills",)
    """Filesystem directories scanned for `<skill>/SKILL.md` packages."""
    sources: tuple[Any, ...] = ()
    """Additional `SkillSource` providers supplying programmatic skills."""
    directory_read: bool = True
    """Allow clients to list relative file paths inside `skill://<name>/` directories."""
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
    ) -> None:
        self._skills: dict[str, Skill] = {s.name: s for s in skills}
        self.telemetry = telemetry if telemetry is not None else AgentTelemetryRecorder()

    @classmethod
    def from_config(
        cls,
        config: MCPSkillsConfig,
        telemetry: AgentTelemetryRecorder | None = None,
    ) -> "SkillCatalog":
        """Instantiate a SkillCatalog from an MCPSkillsConfig."""
        fs_source = FilesystemSkillSource(
            base_paths=config.paths,
            max_files_per_skill=config.max_files_per_skill,
            max_bytes_per_skill=config.max_bytes_per_skill,
        )
        loaded = list(fs_source.load_skills())
        for src in config.sources:
            if hasattr(src, "load_skills") and callable(src.load_skills):
                loaded.extend(src.load_skills())
        return cls(skills=loaded, telemetry=telemetry)

    def register_skill(self, skill: Skill) -> None:
        """Register a new skill into the catalog."""
        self._skills[skill.name] = skill

    def list_skills(self) -> list[Skill]:
        """Return all skills sorted by name."""
        return [self._skills[k] for k in sorted(self._skills.keys())]

    def get_skill(self, name: str) -> Skill | None:
        """Retrieve a skill by name."""
        return self._skills.get(name)

    def read_directory(self, uri: str) -> list[str]:
        """Return file relative paths belonging to the requested skill URI."""
        if not uri.startswith(SKILL_URI_PREFIX):
            return []
        raw = uri.removeprefix(SKILL_URI_PREFIX).strip("/")
        parts = raw.split("/", 1)
        skill_name = parts[0]
        skill = self.get_skill(skill_name)
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
        parts = raw.split("/", 1)
        skill_name = parts[0]
        subpath = parts[1] if len(parts) > 1 else SKILL_FILE_NAME

        skill = self.get_skill(skill_name)
        if skill is None:
            raise KeyError(f"Skill '{skill_name}' not found")

        skill_file = skill.files.get(subpath)
        if skill_file is None:
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


__all__ = [
    "FilesystemSkillSource",
    "MCPSkillsConfig",
    "SKILL_FILE_NAME",
    "SKILL_URI_PREFIX",
    "Skill",
    "SkillCatalog",
    "SkillFile",
    "SkillIntegrityError",
    "SkillSource",
    "parse_skill_frontmatter",
]
