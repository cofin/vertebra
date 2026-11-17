"""ObjectStore-backed skill source for litestar-mcp skills extension."""

from __future__ import annotations

import hashlib
import inspect
from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar import Request
    from litestar_mcp import Skill, SkillFile


class ObjectStoreSkillSource:
    """Async skill source backed by Vertebra ObjectStore storage."""

    __slots__ = (
        "_max_bytes_per_skill",
        "_max_files_per_skill",
        "_prefix",
        "_skills",
        "_skills_by_name",
        "_store",
    )

    def __init__(
        self,
        store: Any,
        *,
        prefix: str = "skills",
        max_files_per_skill: int = 512,
        max_bytes_per_skill: int = 16_777_216,
    ) -> None:
        """Initialize the ObjectStore skill source with storage client and prefix."""
        ensure_module("litestar_mcp", extra="mcp")
        self._store = store
        self._prefix = prefix.strip("/")
        self._max_files_per_skill = max(1, max_files_per_skill)
        self._max_bytes_per_skill = max(1, max_bytes_per_skill)
        self._skills: tuple[Skill, ...] = ()
        self._skills_by_name: dict[str, Skill] = {}

    @property
    def prefix(self) -> str:
        """Return the configured object storage key prefix."""
        return self._prefix

    @property
    def skills(self) -> tuple[Skill, ...]:
        """Return loaded skills sorted by name."""
        return self._skills

    def register_skill(self, skill: Skill) -> None:
        """Register an indexed skill definition in the source catalog."""
        self._skills_by_name[skill.name] = skill
        self._skills = tuple(sorted(self._skills_by_name.values(), key=lambda s: s.name))

    async def list_skills(self, request: Request[Any, Any, Any], /) -> Sequence[Skill]:
        """Return all available skills from object storage."""
        _ = request
        return self._skills

    async def get_skill(self, request: Request[Any, Any, Any], name: str, /) -> Skill | None:
        """Return the skill matching name if indexed."""
        _ = request
        return self._skills_by_name.get(name)

    async def read_file(
        self,
        request: Request[Any, Any, Any],
        skill: Skill,
        file: SkillFile,
        /,
    ) -> bytes:
        """Read bytes for an indexed skill file from object storage."""
        _ = (request, skill)
        mcp_mod = ensure_module("litestar_mcp", extra="mcp")
        key = f"{self._prefix}/{file.uri.removeprefix('skill://')}"
        data: bytes | None = None
        if hasattr(self._store, "get"):
            raw = self._store.get(key)
            if inspect.isawaitable(raw):
                raw = await raw
            data = bytes(raw) if raw is not None else None

        if data is None:
            msg = f"Skill file not found in storage: {key}"
            raise LookupError(msg)

        if len(data) > self._max_bytes_per_skill:
            msg = f"Skill file {file.uri} exceeds maximum size {self._max_bytes_per_skill}"
            raise mcp_mod.SkillIntegrityError(msg)

        actual_digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
        if getattr(file, "digest", None) and file.digest != actual_digest:
            msg = f"Skill file digest mismatch for {file.uri}: expected {file.digest}, got {actual_digest}"
            raise mcp_mod.SkillIntegrityError(msg)

        return data
