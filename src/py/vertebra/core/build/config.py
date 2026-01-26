"""Build configuration."""

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class TargetConfig:
    profile: str | None = None


@dataclass
class BuildConfig:
    app: str | None = None
    profile: str = "modern"
    targets: dict[str, TargetConfig] = field(default_factory=dict)

    @classmethod
    def load(cls, project_dir: Path) -> "BuildConfig":
        pyproject = project_dir / "pyproject.toml"
        if not pyproject.exists():
            return cls()

        with pyproject.open("rb") as f:
            data = tomllib.load(f)

        tool = data.get("tool", {}).get("vertebra", {})

        targets = {}
        for name, cfg in tool.get("build", {}).get("targets", {}).items():
            targets[name] = TargetConfig(profile=cfg.get("profile"))

        return cls(
            app=tool.get("app"),
            profile=tool.get("profile", "modern"),
            targets=targets,
        )
