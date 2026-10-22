"""Configuration and environment utilities for Vertebra.

Provides helpers for:
- Environment variable parsing with type coercion
- pyproject.toml discovery and section reading
- connection config normalization
"""

import os
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, overload

if TYPE_CHECKING:
    pass

__all__ = (
    "FALSE_VALUES",
    "TRUE_VALUES",
    "find_pyproject_toml",
    "get_env",
    "parse_pyproject_config",
)

TRUE_VALUES: Final = {"1", "true", "yes", "on"}
FALSE_VALUES: Final = {"0", "false", "no", "off"}


@overload
def get_env(key: str) -> str | None: ...


@overload
def get_env[T](key: str, default: T) -> str | T: ...


@overload
def get_env[T](key: str, default: T, type_hint: type[T]) -> T: ...


def get_env(key: str, default: Any = None, type_hint: type | None = None) -> Any:
    """Get an environment variable with optional type coercion."""
    value = os.getenv(key)
    if value is None:
        return default

    if type_hint is bool:
        return _coerce_bool(value)

    if type_hint is not None:
        try:
            return type_hint(value)
        except (ValueError, TypeError) as e:
            msg = f"Failed to coerce environment variable {key}={value} to {type_hint.__name__}"
            raise ValueError(msg) from e

    return value


def find_pyproject_toml() -> Path | None:
    """Walk filesystem upward to find pyproject.toml, stopping at .git or root."""
    current = Path.cwd()

    while True:
        pyproject = current / "pyproject.toml"
        if pyproject.exists():
            return pyproject

        if (current / ".git").exists():
            return None

        if current == current.parent:
            return None

        current = current.parent


def parse_pyproject_config(pyproject_path: Path) -> str | None:
    """Parse pyproject.toml for [tool.vtb] config."""
    try:
        with pyproject_path.open("rb") as f:
            data = tomllib.load(f)
    except Exception as e:
        msg = f"Failed to parse {pyproject_path}: {e}"
        raise ValueError(msg) from e

    tool_section = data.get("tool", {})
    if not isinstance(tool_section, dict):
        return None

    vtb_section = tool_section.get("vtb", {})
    if not isinstance(vtb_section, dict):
        return None

    config = vtb_section.get("config")
    if config is None:
        return None

    if isinstance(config, str):
        return config

    if isinstance(config, list):
        if not all(isinstance(item, str) for item in config):
            msg = f"Invalid [tool.vtb].config in {pyproject_path}: list items must be strings"
            raise ValueError(msg)
        return ",".join(config)

    msg = (
        f"Invalid [tool.vtb].config in {pyproject_path}: must be string or list of strings, got {type(config).__name__}"
    )
    raise ValueError(msg)


def _coerce_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    msg = f"Invalid boolean environment variable: {value}"
    raise ValueError(msg)
