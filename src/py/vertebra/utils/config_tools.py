"""Configuration and environment utilities for Vertebra.

Provides helpers for:
- Environment variable parsing with type coercion
- pyproject.toml discovery and section reading
- connection config normalization
"""

import os
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, TypeVar, overload

from vertebra.cli.errors import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping

T = TypeVar("T")

__all__ = (
    "FALSE_VALUES",
    "TRUE_VALUES",
    "discover_config_from_pyproject",
    "find_pyproject_toml",
    "get_env",
    "normalize_connection_config",
    "parse_pyproject_config",
    "read_pyproject_section",
)

TRUE_VALUES: Final = {"1", "true", "yes", "on"}
FALSE_VALUES: Final = {"0", "false", "no", "off"}


# =============================================================================
# Environment Variable Parsing
# =============================================================================


def _coerce_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    msg = f"Invalid boolean environment variable: {value}"
    raise ValueError(msg)


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


# =============================================================================
# pyproject.toml Discovery
# =============================================================================


def find_pyproject_toml() -> Path | None:
    """Walk filesystem upward to find pyproject.toml."""
    current = Path.cwd()

    while True:
        pyproject = current / "pyproject.toml"
        if pyproject.exists():
            return pyproject

        # Stop at .git boundary
        if (current / ".git").exists():
            return None

        # Stop at filesystem root
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


def discover_config_from_pyproject() -> str | None:
    """Find and parse pyproject.toml for vtb config."""
    pyproject_path = find_pyproject_toml()
    if pyproject_path is None:
        return None

    return parse_pyproject_config(pyproject_path)


def read_pyproject_section(section: str) -> dict[str, Any]:
    """Read a specific section from [tool.vtb.<section>] in pyproject.toml."""
    path = find_pyproject_toml()
    if path is None:
        return {}

    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        return {}

    tool_vtb = data.get("tool", {}).get("vtb", {})
    if not isinstance(tool_vtb, dict):
        return {}

    return dict(tool_vtb.get(section, {}))


# =============================================================================
# Connection Config Normalization
# =============================================================================


def normalize_connection_config(
    connection_config: "Mapping[str, Any] | None", *, extra_key: str = "extra"
) -> dict[str, Any]:
    """Normalize an adapter connection_config dictionary."""
    normalized: dict[str, Any] = dict(connection_config) if connection_config else {}
    extras = normalized.pop(extra_key, {})
    if extras is None:
        return normalized
    if not isinstance(extras, dict):
        msg = f"The '{extra_key}' field in connection_config must be a dictionary."
        raise ImproperConfigurationError(msg)
    normalized.update(extras)
    return normalized
