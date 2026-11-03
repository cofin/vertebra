"""Configuration, pyproject.toml discovery, environment variable coercion, and DSN utilities."""

from __future__ import annotations

import inspect
import os
import threading
import tomllib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar, Final, get_args, get_origin, overload
from urllib.parse import parse_qs, unquote, urlparse

from vertebra.utils.serializers import decode_json

__all__ = (
    "FALSE_VALUES",
    "TRUE_VALUES",
    "SingletonMeta",
    "find_pyproject_toml",
    "get_config_val",
    "get_config_val_with_aliases",
    "get_env",
    "get_env_with_aliases",
    "is_env_set",
    "normalize_connection_config",
    "parse_mysql_dsn",
    "parse_odbc_connection_string",
    "parse_pyproject_config",
    "read_pyproject_section",
    "resolve_config_async",
    "resolve_config_sync",
)

TRUE_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on", "t", "y"})
FALSE_VALUES: Final[frozenset[str]] = frozenset({"0", "false", "no", "off", "f", "n"})


class SingletonMeta(type):
    """Thread-safe metaclass that caches and returns a single instance per subclass."""

    _instances: ClassVar[dict[type[Any], Any]] = {}
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """Return the cached singleton instance or construct it on first call."""
        if cls in cls._instances:
            return cls._instances[cls]
        with cls._lock:
            if cls not in cls._instances:
                cls._instances[cls] = super().__call__(*args, **kwargs)
            return cls._instances[cls]


@overload
def get_env(key: str) -> str | None: ...


@overload
def get_env[T](key: str, default: T) -> str | T: ...


@overload
def get_env[T](key: str, default: T, type_hint: type[T]) -> T: ...


@overload
def get_env(key: str, default: Any, type_hint: Any) -> Any: ...


def get_env(key: str, default: Any = None, type_hint: Any = None) -> Any:
    """Read an environment variable with optional type coercion (`bool`, `int`, `float`, `Path`, `list`, `dict`)."""
    value = os.getenv(key)
    if value is None:
        return default
    if type_hint is None:
        return value
    return _coerce_env_value(key, value, type_hint)


def get_env_with_aliases(
    primary_key: str,
    aliases: Sequence[str] = (),
    *,
    default: Any = None,
    type_hint: Any = None,
) -> Any:
    """Read an environment variable checking ``primary_key`` first, then ``aliases`` in order."""
    for candidate in (primary_key, *aliases):
        raw = os.getenv(candidate)
        if raw is not None:
            if type_hint is None:
                return raw
            return _coerce_env_value(candidate, raw, type_hint)
    return default


def is_env_set(key: str, *, default: bool = False) -> bool:
    """Return True if environment variable ``key`` is set and not a falsy boolean token."""
    raw = os.getenv(key)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    return bool(normalized) and normalized not in FALSE_VALUES


def find_pyproject_toml(start_path: Path | str | None = None) -> Path | None:
    """Walk filesystem upward from ``start_path`` (or ``Path.cwd()``) to find ``pyproject.toml``."""
    current = Path(start_path).resolve() if start_path is not None else Path.cwd().resolve()
    if current.is_file():
        current = current.parent

    while True:
        pyproject = current / "pyproject.toml"
        if pyproject.is_file():
            return pyproject

        if (current / ".git").exists():
            return None

        if current == current.parent:
            return None

        current = current.parent


def read_pyproject_section(
    *section_path: str | None,
    start_path: Path | str | None = None,
) -> dict[str, Any]:
    """Read ``[tool.vertebra]`` (with ``[tool.vtb]`` fallback) or a nested subsection from ``pyproject.toml``."""
    if start_path is not None:
        candidate = Path(start_path)
        if candidate.is_file() and candidate.name == "pyproject.toml":
            pyproject_path: Path | None = candidate
        elif (candidate / "pyproject.toml").is_file():
            pyproject_path = candidate / "pyproject.toml"
        else:
            pyproject_path = find_pyproject_toml(candidate)
    else:
        pyproject_path = find_pyproject_toml()

    if pyproject_path is None or not pyproject_path.is_file():
        return {}

    try:
        with pyproject_path.open("rb") as fh:
            data = tomllib.load(fh)
    except Exception:
        return {}

    tool_section = data.get("tool")
    if not isinstance(tool_section, dict):
        return {}

    base_section = tool_section.get("vertebra")
    if not isinstance(base_section, dict):
        base_section = tool_section.get("vtb")
    if not isinstance(base_section, dict):
        return {}

    raw_segments = [seg.strip() for seg in section_path if seg and seg.strip()]
    if not raw_segments:
        return dict(base_section)

    normalized = ".".join(raw_segments)
    for prefix in ("tool.vertebra.", "tool.vtb.", "vertebra.", "vtb."):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
            break
    if normalized in {"tool.vertebra", "tool.vtb", "vertebra", "vtb", ""}:
        return dict(base_section)

    cursor: Any = base_section
    for part in normalized.split("."):
        if not part:
            continue
        if not isinstance(cursor, dict) or part not in cursor:
            return {}
        cursor = cursor[part]
    return dict(cursor) if isinstance(cursor, dict) else {}


def parse_pyproject_config(pyproject_path: Path) -> str | None:
    """Parse ``pyproject.toml`` for ``[tool.vertebra].config`` (or ``[tool.vtb].config`` fallback)."""
    try:
        with pyproject_path.open("rb") as f:
            data = tomllib.load(f)
    except Exception as e:
        msg = f"Failed to parse {pyproject_path}: {e}"
        raise ValueError(msg) from e

    tool_section = data.get("tool", {})
    if not isinstance(tool_section, dict):
        return None

    vtb_section = tool_section.get("vertebra")
    section_label = "[tool.vertebra]"
    if not isinstance(vtb_section, dict):
        vtb_section = tool_section.get("vtb", {})
        section_label = "[tool.vtb]"
    if not isinstance(vtb_section, dict):
        return None

    config = vtb_section.get("config")
    if config is None:
        return None

    if isinstance(config, str):
        return config

    if isinstance(config, list):
        if not all(isinstance(item, str) for item in config):
            msg = f"Invalid {section_label}.config in {pyproject_path}: list items must be strings"
            raise ValueError(msg)
        return ",".join(config)

    msg = f"Invalid {section_label}.config in {pyproject_path}: must be string or list of strings, got {type(config).__name__}"
    raise ValueError(msg)


def get_config_val(
    key: str,
    *,
    value: Any = None,
    default: Any = None,
    env_key: str | None = None,
    type_hint: Any = None,
    pyproject_section: str | None = None,
    start_path: Path | str | None = None,
) -> Any:
    """Resolve a configuration setting with precedence: explicit ``value`` -> ``env_key`` -> ``pyproject.toml`` -> ``default``."""
    return get_config_val_with_aliases(
        key,
        value=value,
        default=default,
        env_keys=(env_key,) if env_key else (),
        type_hint=type_hint,
        pyproject_section=pyproject_section,
        start_path=start_path,
    )


def get_config_val_with_aliases(
    key: str,
    *,
    value: Any = None,
    default: Any = None,
    env_keys: Sequence[str] = (),
    pyproject_aliases: Sequence[str] = (),
    type_hint: Any = None,
    pyproject_section: str | None = None,
    start_path: Path | str | None = None,
) -> Any:
    """Resolve a configuration value across explicit override, environment aliases, and ``pyproject.toml``."""
    if value is not None:
        return value

    for env_name in env_keys:
        if env_name and os.getenv(env_name) is not None:
            return get_env(env_name, default, type_hint)

    section_data = read_pyproject_section(pyproject_section, start_path=start_path)
    if section_data:
        for candidate_key in (key, key.replace("_", "-"), key.replace("-", "_"), *pyproject_aliases):
            if candidate_key in section_data and section_data[candidate_key] is not None:
                raw_val = section_data[candidate_key]
                if type_hint is not None and isinstance(raw_val, str):
                    return _coerce_env_value(candidate_key, raw_val, type_hint)
                return raw_val

    return default


def normalize_connection_config(
    config: str | Mapping[str, Any] | None = None,
    *,
    dsn_key: str = "dsn",
    **kwargs: Any,
) -> dict[str, Any]:
    """Normalize a database or service connection configuration from a DSN string, mapping, or keyword arguments."""
    normalized: dict[str, Any] = {}
    if isinstance(config, str):
        normalized[dsn_key] = config
    elif isinstance(config, Mapping):
        normalized.update(config)
    elif config is not None:
        msg = f"Connection config must be a DSN string, Mapping, or None; got {type(config)!r}"
        raise TypeError(msg)
    for key, val in kwargs.items():
        if val is not None:
            normalized[key] = val
    return normalized


def parse_mysql_dsn(dsn: str) -> dict[str, Any]:
    """Parse a MySQL or MariaDB connection URL (`mysql://user:pass@host:3306/db?opt=val`) into connection kwargs."""
    parsed = urlparse(dsn)
    scheme = parsed.scheme.split("+", 1)[0].lower()
    if scheme not in {"mysql", "mariadb"}:
        msg = f"Unsupported MySQL DSN scheme: {parsed.scheme!r}"
        raise ValueError(msg)

    result: dict[str, Any] = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 3306,
    }
    if parsed.username is not None:
        result["user"] = unquote(parsed.username)
    if parsed.password is not None:
        result["password"] = unquote(parsed.password)
    database = parsed.path.lstrip("/")
    if database:
        result["database"] = unquote(database)
    for k, values in parse_qs(parsed.query, keep_blank_values=True).items():
        if values:
            result[k] = values[-1]
    return result


def parse_odbc_connection_string(conn_str: str) -> dict[str, str]:
    """Parse a semicolon-delimited ODBC connection string (`Driver={...};Server=...;`) into a key-value dict."""
    parsed: dict[str, str] = {}
    for segment in conn_str.split(";"):
        piece = segment.strip()
        if not piece or "=" not in piece:
            continue
        key, val = piece.split("=", 1)
        clean_key = key.strip()
        if clean_key:
            parsed[clean_key] = val.strip()
    return parsed


def resolve_config_sync(config: Any) -> Any:
    """Resolve a configuration object or synchronous zero-argument factory callable."""
    resolved = config() if callable(config) else config
    if inspect.isawaitable(resolved):
        msg = "resolve_config_sync received an awaitable; use resolve_config_async instead"
        raise TypeError(msg)
    return resolved


async def resolve_config_async(config: Any) -> Any:
    """Resolve a configuration object, synchronous factory, or asynchronous coroutine factory."""
    resolved = config() if callable(config) else config
    if inspect.isawaitable(resolved):
        return await resolved
    return resolved


def _coerce_bool(value: str) -> bool:
    """Coerce a string environment variable into a boolean."""
    normalized = value.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    msg = f"Invalid boolean environment variable: {value}"
    raise ValueError(msg)


def _coerce_env_value(key: str, value: str, type_hint: Any) -> Any:
    """Coerce ``value`` into ``type_hint`` supporting scalars, ``Path``, ``list[T]``, and ``dict[K, V]``."""
    if type_hint is bool:
        return _coerce_bool(value)

    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if type_hint is list or origin is list or type_hint is tuple or origin is tuple:
        elem_type: Any = args[0] if args else str
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                decoded = decode_json(stripped)
                if isinstance(decoded, list):
                    items = [_coerce_scalar_elem(key, item, elem_type) for item in decoded]
                    return tuple(items) if (type_hint is tuple or origin is tuple) else items
            except Exception as exc:
                msg = f"Failed to parse JSON list environment variable {key}={value}"
                raise ValueError(msg) from exc
        raw_parts = [part.strip() for part in stripped.split(",") if part.strip()] if stripped else []
        items = [_coerce_scalar_elem(key, part, elem_type) for part in raw_parts]
        return tuple(items) if (type_hint is tuple or origin is tuple) else items

    if type_hint is dict or origin is dict:
        val_type: Any = args[1] if len(args) >= 2 else str
        stripped = value.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                decoded = decode_json(stripped)
                if isinstance(decoded, dict):
                    return {
                        str(k): (v if val_type is Any else _coerce_scalar_elem(key, v, val_type))
                        for k, v in decoded.items()
                    }
            except Exception as exc:
                msg = f"Failed to parse JSON dict environment variable {key}={value}"
                raise ValueError(msg) from exc
        parsed_map: dict[str, Any] = {}
        for entry in stripped.split(","):
            piece = entry.strip()
            if not piece:
                continue
            if "=" not in piece:
                msg = f"Invalid key=value entry in environment variable {key}={value}"
                raise ValueError(msg)
            k, v = piece.split("=", 1)
            parsed_map[k.strip()] = v.strip() if val_type is Any else _coerce_scalar_elem(key, v.strip(), val_type)
        return parsed_map

    if type_hint is Path:
        return Path(value)

    if callable(type_hint):
        try:
            return type_hint(value)
        except (ValueError, TypeError) as e:
            type_name = getattr(type_hint, "__name__", str(type_hint))
            msg = f"Failed to coerce environment variable {key}={value} to {type_name}"
            raise ValueError(msg) from e

    return value


def _coerce_scalar_elem(key: str, raw: Any, elem_type: Any) -> Any:
    """Coerce a single list/dict element to ``elem_type``."""
    if elem_type is Any or elem_type is str:
        return str(raw)
    if elem_type is bool:
        return raw if isinstance(raw, bool) else _coerce_bool(str(raw))
    if isinstance(elem_type, type) and isinstance(raw, elem_type):
        return raw
    try:
        return elem_type(raw)
    except (ValueError, TypeError) as e:
        type_name = getattr(elem_type, "__name__", str(elem_type))
        msg = f"Failed to coerce element {raw!r} in environment variable {key} to {type_name}"
        raise ValueError(msg) from e
