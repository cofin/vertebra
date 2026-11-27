"""Lazy import-string mount specification parsing, resolution, and CLI composition."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.utils.config_tools import read_pyproject_section
from vertebra.utils.module_loader import import_app
from vertebra.utils.serializers import decode_json, encode_json

if TYPE_CHECKING:
    from vertebra.framework._app import App

__all__ = (
    "MOUNT_ENTRYPOINT_APP_PATH",
    "VALID_MOUNT_PROTOCOLS",
    "MountSpec",
    "build_mounted_app_from_env",
    "deserialize_mount_specs",
    "load_mounts_pyproject_config",
    "merge_mount_specs",
    "parse_mount_spec",
    "resolve_mount_target",
    "serialize_mount_specs",
    "validate_mount_target_spec",
)

MOUNT_ENTRYPOINT_APP_PATH: str = "vertebra.server._mounts:app"
VALID_MOUNT_PROTOCOLS: frozenset[str] = frozenset({"auto", "asgi", "wsgi", "rsgi"})


class MountSpec(msgspec.Struct, frozen=True, gc=False):
    """Immutable specification for a lazy sub-application mount."""

    prefix: str
    target: str
    protocol: str = "auto"


def _normalize_mount_prefix(prefix: str) -> str:
    """Normalize a mount prefix so root maps to empty string and sub-paths have no trailing slash."""
    cleaned = prefix.strip()
    if not cleaned or cleaned == "/":
        return ""
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned.rstrip("/")


def validate_mount_target_spec(target: str, *, prefix: str = "") -> str:
    """Validate a ``module:attr`` or ``module:factory()`` mount import specification.

    Args:
        target: Import string to validate.
        prefix: Optional mount prefix included in error messages.

    Returns:
        Trimmed target string.

    Raises:
        ValueError: If the target string is empty or not in ``module:attr`` format.
    """
    cleaned = target.strip()
    location = f" for prefix {prefix!r}" if prefix else ""
    if not cleaned or ":" not in cleaned:
        msg = f"Mount target{location} must be in 'module:attr' format, got: {target!r}"
        raise ValueError(msg)
    module_path, attr_spec = cleaned.rsplit(":", 1)
    attr_name = attr_spec[:-2] if attr_spec.endswith("()") else attr_spec
    if not module_path.strip() or not attr_name.strip():
        msg = f"Mount target{location} must specify non-empty module and attribute, got: {target!r}"
        raise ValueError(msg)
    return cleaned


def resolve_mount_target(target: Any, *, prefix: str = "") -> Any:
    """Resolve a mount target, importing ``module:attr`` strings lazily and passing live objects through.

    Supports both direct attributes (``pkg.mod:app``) and zero-argument factory
    callables denoted with trailing parentheses (``pkg.mod:create_app()``).

    Args:
        target: Live application object or ``module:attr`` import specification string.
        prefix: Optional mount prefix included in error messages.

    Returns:
        Resolved application object.

    Raises:
        ValueError: If a string target is malformed or a factory target is not callable.
        ImportError: If the target module cannot be imported.
        AttributeError: If the target attribute does not exist on the imported module.
    """
    if not isinstance(target, str):
        return target
    cleaned = validate_mount_target_spec(target, prefix=prefix)
    is_factory = cleaned.endswith("()")
    import_path = cleaned[:-2] if is_factory else cleaned
    resolved = import_app(import_path)
    if is_factory:
        if not callable(resolved):
            location = f" for prefix {prefix!r}" if prefix else ""
            msg = f"Mount factory target{location} is not callable: {target!r}"
            raise ValueError(msg)
        return resolved()
    return resolved


def parse_mount_spec(raw: str) -> MountSpec:
    """Parse a CLI ``PREFIX=MODULE:ATTR[@PROTOCOL]`` mount specification.

    Args:
        raw: Raw CLI mount string such as ``/legacy=pkg.wsgi:app@wsgi``.

    Returns:
        Parsed immutable ``MountSpec``.

    Raises:
        ValueError: If the syntax, target format, or protocol name is invalid.
    """
    text = raw.strip()
    if "=" not in text:
        msg = f"Mount specification must use 'PREFIX=MODULE:ATTR[@PROTOCOL]' format, got: {raw!r}"
        raise ValueError(msg)
    raw_prefix, rhs = text.split("=", 1)
    if not raw_prefix.strip():
        msg = f"Mount specification prefix cannot be empty, got: {raw!r}"
        raise ValueError(msg)
    prefix = _normalize_mount_prefix(raw_prefix)
    rhs = rhs.strip()
    protocol = "auto"
    if "@" in rhs:
        rhs, raw_protocol = rhs.rsplit("@", 1)
        protocol = raw_protocol.strip().lower()
        if protocol not in VALID_MOUNT_PROTOCOLS:
            allowed = ", ".join(sorted(VALID_MOUNT_PROTOCOLS))
            msg = f"Invalid mount protocol {raw_protocol!r} for prefix {prefix or '/'!r}; expected one of: {allowed}"
            raise ValueError(msg)
    target = validate_mount_target_spec(rhs, prefix=prefix or "/")
    return MountSpec(prefix=prefix, target=target, protocol=protocol)


def serialize_mount_specs(specs: Sequence[MountSpec]) -> str:
    """Serialize mount specifications to a JSON string for environment transport."""
    return encode_json(list(specs))


def deserialize_mount_specs(raw: str) -> tuple[MountSpec, ...]:
    """Deserialize mount specifications from a JSON string."""
    if not raw or not raw.strip():
        return ()
    decoded = decode_json(raw)
    if not isinstance(decoded, list):
        msg = f"VERTEBRA_MOUNTS must decode to a JSON list, got: {type(decoded).__name__}"
        raise ValueError(msg)
    specs: list[MountSpec] = []
    for item in decoded:
        if not isinstance(item, Mapping):
            msg = f"Each VERTEBRA_MOUNTS entry must be an object, got: {type(item).__name__}"
            raise ValueError(msg)
        prefix = _normalize_mount_prefix(str(item.get("prefix", "")))
        target = validate_mount_target_spec(str(item.get("target", "")), prefix=prefix or "/")
        protocol = str(item.get("protocol", "auto")).strip().lower() or "auto"
        if protocol not in VALID_MOUNT_PROTOCOLS:
            allowed = ", ".join(sorted(VALID_MOUNT_PROTOCOLS))
            msg = f"Invalid mount protocol {protocol!r} for prefix {prefix or '/'!r}; expected one of: {allowed}"
            raise ValueError(msg)
        specs.append(MountSpec(prefix=prefix, target=target, protocol=protocol))
    return tuple(specs)


def merge_mount_specs(
    base_specs: Sequence[MountSpec],
    override_specs: Sequence[MountSpec],
) -> tuple[MountSpec, ...]:
    """Merge mount specs preserving order while allowing override_specs to replace matching prefixes."""
    merged: dict[str, MountSpec] = {}
    for spec in base_specs:
        merged[spec.prefix] = spec
    for spec in override_specs:
        merged[spec.prefix] = spec
    return tuple(merged.values())


def load_mounts_pyproject_config(project_dir: Path | None = None) -> tuple[MountSpec, ...]:
    """Read ``[tool.vertebra.mounts]`` entries from ``pyproject.toml`` when present.

    Supports both shorthand string entries (``"/api" = "pkg.asgi:app@asgi"``) and
    table entries (``"/legacy" = { target = "pkg.wsgi:app", protocol = "wsgi" }``).

    Args:
        project_dir: Directory containing ``pyproject.toml``. Defaults to ``Path.cwd()``.

    Returns:
        Tuple of parsed ``MountSpec`` entries in declaration order.
    """
    root = project_dir if project_dir is not None else Path.cwd()
    pyproject_path = root / "pyproject.toml"
    if not pyproject_path.is_file():
        return ()
    mounts_cfg = read_pyproject_section("mounts", start_path=pyproject_path)
    if not mounts_cfg:
        return ()
    specs: list[MountSpec] = []
    for raw_prefix, entry in mounts_cfg.items():
        prefix_str = str(raw_prefix).strip()
        if isinstance(entry, str):
            specs.append(parse_mount_spec(f"{prefix_str}={entry}"))
        elif isinstance(entry, Mapping):
            prefix = _normalize_mount_prefix(prefix_str)
            raw_target = entry.get("target")
            if not isinstance(raw_target, str):
                msg = f"[tool.vertebra.mounts] entry for {prefix_str!r} must include a string 'target'"
                raise ValueError(msg)
            protocol = str(entry.get("protocol", "auto")).strip().lower() or "auto"
            if protocol not in VALID_MOUNT_PROTOCOLS:
                allowed = ", ".join(sorted(VALID_MOUNT_PROTOCOLS))
                msg = f"Invalid mount protocol {protocol!r} for prefix {prefix_str!r}; expected one of: {allowed}"
                raise ValueError(msg)
            target = validate_mount_target_spec(raw_target, prefix=prefix or "/")
            specs.append(MountSpec(prefix=prefix, target=target, protocol=protocol))
        else:
            msg = f"Unsupported [tool.vertebra.mounts] value for {prefix_str!r}: {type(entry).__name__}"
            raise ValueError(msg)
    return tuple(specs)


def build_mounted_app_from_env(environ: Mapping[str, str] | None = None) -> App:
    """Construct a composite ``App`` from ``VERTEBRA_MOUNTS`` and optional ``VERTEBRA_BASE_APP``.

    Args:
        environ: Optional environment mapping. Defaults to ``os.environ``.

    Returns:
        Configured ``App`` with lazy string mounts attached.
    """
    from vertebra.framework._app import App

    env = os.environ if environ is None else environ
    raw_mounts = env.get("VERTEBRA_MOUNTS", "").strip()
    mount_specs = deserialize_mount_specs(raw_mounts) if raw_mounts else ()
    base_app_path = env.get("VERTEBRA_BASE_APP", "").strip()

    root_app: App
    if base_app_path:
        loaded_base = import_app(base_app_path)
        if isinstance(loaded_base, App):
            root_app = loaded_base
            for spec in mount_specs:
                root_app.mount(spec.prefix or "/", spec.target, protocol=spec.protocol, lazy=True)
            return root_app
        root_app = App()
        for spec in mount_specs:
            root_app.mount(spec.prefix or "/", spec.target, protocol=spec.protocol, lazy=True)
        has_root = any(spec.prefix == "" for spec in mount_specs)
        if not has_root:
            base_proto = env.get("VERTEBRA_INTERFACE", "auto").strip().lower() or "auto"
            root_app.mount("/", loaded_base, protocol=base_proto)
        return root_app

    root_app = App()
    for spec in mount_specs:
        root_app.mount(spec.prefix or "/", spec.target, protocol=spec.protocol, lazy=True)
    return root_app


def __getattr__(name: str) -> Any:
    """Resolve ``vertebra.server._mounts:app`` lazily from environment mount specs."""
    if name == "app":
        return build_mounted_app_from_env()
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
