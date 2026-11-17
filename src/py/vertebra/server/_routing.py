"""Low-level route path parameter converters, segment trie routing, and FNV-1a handler ID hashing."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import msgspec

try:
    import vertebra._vertebra as _vtb

    _native_handler_id: Callable[[str, str], int] | None = getattr(_vtb, "handler_id", None)
except ImportError:
    _native_handler_id = None

__all__ = (
    "PARAM_CONVERTERS",
    "BaseRouteCompiler",
    "ExtractedRoute",
    "RouterTable",
    "convert_path_params",
    "make_handler_id",
    "normalize_path_for_matchit",
    "parse_path_param",
    "parse_timedelta",
)

_FNV_OFFSET = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3
_MASK_64 = 0xFFFFFFFFFFFFFFFF
_ISO_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)
_SEGMENT_PARAM_RE = re.compile(r"^\{(\*?)([a-zA-Z_][a-zA-Z0-9_]*)(?::([a-zA-Z0-9_]+))?\}$")
_GENERIC_PARAM_TYPES: frozenset[str] = frozenset({"str", "string", "slug"})


def parse_timedelta(raw: str) -> timedelta:
    """Parse an ISO-8601 duration or float seconds string into a timedelta."""
    match = _ISO_DURATION_RE.match(raw)
    if match and any(match.groupdict().values()):
        days = int(match.group("days") or 0)
        hours = int(match.group("hours") or 0)
        minutes = int(match.group("minutes") or 0)
        seconds = float(match.group("seconds") or 0.0)
        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return timedelta(seconds=float(raw))


def parse_path_param(raw: str) -> str:
    """Normalize a greedy path parameter by stripping any leading slash."""
    return raw.lstrip("/")


PARAM_CONVERTERS: dict[str, Callable[[str], Any]] = {
    "str": str,
    "string": str,
    "int": int,
    "float": float,
    "uuid": UUID,
    "decimal": Decimal,
    "date": date.fromisoformat,
    "datetime": datetime.fromisoformat,
    "time": time.fromisoformat,
    "timedelta": parse_timedelta,
    "path": parse_path_param,
    "slug": str,
}


def make_handler_id(method: str, path: str, name: str | None = None) -> int:
    """Derive a stable uint64 handler ID from method, path, and optional route name using FNV-1a."""
    if name is None and _native_handler_id is not None:
        return int(_native_handler_id(method, path))
    h = _FNV_OFFSET
    h = _fnv1a_write(h, method.upper().encode())
    h = _fnv1a_write(h, b"\xff")
    h = _fnv1a_write(h, b" ")
    h = _fnv1a_write(h, path.encode())
    h = _fnv1a_write(h, b"\xff")
    if name is not None:
        h = _fnv1a_write(h, name.encode())
        h = _fnv1a_write(h, b"\xfe")
    return h


def _fnv1a_write(h: int, data: bytes) -> int:
    """Update FNV-1a hash with each byte of ``data``."""
    for b in data:
        h ^= b
        h = (h * _FNV_PRIME) & _MASK_64
    return h


def normalize_path_for_matchit(path: str) -> str:
    """Convert a typed route path template into a matchit 0.9 route pattern."""
    if "{" not in path:
        return path
    segments: list[str] = []
    for segment in path.split("/"):
        matched = _SEGMENT_PARAM_RE.match(segment)
        if matched is None:
            segments.append(segment)
            continue
        is_catchall, param_name, param_type = matched.group(1), matched.group(2), matched.group(3) or "str"
        if is_catchall or param_type == "path":
            segments.append(f"{{*{param_name}}}")
        else:
            segments.append(f"{{{param_name}}}")
    return "/".join(segments)


def convert_path_params(
    raw_params: dict[str, str],
    param_types: dict[str, str],
) -> dict[str, Any]:
    """Convert raw string path parameters into typed Python objects using ``PARAM_CONVERTERS``."""
    converted: dict[str, Any] = {}
    for name, raw_val in raw_params.items():
        type_name = param_types.get(name, "str")
        converter = PARAM_CONVERTERS.get(type_name, str)
        converted[name] = converter(raw_val)
    return converted


def _extract_param_types_from_path(path: str) -> dict[str, str]:
    param_types: dict[str, str] = {}
    for segment in path.split("/"):
        matched = _SEGMENT_PARAM_RE.match(segment)
        if matched is not None:
            is_catchall, param_name, param_type = matched.group(1), matched.group(2), matched.group(3)
            if is_catchall:
                param_types[param_name] = "path"
            else:
                param_types[param_name] = param_type or "str"
    return param_types


def _compile_match_regex(path_template: str) -> re.Pattern[str]:
    parts: list[str] = []
    for segment in path_template.split("/"):
        matched = _SEGMENT_PARAM_RE.match(segment)
        if matched is None:
            parts.append(re.escape(segment))
            continue
        is_catchall, param_name, param_type = matched.group(1), matched.group(2), matched.group(3) or "str"
        if is_catchall or param_type == "path":
            parts.append(f"(?P<{param_name}>.+)")
        else:
            parts.append(f"(?P<{param_name}>[^/]+)")
    pattern = "/".join(parts) or "/"
    return re.compile(f"^{pattern}$")


class ExtractedRoute(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Framework-neutral compiled route descriptor indexed by stable uint64 handler_id."""

    handler_id: int
    method: str
    path_template: str
    matchit_path: str
    param_types: dict[str, str] = msgspec.field(default_factory=dict)
    regex_pattern: re.Pattern[str] | None = None
    name: str | None = None
    handler: Any = None


class _TrieNode:
    """Segment trie node resolving static segments before typed parameters and catch-alls."""

    __slots__ = ("param_children", "route", "static_children", "wildcard_child")

    def __init__(self) -> None:
        self.static_children: dict[str, _TrieNode] = {}
        self.param_children: list[tuple[str, str, _TrieNode]] = []
        self.wildcard_child: tuple[str, _TrieNode] | None = None
        self.route: Any = None


class RouterTable:
    """Lookup table mapping Rust handler_id and (method, path) to route entries via segment trie."""

    __slots__ = ("_by_method", "_fallback_routes", "_next_id", "_static_routes", "_trees", "routes")

    def __init__(self, routes: dict[int, Any] | Sequence[Any] | None = None) -> None:
        """Initialize the routing table from an existing handler-ID mapping or route sequence."""
        self.routes: dict[int, Any] = {}
        self._by_method: dict[str, list[Any]] = {}
        self._static_routes: dict[str, dict[str, Any]] = {}
        self._trees: dict[str, _TrieNode] = {}
        self._fallback_routes: dict[str, list[Any]] = {}
        self._next_id = 1

        if isinstance(routes, dict):
            for hid, entry in routes.items():
                resolved_id = int(hid)
                if resolved_id >= self._next_id:
                    self._next_id = resolved_id + 1
                self._index_entry(resolved_id, entry)
        elif routes is not None:
            for entry in routes:
                raw_id = getattr(entry, "handler_id", 0)
                resolved_id = int(raw_id) if isinstance(raw_id, int) and raw_id > 0 and raw_id not in self.routes else 0
                if resolved_id == 0:
                    resolved_id = self._allocate_id()
                    if isinstance(entry, ExtractedRoute):
                        entry = msgspec.structs.replace(entry, handler_id=resolved_id)
                elif resolved_id >= self._next_id:
                    self._next_id = resolved_id + 1
                self._index_entry(resolved_id, entry)

    def add_route(
        self,
        method: str,
        path_template: str,
        handler: Any = None,
        *,
        name: str | None = None,
        handler_id: int | None = None,
    ) -> ExtractedRoute:
        """Register a route into the table, assigning a distinct monotonic handler ID when omitted."""
        resolved_id = handler_id if handler_id is not None and handler_id not in self.routes else self._allocate_id()
        if resolved_id >= self._next_id:
            self._next_id = resolved_id + 1
        normalized_method = method.upper()
        normalized_path = path_template if path_template.startswith("/") else f"/{path_template}"
        entry = ExtractedRoute(
            handler_id=resolved_id,
            method=normalized_method,
            path_template=normalized_path,
            matchit_path=normalize_path_for_matchit(normalized_path),
            param_types=_extract_param_types_from_path(normalized_path),
            regex_pattern=_compile_match_regex(normalized_path),
            name=name,
            handler=handler,
        )
        self._index_entry(resolved_id, entry)
        return entry

    def get_by_handler_id(self, handler_id: int) -> Any | None:
        """Look up an extracted route by its stable uint64 handler_id."""
        return self.routes.get(handler_id)

    def match(self, method: str, path: str) -> Any | None:
        """Match a request method and path to a route entry, preferring static segments over parameters."""
        matched = self.match_with_params(method, path)
        return None if matched is None else matched[0]

    def match_with_params(self, method: str, path: str) -> tuple[Any, dict[str, Any]] | None:
        """Match a request method and path and return the route entry with converted path parameters."""
        method_upper = method.upper()
        static_entry = self._static_routes.get(method_upper, {}).get(path)
        if static_entry is not None:
            return static_entry, {}

        root = self._trees.get(method_upper)
        if root is not None:
            segments = _split_path_segments(path)
            raw_params: dict[str, Any] = {}
            matched_entry = _match_trie(root, segments, 0, raw_params)
            if matched_entry is not None:
                return matched_entry, raw_params

        for entry in self._fallback_routes.get(method_upper, ()):
            pattern = getattr(entry, "regex_pattern", None)
            if pattern is None:
                continue
            matched = pattern.match(path)
            if matched is not None:
                raw_groups = matched.groupdict()
                param_types = getattr(entry, "param_types", {})
                params = convert_path_params(raw_groups, param_types) if raw_groups else {}
                return entry, params
        return None

    def allowed_methods(self, path: str) -> list[str]:
        """Return a sorted list of HTTP methods matching ``path`` for 405 Allow generation."""
        allowed: set[str] = set()
        for method in self._by_method:
            if self.match(method, path) is not None:
                allowed.add(method)
        return sorted(allowed)

    def convert_params(
        self,
        route_entry: Any,
        raw_params: dict[str, str],
    ) -> dict[str, Any]:
        """Convert raw string path parameters using the route's pre-compiled converters."""
        param_types = getattr(route_entry, "param_types", {})
        return convert_path_params(raw_params, param_types)

    def _allocate_id(self) -> int:
        while self._next_id in self.routes:
            self._next_id += 1
        assigned = self._next_id
        self._next_id += 1
        return assigned

    def _index_entry(self, handler_id: int, entry: Any) -> None:
        self.routes[handler_id] = entry
        method_upper = str(getattr(entry, "method", "GET")).upper()
        self._by_method.setdefault(method_upper, []).append(entry)

        path_template = str(getattr(entry, "path_template", getattr(entry, "matchit_path", "/")))
        matchit_path = str(getattr(entry, "matchit_path", path_template))
        param_types: dict[str, str] = dict(getattr(entry, "param_types", {}))

        if "{" not in matchit_path and "{" not in path_template:
            self._static_routes.setdefault(method_upper, {})[path_template] = entry
            return

        if _insert_trie_route(self._trees.setdefault(method_upper, _TrieNode()), matchit_path, param_types, entry):
            return
        self._fallback_routes.setdefault(method_upper, []).append(entry)


class BaseRouteCompiler:
    """Framework-neutral base compiler providing path normalization, monotonic IDs, and parameter conversion."""

    __slots__ = ("_next_id",)

    def __init__(self) -> None:
        """Initialize the base route compiler with a monotonic handler ID counter."""
        self._next_id = 0

    def allocate_handler_id(self) -> int:
        """Return the next strictly increasing handler identifier."""
        self._next_id += 1
        return self._next_id

    def normalize_path(self, path: str) -> str:
        """Convert a typed route template into a matchit 0.9 pattern."""
        return normalize_path_for_matchit(path)

    def extract_param_types(self, path: str) -> dict[str, str]:
        """Extract parameter name to type-name mappings from a route template."""
        return _extract_param_types_from_path(path)

    def convert_params(self, raw_params: dict[str, str], param_types: dict[str, str]) -> dict[str, Any]:
        """Convert raw string path parameters using ``PARAM_CONVERTERS``."""
        return convert_path_params(raw_params, param_types)

    def compile_route(
        self,
        method: str,
        path_template: str,
        *,
        handler: Any = None,
        name: str | None = None,
        handler_id: int | None = None,
    ) -> ExtractedRoute:
        """Compile a single method and path template into an ``ExtractedRoute``."""
        normalized_method = method.upper()
        normalized_path = path_template if path_template.startswith("/") else f"/{path_template}"
        matchit_path = self.normalize_path(normalized_path)
        resolved_id = self.allocate_handler_id() if handler_id is None else handler_id
        return ExtractedRoute(
            handler_id=resolved_id,
            method=normalized_method,
            path_template=normalized_path,
            matchit_path=matchit_path,
            param_types=self.extract_param_types(normalized_path),
            regex_pattern=_compile_match_regex(normalized_path),
            name=name,
            handler=handler,
        )

    def build_table(self, routes: Iterable[ExtractedRoute]) -> RouterTable:
        """Build a ``RouterTable`` from compiled ``ExtractedRoute`` instances."""
        return RouterTable(list(routes))

    def compile(self, app: Any) -> RouterTable:
        """Compile routes exposed on ``app.routes`` into a ``RouterTable``."""
        compiled: list[ExtractedRoute] = []
        for route in getattr(app, "routes", ()):
            path_template = getattr(route, "path_template", getattr(route, "path", None))
            method = getattr(route, "method", None)
            if not isinstance(path_template, str) or not isinstance(method, str):
                continue
            compiled.append(
                self.compile_route(
                    method,
                    path_template,
                    handler=getattr(route, "handler", getattr(route, "endpoint", None)),
                    name=getattr(route, "name", None),
                )
            )
        return self.build_table(compiled)


def _split_path_segments(path: str) -> list[str]:
    if not path:
        return []
    if path.startswith("/"):
        return path.split("/")[1:]
    return path.split("/")


def _insert_trie_route(
    root: _TrieNode,
    matchit_path: str,
    param_types: dict[str, str],
    entry: Any,
) -> bool:
    node = root
    segments = _split_path_segments(matchit_path)
    for idx, segment in enumerate(segments):
        if segment.startswith("{*") and segment.endswith("}"):
            if idx != len(segments) - 1:
                return False
            param_name = segment[2:-1]
            if not param_name:
                return False
            if node.wildcard_child is not None and node.wildcard_child[0] == param_name:
                child = node.wildcard_child[1]
            else:
                child = _TrieNode()
                node.wildcard_child = (param_name, child)
            child.route = entry
            return True

        if segment.startswith("{") and segment.endswith("}"):
            inner = segment[1:-1]
            param_name, _, explicit_type = inner.partition(":")
            if not param_name:
                return False
            param_type = explicit_type or param_types.get(param_name, "str")
            child = _get_or_create_param_child(node, param_name, param_type)
            node = child
            continue

        if "{" in segment or "}" in segment:
            return False
        static_child = node.static_children.get(segment)
        if static_child is None:
            static_child = _TrieNode()
            node.static_children[segment] = static_child
        node = static_child

    node.route = entry
    return True


def _get_or_create_param_child(node: _TrieNode, param_name: str, param_type: str) -> _TrieNode:
    for existing_name, existing_type, existing_child in node.param_children:
        if existing_name == param_name and existing_type == param_type:
            return existing_child
    child = _TrieNode()
    node.param_children.append((param_name, param_type, child))
    node.param_children.sort(key=lambda item: 1 if item[1] in _GENERIC_PARAM_TYPES else 0)
    return child


def _match_trie(
    node: _TrieNode,
    segments: list[str],
    index: int,
    params: dict[str, Any],
) -> Any | None:
    if index == len(segments):
        return node.route

    segment = segments[index]
    static_child = node.static_children.get(segment)
    if static_child is not None:
        matched = _match_trie(static_child, segments, index + 1, params)
        if matched is not None:
            return matched

    if segment != "":
        for param_name, param_type, param_child in node.param_children:
            converter = PARAM_CONVERTERS.get(param_type, str)
            try:
                converted_val = converter(segment)
            except (ValueError, TypeError):
                continue
            had_prev = param_name in params
            prev_val = params.get(param_name)
            params[param_name] = converted_val
            matched = _match_trie(param_child, segments, index + 1, params)
            if matched is not None:
                return matched
            if had_prev:
                params[param_name] = prev_val
            else:
                params.pop(param_name, None)

    if node.wildcard_child is not None:
        param_name, wildcard_node = node.wildcard_child
        if wildcard_node.route is not None:
            params[param_name] = "/".join(segments[index:])
            return wildcard_node.route

    return None
