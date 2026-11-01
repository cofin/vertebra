"""Flask / Werkzeug URLMap to Vertebra CompiledRouter compiler."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.server._routing import make_handler_id

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "CompiledRouter",
    "RouteCompiler",
    "RouteEntry",
    "convert_werkzeug_rule",
)

_WERKZEUG_PARAM_RE = re.compile(r"<(?:(?P<converter>[a-zA-Z_][a-zA-Z0-9_]*):)?(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)>")


def convert_werkzeug_rule(rule_str: str) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Convert a Werkzeug URL rule ``/users/<int:user_id>/<path:rest>`` into matchit syntax.

    Args:
        rule_str: Werkzeug route pattern string.

    Returns:
        Tuple of ``(matchit_pattern, ((param_name, converter_name), ...))``.
    """
    converters: list[tuple[str, str]] = []

    def _replace(match: re.Match[str]) -> str:
        conv = (match.group("converter") or "string").lower()
        name = match.group("name")
        converters.append((name, conv))
        if conv == "path":
            return f"{{*{name}}}"
        return f"{{{name}}}"

    converted = _WERKZEUG_PARAM_RE.sub(_replace, rule_str)
    if not converted.startswith("/"):
        converted = "/" + converted
    return converted, tuple(converters)


def _compile_werkzeug_regex(rule_str: str) -> re.Pattern[str]:
    """Compile a Werkzeug URL rule into a named-group regular expression."""
    pos = 0
    parts: list[str] = []
    for match in _WERKZEUG_PARAM_RE.finditer(rule_str):
        parts.append(re.escape(rule_str[pos : match.start()]))
        conv = (match.group("converter") or "string").lower()
        name = match.group("name")
        if conv == "path":
            parts.append(f"(?P<{name}>.+)")
        else:
            parts.append(f"(?P<{name}>[^/]+)")
        pos = match.end()
    parts.append(re.escape(rule_str[pos:]))
    pattern = "".join(parts) or "/"
    if not pattern.startswith("/"):
        pattern = "/" + pattern
    return re.compile(f"^{pattern}$")


class RouteEntry(msgspec.Struct, frozen=True, gc=False):
    """Metadata for a compiled Flask route rule."""

    rule: str
    matchit_path: str
    endpoint: str
    methods: tuple[str, ...]
    handler_id: int
    view_func: Callable[..., Any]
    regex_pattern: re.Pattern[str]
    param_converters: tuple[tuple[str, str], ...] = ()
    is_fastpath_eligible: bool = False


class CompiledRouter:
    """Compiled route matcher mapping ``(method, path)`` to ``(handler_id, params)``."""

    __slots__ = ("_by_method",)

    def __init__(self, entries: dict[int, RouteEntry]) -> None:
        by_method: dict[str, list[RouteEntry]] = {}
        for entry in entries.values():
            for method in entry.methods:
                by_method.setdefault(method.upper(), []).append(entry)
        self._by_method = by_method

    def match_route(self, method: str, path: str) -> tuple[int, dict[str, str]] | None:
        """Match ``(method, path)`` and return ``(handler_id, raw_params)``."""
        for entry in self._by_method.get(method.upper(), ()):
            matched = entry.regex_pattern.match(path)
            if matched is not None:
                return entry.handler_id, matched.groupdict()
        return None


class RouteCompiler:
    """Compiles a Flask application's ``url_map`` into a ``CompiledRouter``."""

    __slots__ = ("_entries", "_router")

    def __init__(self) -> None:
        self._entries: dict[int, RouteEntry] = {}
        self._router: CompiledRouter | None = None

    @property
    def entries(self) -> dict[int, RouteEntry]:
        """Return mapping from ``handler_id`` to ``RouteEntry``."""
        return self._entries

    @property
    def router(self) -> CompiledRouter:
        """Return the compiled router, compiling lazily if needed."""
        if self._router is None:
            self._router = CompiledRouter(self._entries)
        return self._router

    def compile_app(self, app: Any) -> CompiledRouter:
        """Inspect ``app.url_map`` and ``app.view_functions`` and build a ``CompiledRouter``."""
        url_map = getattr(app, "url_map", None)
        view_functions: dict[str, Callable[..., Any]] = getattr(app, "view_functions", {}) or {}
        if url_map is not None and hasattr(url_map, "iter_rules"):
            for rule_obj in url_map.iter_rules():
                endpoint = str(getattr(rule_obj, "endpoint", ""))
                if endpoint == "static":
                    continue
                view_func = view_functions.get(endpoint)
                if view_func is None:
                    continue
                rule_str = str(getattr(rule_obj, "rule", "/"))
                raw_methods = getattr(rule_obj, "methods", None) or ("GET",)
                methods = tuple(
                    sorted(str(m).upper() for m in raw_methods if str(m).upper() not in {"HEAD", "OPTIONS"})
                )
                if not methods:
                    methods = ("GET",)
                self.register_rule(rule_str, endpoint, methods, view_func)
        self._router = CompiledRouter(self._entries)
        return self._router

    def register_rule(
        self,
        rule_str: str,
        endpoint: str,
        methods: tuple[str, ...],
        view_func: Callable[..., Any],
    ) -> None:
        """Register a single Werkzeug rule and view function into the route table."""
        matchit_path, converters = convert_werkzeug_rule(rule_str)
        regex_pattern = _compile_werkzeug_regex(rule_str)
        is_fast = bool(getattr(view_func, "__vertebra_fastpath__", False))
        for method in methods:
            handler_id = make_handler_id(method, matchit_path)
            self._entries[handler_id] = RouteEntry(
                rule=rule_str,
                matchit_path=matchit_path,
                endpoint=endpoint,
                methods=(method,),
                handler_id=handler_id,
                view_func=view_func,
                regex_pattern=regex_pattern,
                param_converters=converters,
                is_fastpath_eligible=is_fast,
            )
