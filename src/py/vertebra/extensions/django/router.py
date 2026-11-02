"""Django URLPattern / URLResolver to Vertebra CompiledRouter compiler."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.server._routing import make_handler_id
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

__all__ = (
    "CompiledRouter",
    "RouteCompiler",
    "RouteEntry",
    "convert_django_pattern",
)

_DJANGO_PARAM_RE = re.compile(r"<(?:(?P<converter>[a-zA-Z_][a-zA-Z0-9_]*):)?(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)>")
_REGEX_GROUP_RE = re.compile(r"\(\?P<(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)>(?P<expr>[^)]+)\)")


def convert_django_pattern(
    pattern_str: str,
    *,
    is_regex: bool = False,
) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Convert a Django ``path()`` or ``re_path()`` route string into matchit 0.9 syntax.

    Args:
        pattern_str: Django URL route pattern string.
        is_regex: Whether the pattern originates from ``re_path()`` / regex syntax.

    Returns:
        Tuple of ``(matchit_pattern, ((param_name, converter_name), ...))``.
    """
    cleaned = pattern_str.strip("^$")
    converters: list[tuple[str, str]] = []

    if is_regex or "(?P<" in cleaned:

        def _replace_re(match: re.Match[str]) -> str:
            name = match.group("name")
            expr = match.group("expr")
            conv = "int" if expr in {r"\d+", "[0-9]+"} else ("path" if ".*" in expr or ".+" in expr else "str")
            converters.append((name, conv))
            if conv == "path":
                return f"{{*{name}}}"
            return f"{{{name}}}"

        converted = _REGEX_GROUP_RE.sub(_replace_re, cleaned)
    else:

        def _replace_path(match: re.Match[str]) -> str:
            conv = (match.group("converter") or "str").lower()
            name = match.group("name")
            converters.append((name, conv))
            if conv == "path":
                return f"{{*{name}}}"
            return f"{{{name}}}"

        converted = _DJANGO_PARAM_RE.sub(_replace_path, cleaned)

    if not converted.startswith("/"):
        converted = "/" + converted
    return converted, tuple(converters)


def _compile_django_regex(matchit_path: str) -> re.Pattern[str]:
    """Compile a normalized matchit path into a named-group regex."""
    segments: list[str] = []
    for seg in matchit_path.split("/"):
        if seg.startswith("{*") and seg.endswith("}"):
            name = seg[2:-1]
            segments.append(f"(?P<{name}>.+)")
        elif seg.startswith("{") and seg.endswith("}"):
            name = seg[1:-1]
            segments.append(f"(?P<{name}>[^/]+)")
        else:
            segments.append(re.escape(seg))
    joined = "/".join(segments) or "/"
    return re.compile(f"^{joined}$")


class RouteEntry(msgspec.Struct, frozen=True, gc=False):
    """Metadata for a compiled Django URL pattern."""

    pattern: str
    matchit_path: str
    name: str
    methods: tuple[str, ...]
    handler_id: int
    callback: Callable[..., Any]
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
    """Recursively walks Django ``URLPattern`` and ``URLResolver`` trees into a ``CompiledRouter``."""

    __slots__ = ("_entries", "_router")

    def __init__(
        self,
        *,
        urlpatterns: Iterable[Any] | None = None,
        urlconf: str | Any = None,
    ) -> None:
        self._entries: dict[int, RouteEntry] = {}
        self._router: CompiledRouter | None = None
        if urlpatterns is not None:
            self.compile_urlpatterns(urlpatterns)
        elif urlconf is not None:
            self.compile_urlconf(urlconf)

    @property
    def entries(self) -> list[RouteEntry]:
        """Return the compiled ``RouteEntry`` list."""
        return list(self._entries.values())

    @property
    def entries_by_id(self) -> dict[int, RouteEntry]:
        """Return mapping from ``handler_id`` to ``RouteEntry``."""
        return self._entries

    @property
    def router(self) -> CompiledRouter:
        """Return the compiled router."""
        if self._router is None:
            self._router = CompiledRouter(self._entries)
        return self._router

    def compile_urlconf(self, urlconf: str | Any) -> CompiledRouter:
        """Compile a Django ``ROOT_URLCONF`` module path or module object."""
        mod = import_optional(urlconf) if isinstance(urlconf, str) else urlconf
        patterns = getattr(mod, "urlpatterns", ()) if mod is not None else ()
        return self.compile_urlpatterns(patterns)

    def compile_urlpatterns(self, urlpatterns: Iterable[Any], *, prefix: str = "") -> CompiledRouter:
        """Walk an iterable of Django ``URLPattern`` and ``URLResolver`` objects."""
        for item in urlpatterns:
            raw_pat = getattr(item, "pattern", getattr(item, "route", ""))
            regex_attr = getattr(raw_pat, "_regex", None)
            route_attr = getattr(raw_pat, "_route", None)
            pat_str = str(regex_attr if regex_attr is not None else (route_attr if route_attr is not None else raw_pat))
            combined = f"{prefix.rstrip('/')}/{pat_str.lstrip('^/')}" if prefix else pat_str
            nested = getattr(item, "url_patterns", None)
            if nested is not None:
                self.compile_urlpatterns(nested, prefix=combined)
                continue
            callback = getattr(item, "callback", None)
            if callback is None:
                continue
            name = str(getattr(item, "name", None) or getattr(callback, "__name__", "view"))
            methods = getattr(callback, "__vertebra_methods__", ("GET", "POST", "PUT", "PATCH", "DELETE"))
            self.register_pattern(combined, name, tuple(methods), callback)
        self._router = CompiledRouter(self._entries)
        return self._router

    def register_pattern(
        self,
        pattern_str: str,
        name: str,
        methods: tuple[str, ...],
        callback: Callable[..., Any],
    ) -> None:
        """Register a single Django URL pattern and view callback."""
        matchit_path, converters = convert_django_pattern(pattern_str)
        regex_pattern = _compile_django_regex(matchit_path)
        is_fast = bool(getattr(callback, "__vertebra_fastpath__", False))
        for method in methods:
            method_upper = method.upper()
            handler_id = make_handler_id(method_upper, matchit_path)
            self._entries[handler_id] = RouteEntry(
                pattern=pattern_str,
                matchit_path=matchit_path,
                name=name,
                methods=(method_upper,),
                handler_id=handler_id,
                callback=callback,
                regex_pattern=regex_pattern,
                param_converters=converters,
                is_fastpath_eligible=is_fast,
            )
