"""Inertia.js v3 protocol middleware for Vertebra.

Implements the full Inertia v3 protocol: request detection, version conflict
handling, partial reloads, deferred/merge/deep-merge/once/scroll/optional
prop types, page object construction, SSR integration with CSR fallback,
and redirect fixup.
"""

import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from vertebra.framework._types import Headers, Response
from vertebra.utils.serializers._json import encode_json

__all__ = (
    "DeepMergeProp",
    "DeferredProp",
    "InertiaConfig",
    "InertiaMiddleware",
    "InertiaResponse",
    "MergeProp",
    "OnceProp",
    "OptionalProp",
    "ScrollProp",
    "SsrClient",
    "SsrResult",
    "deep_merge",
    "defer",
    "inertia",
    "merge",
    "once",
    "optional",
    "scroll",
)

logger = logging.getLogger("vertebra.inertia")


@dataclass
class InertiaConfig:
    """Configuration for the Inertia.js v3 middleware."""

    enabled: bool = False
    root_template: str = "index.html"
    version: str | None = None
    ssr_enabled: bool = False
    ssr_mode: str = "native"
    ssr_url: str = "http://127.0.0.1:13714"
    ssr_timeout: float = 2.0
    ssr_pool_size: int = 4
    ssr_entry: str | None = None
    sidecar_port: int = 13714
    shared_data: dict[str, Any] = field(default_factory=dict)
    manifest_path: str = "dist/.vite/manifest.json"


class DeferredProp:
    """A prop whose value is deferred until a subsequent partial reload."""

    __slots__ = ("group", "once", "resolver")

    def __init__(self, resolver: Callable[[], Any], *, group: str = "default", once: bool = False) -> None:
        self.resolver = resolver
        self.group = group
        self.once = once


class MergeProp:
    """A prop whose value is merged (appended or prepended) during partial reloads."""

    __slots__ = ("prepend", "resolver")

    def __init__(self, resolver: Callable[[], Any], *, prepend: bool = False) -> None:
        self.resolver = resolver
        self.prepend = prepend


class DeepMergeProp:
    """A prop whose value is deep-merged during partial reloads."""

    __slots__ = ("resolver",)

    def __init__(self, resolver: Callable[[], Any]) -> None:
        self.resolver = resolver


class OnceProp:
    """A prop resolved only once; subsequent requests skip it if cached."""

    __slots__ = ("expires_in", "resolver")

    def __init__(self, resolver: Callable[[], Any], *, expires_in: int | None = None) -> None:
        self.resolver = resolver
        self.expires_in = expires_in


class ScrollProp:
    """A prop carrying infinite-scroll pagination metadata."""

    __slots__ = ("current_page", "next_page", "page_name", "previous_page", "resolver")

    def __init__(
        self,
        resolver: Callable[[], Any],
        *,
        page_name: str = "page",
        current_page: int = 1,
        next_page: int | None = None,
        previous_page: int | None = None,
    ) -> None:
        self.resolver = resolver
        self.page_name = page_name
        self.current_page = current_page
        self.next_page = next_page
        self.previous_page = previous_page


class OptionalProp:
    """A prop resolved only during partial reloads that explicitly request it."""

    __slots__ = ("resolver",)

    def __init__(self, resolver: Callable[[], Any]) -> None:
        self.resolver = resolver


def defer(resolver: Callable[[], Any], *, group: str = "default", once: bool = False) -> DeferredProp:
    """Create a deferred prop."""
    return DeferredProp(resolver, group=group, once=once)


def merge(resolver: Callable[[], Any], *, prepend: bool = False) -> MergeProp:
    """Create a merge prop."""
    return MergeProp(resolver, prepend=prepend)


def deep_merge(resolver: Callable[[], Any]) -> DeepMergeProp:
    """Create a deep-merge prop."""
    return DeepMergeProp(resolver)


def once(resolver: Callable[[], Any], *, expires_in: int | None = None) -> OnceProp:
    """Create a once prop."""
    return OnceProp(resolver, expires_in=expires_in)


def scroll(
    resolver: Callable[[], Any],
    *,
    page_name: str = "page",
    current_page: int = 1,
    next_page: int | None = None,
    previous_page: int | None = None,
) -> ScrollProp:
    """Create a scroll prop with pagination metadata."""
    return ScrollProp(
        resolver,
        page_name=page_name,
        current_page=current_page,
        next_page=next_page,
        previous_page=previous_page,
    )


def optional(resolver: Callable[[], Any]) -> OptionalProp:
    """Create an optional prop."""
    return OptionalProp(resolver)


@dataclass
class InertiaResponse:
    """Value returned by an Inertia page handler."""

    component: str
    props: dict[str, Any] = field(default_factory=dict)
    encrypt_history: bool = False
    clear_history: bool = False


def inertia(
    component: str,
    props: dict[str, Any] | None = None,
    *,
    encrypt_history: bool = False,
    clear_history: bool = False,
) -> InertiaResponse:
    """Create an Inertia response."""
    return InertiaResponse(
        component=component,
        props=props or {},
        encrypt_history=encrypt_history,
        clear_history=clear_history,
    )


@dataclass
class SsrResult:
    """Result from an SSR render call."""

    head: list[str]
    body: str


class SsrClient(Protocol):
    """Protocol for SSR render clients (sidecar HTTP or embedded V8)."""

    def render(self, page: dict[str, Any]) -> SsrResult: ...


class InertiaMiddleware:
    """Inertia.js v3 protocol handler for Vertebra native handlers."""

    __slots__ = ("_config", "_ssr", "_version")

    def __init__(self, config: InertiaConfig | None = None, ssr_client: SsrClient | None = None) -> None:
        self._config = config or InertiaConfig()
        self._ssr = ssr_client
        self._version: str | None = self._config.version

    def get_version(self) -> str:
        """Return the current asset version string."""
        if self._version is not None:
            return self._version
        return self._compute_manifest_version()

    def is_inertia_request(self, request: Any) -> bool:
        """Return ``True`` when the request carries ``X-Inertia: true``."""
        value: str = request.headers.get("X-Inertia", "")
        return value.lower() == "true"

    def check_version(self, request: Any) -> str | None:
        """Compare client version against current version, returning current version on mismatch."""
        client_version = request.headers.get("X-Inertia-Version", "")
        current = self.get_version()
        if client_version and client_version != current:
            return current
        return None

    def resolve_props(
        self,
        request: Any,
        props: dict[str, Any],
        component: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Resolve props and build Inertia v3 metadata."""
        partial_component, include_fields, exclude_fields = self._parse_partial(request)
        is_partial = partial_component is not None and partial_component == component
        reset_str = request.headers.get("X-Inertia-Reset", "")
        reset_fields = [f.strip() for f in reset_str.split(",") if f.strip()] if reset_str else []
        except_once_str = request.headers.get("X-Inertia-Except-Once-Props", "")
        except_once = [f.strip() for f in except_once_str.split(",") if f.strip()] if except_once_str else []

        resolved: dict[str, Any] = {}
        deferred: dict[str, list[str]] = {}
        merge_props: list[str] = []
        prepend_props: list[str] = []
        deep_merge_props: list[str] = []
        scroll_props: dict[str, dict[str, Any]] = {}
        once_props: dict[str, dict[str, Any]] = {}

        for key, value in props.items():
            if is_partial and exclude_fields and key in exclude_fields:
                continue

            if isinstance(value, DeferredProp):
                if not is_partial:
                    deferred.setdefault(value.group, []).append(key)
                    continue
                if include_fields and key not in include_fields:
                    deferred.setdefault(value.group, []).append(key)
                    continue
                resolved[key] = value.resolver()
            elif isinstance(value, MergeProp):
                if is_partial and include_fields and key not in include_fields:
                    continue
                resolved[key] = value.resolver()
                if value.prepend:
                    prepend_props.append(key)
                else:
                    merge_props.append(key)
            elif isinstance(value, DeepMergeProp):
                if is_partial and include_fields and key not in include_fields:
                    continue
                resolved[key] = value.resolver()
                deep_merge_props.append(key)
            elif isinstance(value, OnceProp):
                if key in except_once:
                    continue
                if is_partial and include_fields and key not in include_fields:
                    continue
                resolved[key] = value.resolver()
                meta: dict[str, Any] = {"prop": key}
                if value.expires_in is not None:
                    meta["expiresAt"] = value.expires_in
                once_props[key] = meta
            elif isinstance(value, ScrollProp):
                if is_partial and include_fields and key not in include_fields:
                    continue
                resolved[key] = value.resolver()
                scroll_props[key] = {
                    "pageName": value.page_name,
                    "currentPage": value.current_page,
                    "nextPage": value.next_page,
                    "previousPage": value.previous_page,
                }
            elif isinstance(value, OptionalProp):
                if is_partial and (not include_fields or key in include_fields):
                    resolved[key] = value.resolver()
            elif callable(value):
                if is_partial and include_fields and key not in include_fields:
                    continue
                resolved[key] = value()
            else:
                if is_partial and include_fields and key not in include_fields:
                    continue
                resolved[key] = value

        metadata: dict[str, Any] = {}
        if deferred:
            metadata["deferredProps"] = deferred
        if merge_props:
            metadata["mergeProps"] = merge_props
        if prepend_props:
            metadata["prependProps"] = prepend_props
        if deep_merge_props:
            metadata["deepMergeProps"] = deep_merge_props
        if scroll_props:
            metadata["scrollProps"] = scroll_props
        if once_props:
            metadata["onceProps"] = once_props
        if reset_fields:
            metadata["resetProps"] = reset_fields

        return resolved, metadata

    def build_page_object(self, request: Any, response: InertiaResponse) -> dict[str, Any]:
        """Construct the full Inertia v3 page object."""
        merged_props: dict[str, Any] = {}
        if self._config.shared_data:
            merged_props.update(self._config.shared_data)
        merged_props.update(response.props)

        resolved_props, metadata = self.resolve_props(request, merged_props, response.component)

        page: dict[str, Any] = {
            "component": response.component,
            "props": resolved_props,
            "url": request.path,
            "version": self.get_version(),
        }

        if response.encrypt_history:
            page["encryptHistory"] = True
        if response.clear_history:
            page["clearHistory"] = True

        page.update(metadata)
        return page

    def should_redirect_303(self, method: str, status_code: int) -> bool:
        """Return ``True`` when a 301/302 redirect after PUT/PATCH/DELETE should become 303."""
        return method.upper() in ("PUT", "PATCH", "DELETE") and status_code in (301, 302)

    def process(self, request: Any, response: InertiaResponse) -> Response:
        """Process an Inertia response into a full HTTP response."""
        is_inertia = self.is_inertia_request(request)

        if is_inertia and request.method.upper() == "GET":
            conflict_version = self.check_version(request)
            if conflict_version is not None:
                return Response(
                    status=409,
                    headers=Headers(
                        _data=[
                            ("X-Inertia-Location", request.path),
                        ]
                    ),
                    body=b"",
                )

        page = self.build_page_object(request, response)

        if is_inertia:
            body = encode_json(page, as_bytes=True)
            return Response(
                status=200,
                headers=Headers(
                    _data=[
                        ("Content-Type", "application/json"),
                        ("X-Inertia", "true"),
                        ("Vary", "X-Inertia"),
                    ]
                ),
                body=body,
            )

        ssr_result: SsrResult | None = None
        if self._config.ssr_enabled and self._ssr is not None:
            try:
                ssr_result = self._ssr.render(page)
            except Exception:
                logger.warning("SSR render failed, falling back to CSR", exc_info=True)
                ssr_result = None

        html = self._render_html(page, ssr_result)
        return Response(
            status=200,
            headers=Headers(
                _data=[
                    ("Content-Type", "text/html; charset=utf-8"),
                    ("Vary", "X-Inertia"),
                ]
            ),
            body=html.encode("utf-8"),
        )

    def _compute_manifest_version(self) -> str:
        manifest_path = Path(self._config.manifest_path)
        if manifest_path.exists():
            content = manifest_path.read_bytes()
            return hashlib.md5(content).hexdigest()[:8]
        return "dev"

    def _parse_partial(self, request: Any) -> tuple[str | None, list[str], list[str]]:
        """Extract ``(component, include_fields, exclude_fields)`` for partial reloads."""
        component = request.headers.get("X-Inertia-Partial-Component")
        include_str = request.headers.get("X-Inertia-Partial-Data", "")
        exclude_str = request.headers.get("X-Inertia-Partial-Except", "")
        include = [f.strip() for f in include_str.split(",") if f.strip()] if include_str else []
        exclude = [f.strip() for f in exclude_str.split(",") if f.strip()] if exclude_str else []
        return component, include, exclude

    def _render_html(self, page: dict[str, Any], ssr_result: SsrResult | None = None) -> str:
        """Render the initial HTML page with embedded Inertia v3 page data."""
        page_json = encode_json(page)

        if ssr_result is not None:
            head_html = "\n".join(ssr_result.head)
            body_html = ssr_result.body
        else:
            head_html = ""
            body_html = ""

        return (
            "<!DOCTYPE html>\n"
            "<html>\n"
            "<head>\n"
            f"{head_html}\n"
            "</head>\n"
            "<body>\n"
            '<div id="app">\n'
            f"{body_html}\n"
            '<script data-page="app" type="application/json">\n'
            f"{page_json}\n"
            "</script>\n"
            "</div>\n"
            "</body>\n"
            "</html>"
        )
