"""Inertia.js v3 protocol middleware and response primitives.

Implements the full Inertia v3 protocol: request detection, version conflict
handling, partial reloads, deferred/merge/deep-merge/once/scroll/optional
prop types, page object construction, SSR integration with CSR fallback,
and redirect fixup.
"""

from __future__ import annotations

import hashlib
import inspect
import logging
from collections.abc import AsyncIterable, Callable
from pathlib import Path
from typing import Any

import msgspec

from vertebra.build._assets import AssetManifest, EarlyHintsSpec, ResolvedAssetTags
from vertebra.framework.ssr import (
    AsyncSSRClient,
    SSRConfig,
    SSRResult,
    SyncSSRClient,
    render_page_shell,
)
from vertebra.utils.serializers import encode_json

__all__ = (
    "AsyncInertiaMiddleware",
    "AsyncSSRClient",
    "DeepMergeProp",
    "DeferredProp",
    "InertiaConfig",
    "InertiaHttpResponse",
    "InertiaResponse",
    "MergeProp",
    "OnceProp",
    "OptionalProp",
    "SSRResult",
    "ScrollProp",
    "SyncInertiaMiddleware",
    "SyncSSRClient",
    "inertia",
)

logger = logging.getLogger("vertebra.inertia")


class InertiaConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for the Inertia.js v3 middleware."""

    enabled: bool = False
    root_template: str = "index.html"
    version: str | None = None
    ssr: SSRConfig | None = None
    shared_data: dict[str, Any] = msgspec.field(default_factory=dict)
    static_props: dict[str, Any] = msgspec.field(default_factory=dict)
    manifest_path: str = "dist/.vite/manifest.json"
    asset_manifest: AssetManifest | None = None
    entry: str = "index.html"
    asset_url: str = "/static/"
    early_hints: bool = True
    csp_nonce_header: str = "X-CSP-Nonce"

    @property
    def ssr_enabled(self) -> bool:
        """Return True when SSR configuration is present."""
        return self.ssr is not None


class DeferredProp(msgspec.Struct, frozen=True, gc=False):
    """A prop whose value is deferred until a subsequent partial reload."""

    resolver: Callable[[], Any]
    group: str = "default"
    once: bool = False
    expires_in: int | None = None


class MergeProp(msgspec.Struct, frozen=True, gc=False):
    """A prop whose value is merged during partial reloads."""

    resolver: Callable[[], Any]
    prepend: bool = False


class DeepMergeProp(msgspec.Struct, frozen=True, gc=False):
    """A prop whose value is deep-merged during partial reloads."""

    resolver: Callable[[], Any]


class OnceProp(msgspec.Struct, frozen=True, gc=False):
    """A prop resolved only once; subsequent requests skip it if cached."""

    resolver: Callable[[], Any]
    expires_in: int | None = None


class ScrollProp(msgspec.Struct, frozen=True, gc=False):
    """A prop carrying infinite-scroll pagination metadata."""

    resolver: Callable[[], Any]
    page_name: str = "page"
    current_page: int = 1
    next_page: int | None = None
    previous_page: int | None = None


class OptionalProp(msgspec.Struct, frozen=True, gc=False):
    """A prop resolved only during partial reloads that explicitly request it."""

    resolver: Callable[[], Any]


class InertiaResponse(msgspec.Struct, frozen=True, gc=False):
    """Value returned by an Inertia page handler."""

    component: str
    props: dict[str, Any] = msgspec.field(default_factory=dict)
    encrypt_history: bool = False
    clear_history: bool = False


class InertiaHttpResponse(msgspec.Struct, frozen=True, gc=False):
    """HTTP response produced by Inertia middleware."""

    status: int = 200
    headers: dict[str, str] = msgspec.field(default_factory=dict)
    body: bytes = b""
    early_hints: EarlyHintsSpec | None = None


def inertia(
    component: str,
    props: dict[str, Any] | None = None,
    *,
    encrypt_history: bool = False,
    clear_history: bool = False,
) -> InertiaResponse:
    """Create an Inertia response descriptor."""
    return InertiaResponse(
        component=component,
        props=props or {},
        encrypt_history=encrypt_history,
        clear_history=clear_history,
    )


def _get_header(request: Any, name: str) -> str | None:
    """Lookup an HTTP request header case-insensitively."""
    headers = getattr(request, "headers", None)
    if headers is None:
        return None
    if hasattr(headers, "get"):
        val = headers.get(name)
        if val is not None:
            return str(val)
        lower_name = name.lower()
        val = headers.get(lower_name)
        if val is not None:
            return str(val)
        if hasattr(headers, "items"):
            for key, item_val in headers.items():
                if str(key).lower() == lower_name:
                    return str(item_val)
    return None


def _build_request_url(request: Any) -> str:
    """Build the Inertia page URL including non-empty query string."""
    path = str(getattr(request, "path", "/"))
    if "?" in path:
        return path
    query_val = getattr(request, "query_string", None)
    if query_val is None:
        query_val = getattr(request, "query", None)
    if query_val is None:
        query_val = getattr(request, "raw_query", None)
    if isinstance(query_val, bytes):
        query_str = query_val.decode("utf-8", errors="replace")
    elif query_val:
        query_str = str(query_val)
    else:
        query_str = ""
    if query_str.startswith("?"):
        query_str = query_str[1:]
    return f"{path}?{query_str}" if query_str else path


def _build_once_meta(key: str, expires_in: int | None) -> dict[str, Any]:
    """Construct Inertia v3 onceProps metadata entry."""
    meta: dict[str, Any] = {"prop": key}
    if expires_in is not None:
        meta["expiresAt"] = expires_in
    return meta


async def _resolve_async_value(value: Any) -> Any:
    """Resolve sync/async callables, awaitables, and async iterables."""
    if callable(value):
        value = value()
    if inspect.isawaitable(value):
        value = await value
    if isinstance(value, AsyncIterable) and not isinstance(value, (str, bytes, bytearray)):
        return [item async for item in value]
    return value


class _BaseInertiaMiddleware:
    """Shared Inertia.js v3 protocol configuration, prop resolution, and HTML shell rendering."""

    __slots__ = ("_config", "_ssr", "_version")

    def __init__(
        self,
        config: InertiaConfig | None = None,
        ssr_client: SyncSSRClient | AsyncSSRClient | None = None,
    ) -> None:
        self._config = config or InertiaConfig()
        self._ssr = ssr_client
        self._version: str | None = self._config.version

    def get_version(self) -> str:
        """Return the current asset version string."""
        if self._version is not None:
            return self._version
        return self._compute_manifest_version()

    def is_inertia_request(self, request: Any) -> bool:
        """Return True when the request carries X-Inertia: true."""
        value = _get_header(request, "X-Inertia") or ""
        return value.lower() == "true"

    def check_version(self, request: Any) -> str | None:
        """Compare client version against current version, returning current version on mismatch."""
        client_version = _get_header(request, "X-Inertia-Version") or ""
        current = self.get_version()
        if client_version and client_version != current:
            return current
        return None

    def _plan_props(
        self,
        request: Any,
        props: dict[str, Any],
        component: str,
        *,
        shared_keys: frozenset[str] = frozenset(),
    ) -> tuple[list[tuple[str, Any, bool]], dict[str, Any]]:
        """Plan prop evaluations and build Inertia v3 metadata."""
        partial_component, include_fields, exclude_fields = self._parse_partial(request)
        is_partial = partial_component is not None and partial_component == component

        reset_str = _get_header(request, "X-Inertia-Reset") or ""
        reset_set = set(self._parse_csv_header(reset_str)) if reset_str else set()
        except_once_str = _get_header(request, "X-Inertia-Except-Once-Props") or ""
        except_once = set(self._parse_csv_header(except_once_str)) if except_once_str else set()

        to_resolve: list[tuple[str, Any, bool]] = []
        deferred: dict[str, list[str]] = {}
        merge_props: list[str] = []
        prepend_props: list[str] = []
        deep_merge_props: list[str] = []
        scroll_props: dict[str, dict[str, Any]] = {}
        once_props: dict[str, dict[str, Any]] = {}

        for key, value in props.items():
            if is_partial and exclude_fields and key in exclude_fields:
                continue

            is_shared = key in shared_keys

            if isinstance(value, DeferredProp):
                if value.once and key in except_once and key not in reset_set:
                    continue
                if not is_partial or (include_fields and key not in include_fields):
                    deferred.setdefault(value.group, []).append(key)
                    if value.once and key not in reset_set:
                        once_props[key] = _build_once_meta(key, value.expires_in)
                    continue
                to_resolve.append((key, value.resolver, True))
                if value.once and key not in reset_set:
                    once_props[key] = _build_once_meta(key, value.expires_in)
            elif isinstance(value, MergeProp):
                if is_partial and include_fields and key not in include_fields and not is_shared:
                    continue
                to_resolve.append((key, value.resolver, True))
                if key not in reset_set:
                    if value.prepend:
                        prepend_props.append(key)
                    else:
                        merge_props.append(key)
            elif isinstance(value, DeepMergeProp):
                if is_partial and include_fields and key not in include_fields and not is_shared:
                    continue
                to_resolve.append((key, value.resolver, True))
                if key not in reset_set:
                    deep_merge_props.append(key)
            elif isinstance(value, OnceProp):
                if key in except_once and key not in reset_set:
                    continue
                if is_partial and include_fields and key not in include_fields and not is_shared:
                    continue
                to_resolve.append((key, value.resolver, True))
                if key not in reset_set:
                    once_props[key] = _build_once_meta(key, value.expires_in)
            elif isinstance(value, ScrollProp):
                if is_partial and include_fields and key not in include_fields and not is_shared:
                    continue
                to_resolve.append((key, value.resolver, True))
                scroll_props[key] = {
                    "pageName": value.page_name,
                    "currentPage": value.current_page,
                    "nextPage": value.next_page,
                    "previousPage": value.previous_page,
                }
            elif isinstance(value, OptionalProp):
                if is_partial and (not include_fields or key in include_fields):
                    to_resolve.append((key, value.resolver, True))
            else:
                if is_partial and include_fields and key not in include_fields and not is_shared:
                    continue
                to_resolve.append((key, value, callable(value)))

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

        return to_resolve, metadata

    def resolve_props(
        self,
        request: Any,
        props: dict[str, Any],
        component: str,
        *,
        shared_keys: frozenset[str] = frozenset(),
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Resolve props synchronously and build Inertia v3 metadata."""
        to_resolve, metadata = self._plan_props(request, props, component, shared_keys=shared_keys)
        resolved: dict[str, Any] = {}
        for key, value, is_call in to_resolve:
            resolved[key] = value() if is_call else value
        return resolved, metadata

    async def aresolve_props(
        self,
        request: Any,
        props: dict[str, Any],
        component: str,
        *,
        shared_keys: frozenset[str] = frozenset(),
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Resolve sync/async props and async iterables and build Inertia v3 metadata."""
        to_resolve, metadata = self._plan_props(request, props, component, shared_keys=shared_keys)
        resolved: dict[str, Any] = {}
        for key, value, _ in to_resolve:
            resolved[key] = await _resolve_async_value(value)
        return resolved, metadata

    def _assemble_page_object(
        self,
        request: Any,
        response: InertiaResponse,
        resolved_props: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Assemble the final Inertia v3 page payload dictionary."""
        page: dict[str, Any] = {
            "component": response.component,
            "props": resolved_props,
            "url": _build_request_url(request),
            "version": self.get_version(),
        }
        if response.encrypt_history:
            page["encryptHistory"] = True
        if response.clear_history:
            page["clearHistory"] = True
        page.update(metadata)
        return page

    def _merge_props(self, response: InertiaResponse) -> tuple[dict[str, Any], frozenset[str]]:
        """Merge configured shared_data with response props."""
        merged_props: dict[str, Any] = {}
        shared_keys = frozenset(self._config.shared_data.keys()) if self._config.shared_data else frozenset()
        if self._config.shared_data:
            merged_props.update(self._config.shared_data)
        merged_props.update(response.props)
        return merged_props, shared_keys

    def build_page_object(self, request: Any, response: InertiaResponse) -> dict[str, Any]:
        """Construct the full Inertia v3 page object synchronously."""
        merged_props, shared_keys = self._merge_props(response)
        resolved_props, metadata = self.resolve_props(
            request,
            merged_props,
            response.component,
            shared_keys=shared_keys,
        )
        return self._assemble_page_object(request, response, resolved_props, metadata)

    async def abuild_page_object(self, request: Any, response: InertiaResponse) -> dict[str, Any]:
        """Construct the full Inertia v3 page object asynchronously."""
        merged_props, shared_keys = self._merge_props(response)
        resolved_props, metadata = await self.aresolve_props(
            request,
            merged_props,
            response.component,
            shared_keys=shared_keys,
        )
        return self._assemble_page_object(request, response, resolved_props, metadata)

    def _render_html_response(
        self,
        request: Any,
        page: dict[str, Any],
        ssr_result: SSRResult | None,
    ) -> InertiaHttpResponse:
        """Render the HTML page shell with resolved asset tags, early hints, and CSP nonce."""
        csp_nonce = _get_header(request, self._config.csp_nonce_header) or None

        asset_tags: ResolvedAssetTags | None = None
        early_hints_spec: EarlyHintsSpec | None = None
        resp_headers: dict[str, str] = {
            "Content-Type": "text/html; charset=utf-8",
            "Vary": "X-Inertia",
        }
        manifest = self._config.asset_manifest
        if manifest is not None and self._config.entry in manifest.entries:
            asset_tags = manifest.resolve_entry(
                self._config.entry,
                asset_url=self._config.asset_url,
                csp_nonce=csp_nonce,
            )
            if self._config.early_hints and asset_tags.early_hints.links:
                early_hints_spec = asset_tags.early_hints
                resp_headers["Link"] = early_hints_spec.to_link_header()

        html = render_page_shell(
            page,
            ssr_result,
            self._config.root_template,
            asset_tags=asset_tags,
            csp_nonce=csp_nonce,
            static_props=self._config.static_props or None,
        )
        return InertiaHttpResponse(
            status=200,
            headers=resp_headers,
            body=html.encode("utf-8"),
            early_hints=early_hints_spec,
        )

    def _compute_manifest_version(self) -> str:
        manifest_path = Path(self._config.manifest_path)
        if manifest_path.exists():
            content = manifest_path.read_bytes()
            return hashlib.blake2b(content, digest_size=4).hexdigest()
        return "dev"

    def _parse_partial(self, request: Any) -> tuple[str | None, list[str], list[str]]:
        """Extract (component, include_fields, exclude_fields) for partial reloads."""
        component = _get_header(request, "X-Inertia-Partial-Component")
        include_str = _get_header(request, "X-Inertia-Partial-Data") or ""
        exclude_str = _get_header(request, "X-Inertia-Partial-Except") or ""
        return (
            component,
            self._parse_csv_header(include_str) if include_str else [],
            self._parse_csv_header(exclude_str) if exclude_str else [],
        )

    @staticmethod
    def _parse_csv_header(value: str) -> list[str]:
        """Parse comma-separated values from a header string."""
        return [field.strip() for field in value.split(",") if field.strip()]


class SyncInertiaMiddleware(_BaseInertiaMiddleware):
    """Synchronous Inertia.js v3 protocol handler for Vertebra native handlers."""

    __slots__ = ()

    def __init__(
        self,
        config: InertiaConfig | None = None,
        ssr_client: SyncSSRClient | None = None,
    ) -> None:
        super().__init__(config=config, ssr_client=ssr_client)

    def process(self, request: Any, response: InertiaResponse) -> InertiaHttpResponse:
        """Process an Inertia response into a full HTTP response synchronously."""
        is_inertia = self.is_inertia_request(request)

        req_method = getattr(request, "method", "GET")
        req_url = _build_request_url(request)

        if is_inertia and req_method.upper() == "GET":
            conflict_version = self.check_version(request)
            if conflict_version is not None:
                return InertiaHttpResponse(
                    status=409,
                    headers={"X-Inertia-Location": req_url},
                    body=b"",
                )

        page = self.build_page_object(request, response)

        if is_inertia:
            body = encode_json(page, as_bytes=True)
            return InertiaHttpResponse(
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "X-Inertia": "true",
                    "Vary": "X-Inertia",
                },
                body=body,
            )

        ssr_result: SSRResult | None = None
        if self._config.ssr_enabled and self._ssr is not None:
            try:
                render_fn = self._ssr.render
                if inspect.iscoroutinefunction(render_fn):
                    logger.warning("AsyncSSRClient passed to synchronous process(); falling back to CSR")
                    ssr_result = None
                else:
                    res = render_fn(page)
                    if isinstance(res, SSRResult):
                        ssr_result = res
            except Exception:
                logger.warning("SSR render failed, falling back to CSR", exc_info=True)
                ssr_result = None

        return self._render_html_response(request, page, ssr_result)


class AsyncInertiaMiddleware(_BaseInertiaMiddleware):
    """Asynchronous Inertia.js v3 protocol handler for Vertebra native handlers."""

    __slots__ = ()

    def __init__(
        self,
        config: InertiaConfig | None = None,
        ssr_client: AsyncSSRClient | None = None,
    ) -> None:
        super().__init__(config=config, ssr_client=ssr_client)

    async def process(self, request: Any, response: InertiaResponse) -> InertiaHttpResponse:
        """Process an Inertia response into a full HTTP response asynchronously."""
        is_inertia = self.is_inertia_request(request)

        req_method = getattr(request, "method", "GET")
        req_url = _build_request_url(request)

        if is_inertia and req_method.upper() == "GET":
            conflict_version = self.check_version(request)
            if conflict_version is not None:
                return InertiaHttpResponse(
                    status=409,
                    headers={"X-Inertia-Location": req_url},
                    body=b"",
                )

        page = await self.abuild_page_object(request, response)

        if is_inertia:
            body = encode_json(page, as_bytes=True)
            return InertiaHttpResponse(
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "X-Inertia": "true",
                    "Vary": "X-Inertia",
                },
                body=body,
            )

        ssr_result: SSRResult | None = None
        if self._config.ssr_enabled and self._ssr is not None:
            try:
                render_fn = self._ssr.render
                if inspect.iscoroutinefunction(render_fn):
                    ssr_result = await render_fn(page)
                else:
                    res = render_fn(page)
                    if inspect.isawaitable(res):
                        ssr_result = await res
                    elif isinstance(res, SSRResult):
                        ssr_result = res
            except Exception:
                logger.warning("SSR render failed, falling back to CSR", exc_info=True)
                ssr_result = None

        return self._render_html_response(request, page, ssr_result)
