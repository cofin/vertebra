"""Inertia.js v3 protocol middleware and response primitives.

Implements the full Inertia v3 protocol: request detection, version conflict
handling, partial reloads, deferred/merge/deep-merge/once/scroll/optional
prop types, page object construction, SSR integration with CSR fallback,
and redirect fixup.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec

from vertebra.framework.ssr import SSRClient, SSRConfig, SSRResult, render_page_shell
from vertebra.utils.serializers import encode_json

__all__ = (
    "DeepMergeProp",
    "DeferredProp",
    "InertiaConfig",
    "InertiaHttpResponse",
    "InertiaMiddleware",
    "InertiaResponse",
    "MergeProp",
    "OnceProp",
    "OptionalProp",
    "SSRClient",
    "SSRResult",
    "ScrollProp",
    "inertia",
)

logger = logging.getLogger("vertebra.inertia")


def _parse_csv_header(value: str) -> list[str]:
    """Parse comma-separated values from a header string."""
    return [field.strip() for field in value.split(",") if field.strip()]


class InertiaConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for the Inertia.js v3 middleware."""

    enabled: bool = False
    root_template: str = "index.html"
    version: str | None = None
    ssr: SSRConfig | None = None
    shared_data: dict[str, Any] = msgspec.field(default_factory=dict)
    manifest_path: str = "dist/.vite/manifest.json"

    @property
    def ssr_enabled(self) -> bool:
        """Return True when SSR configuration is present."""
        return self.ssr is not None


class DeferredProp(msgspec.Struct, frozen=True, gc=False):
    """A prop whose value is deferred until a subsequent partial reload."""

    resolver: Callable[[], Any]
    group: str = "default"
    once: bool = False


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


class InertiaMiddleware:
    """Inertia.js v3 protocol handler for Vertebra native handlers."""

    __slots__ = ("_config", "_ssr", "_version")

    def __init__(self, config: InertiaConfig | None = None, ssr_client: SSRClient | None = None) -> None:
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
        headers = request.headers
        value: str = headers.get("X-Inertia", "") if hasattr(headers, "get") else ""
        return str(value).lower() == "true"

    def check_version(self, request: Any) -> str | None:
        """Compare client version against current version, returning current version on mismatch."""
        headers = request.headers
        client_version = headers.get("X-Inertia-Version", "") if hasattr(headers, "get") else ""
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

        headers = request.headers
        reset_str = headers.get("X-Inertia-Reset", "") if hasattr(headers, "get") else ""
        reset_fields = _parse_csv_header(str(reset_str)) if reset_str else []
        except_once_str = headers.get("X-Inertia-Except-Once-Props", "") if hasattr(headers, "get") else ""
        except_once = _parse_csv_header(str(except_once_str)) if except_once_str else []

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
            "url": getattr(request, "path", "/"),
            "version": self.get_version(),
        }

        if response.encrypt_history:
            page["encryptHistory"] = True
        if response.clear_history:
            page["clearHistory"] = True

        page.update(metadata)
        return page

    def should_redirect_303(self, method: str, status_code: int) -> bool:
        """Return True when a 301/302 redirect after PUT/PATCH/DELETE should become 303."""
        return method.upper() in ("PUT", "PATCH", "DELETE") and status_code in (301, 302)

    def process(self, request: Any, response: InertiaResponse) -> InertiaHttpResponse:
        """Process an Inertia response into a full HTTP response."""
        is_inertia = self.is_inertia_request(request)

        req_method = getattr(request, "method", "GET")
        req_path = getattr(request, "path", "/")

        if is_inertia and req_method.upper() == "GET":
            conflict_version = self.check_version(request)
            if conflict_version is not None:
                return InertiaHttpResponse(
                    status=409,
                    headers={"X-Inertia-Location": req_path},
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
                ssr_result = self._ssr.render(page)
            except Exception:
                logger.warning("SSR render failed, falling back to CSR", exc_info=True)
                ssr_result = None

        html = render_page_shell(page, ssr_result, self._config.root_template)
        return InertiaHttpResponse(
            status=200,
            headers={
                "Content-Type": "text/html; charset=utf-8",
                "Vary": "X-Inertia",
            },
            body=html.encode("utf-8"),
        )

    def _compute_manifest_version(self) -> str:
        manifest_path = Path(self._config.manifest_path)
        if manifest_path.exists():
            content = manifest_path.read_bytes()
            return hashlib.blake2b(content, digest_size=4).hexdigest()
        return "dev"

    def _parse_partial(self, request: Any) -> tuple[str | None, list[str], list[str]]:
        """Extract (component, include_fields, exclude_fields) for partial reloads."""
        headers = request.headers
        component = headers.get("X-Inertia-Partial-Component") if hasattr(headers, "get") else None
        include_str = headers.get("X-Inertia-Partial-Data", "") if hasattr(headers, "get") else ""
        exclude_str = headers.get("X-Inertia-Partial-Except", "") if hasattr(headers, "get") else ""
        return (
            component,
            _parse_csv_header(str(include_str)) if include_str else [],
            _parse_csv_header(str(exclude_str)) if exclude_str else [],
        )
