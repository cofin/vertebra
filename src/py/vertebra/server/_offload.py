"""Framework-neutral offload specs, CORS/OpenAPI evaluators, and worker offload detection."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from typing import Any

import msgspec

from vertebra.utils.serializers import encode_json

__all__ = (
    "CompressionOffloadSpec",
    "CorsOffloadSpec",
    "OffloadPlan",
    "OpenApiOffloadSpec",
    "StaticMountOffloadSpec",
    "evaluate_cors_preflight",
    "evaluate_cors_response_headers",
    "evaluate_openapi_cache_request",
    "is_rust_offload_active",
)

_TRUTHY_FLAGS: frozenset[str] = frozenset({"1", "true", "yes", "on"})
_FALSY_FLAGS: frozenset[str] = frozenset({"0", "false", "no", "off"})


class CompressionOffloadSpec(msgspec.Struct, frozen=True, gc=False):
    """Framework-neutral compression configuration for Rust vertebra-http."""

    backend: str
    minimum_size: int
    gzip_compress_level: int
    brotli_quality: int
    zstd_level: int
    exclude_patterns: tuple[str, ...]
    exclude_opt_key: str | None
    excluded_paths: tuple[str, ...]


class CorsOffloadSpec(msgspec.Struct, frozen=True, gc=False):
    """Framework-neutral CORS configuration for Rust vertebra-http."""

    allow_origins: tuple[str, ...]
    allow_methods: tuple[str, ...]
    allow_headers: tuple[str, ...]
    allow_credentials: bool
    expose_headers: tuple[str, ...]
    max_age: int
    allow_origin_regex: str | None = None


class StaticMountOffloadSpec(msgspec.Struct, frozen=True, gc=False):
    """Framework-neutral static file mount specification for Rust asset serving."""

    path_prefix: str
    directories: tuple[str, ...]
    html_mode: bool
    send_as_attachment: bool
    headers: dict[str, str]


class OpenApiOffloadSpec(msgspec.Struct, frozen=True, gc=False):
    """Pre-rendered OpenAPI JSON payload and ETag for zero-GIL Rust serving."""

    schema_path: str
    body: bytes
    etag: str
    content_type: str = "application/vnd.oai.openapi+json"


class OffloadPlan(msgspec.Struct, frozen=True, gc=False):
    """Complete startup offload plan extracted from a host web application."""

    compression: CompressionOffloadSpec | None
    cors: CorsOffloadSpec | None
    static_mounts: tuple[StaticMountOffloadSpec, ...]
    openapi: OpenApiOffloadSpec | None

    def to_server_config(self) -> dict[str, Any]:
        """Serialize this offload plan into the dictionary schema consumed by vertebra-http ServerConfig."""
        compression_payload: dict[str, Any] | None = None
        if self.compression is not None:
            compression_payload = {
                "backend": self.compression.backend,
                "minimum_size": self.compression.minimum_size,
                "gzip_compress_level": self.compression.gzip_compress_level,
                "brotli_quality": self.compression.brotli_quality,
                "zstd_level": self.compression.zstd_level,
                "exclude_patterns": list(self.compression.exclude_patterns),
                "excluded_paths": list(self.compression.excluded_paths),
            }

        cors_payload: dict[str, Any] | None = None
        if self.cors is not None:
            cors_payload = {
                "allow_origins": list(self.cors.allow_origins),
                "allow_methods": list(self.cors.allow_methods),
                "allow_headers": list(self.cors.allow_headers),
                "allow_credentials": self.cors.allow_credentials,
                "expose_headers": list(self.cors.expose_headers),
                "max_age": max(0, self.cors.max_age),
                "allow_origin_regex": self.cors.allow_origin_regex,
            }

        mounts_payload: list[dict[str, Any]] = [
            {
                "path_prefix": mount.path_prefix,
                "directories": list(mount.directories),
                "html_mode": mount.html_mode,
                "send_as_attachment": mount.send_as_attachment,
                "headers": dict(mount.headers),
            }
            for mount in self.static_mounts
        ]

        openapi_payload: dict[str, Any] | None = None
        if self.openapi is not None:
            openapi_payload = {
                "schema_path": self.openapi.schema_path,
                "body": self.openapi.body.decode("utf-8"),
                "etag": self.openapi.etag,
                "content_type": self.openapi.content_type,
            }

        return {
            "compression": compression_payload,
            "cors": cors_payload,
            "static_mounts": mounts_payload,
            "openapi": openapi_payload,
        }

    def to_server_config_json(self) -> str:
        """Encode this offload plan as a JSON string for VERTEBRA_OFFLOAD_CONFIG."""
        return encode_json(self.to_server_config())


def is_rust_offload_active(worker_state: Any = None) -> bool:
    """Return True when Rust ingress offloading is active via worker state or environment flags."""
    if worker_state is not None:
        if isinstance(worker_state, bool):
            return worker_state
        for key in ("rust_offload_active", "vertebra_rust_offload", "server_active"):
            raw = worker_state.get(key) if hasattr(worker_state, "get") else getattr(worker_state, key, None)
            if raw is not None:
                if isinstance(raw, bool):
                    return raw
                text = str(raw).strip().lower()
                if text in _TRUTHY_FLAGS:
                    return True
                if text in _FALSY_FLAGS:
                    return False
    for env_key in ("VERTEBRA_SERVER_ACTIVE", "VERTEBRA_RUST_OFFLOAD"):
        raw_env = os.environ.get(env_key, "").strip().lower()
        if raw_env in _TRUTHY_FLAGS:
            return True
    return False


def evaluate_cors_preflight(
    spec: CorsOffloadSpec,
    headers: Sequence[tuple[str | bytes, str | bytes]] | Mapping[str | bytes, str | bytes] | None = None,
    *,
    method: str = "OPTIONS",
    origin: str | None = None,
    access_control_request_method: str | None = None,
    access_control_request_headers: str | None = None,
) -> tuple[int, list[tuple[str, str]]] | None:
    """Evaluate an HTTP CORS preflight request against ``CorsOffloadSpec`` without mutating inputs."""
    if method.upper() != "OPTIONS":
        return None

    header_map = _normalize_headers(headers)
    resolved_origin = origin if origin is not None else header_map.get("origin")
    resolved_acrm = (
        access_control_request_method
        if access_control_request_method is not None
        else header_map.get("access-control-request-method")
    )
    resolved_acrh = (
        access_control_request_headers
        if access_control_request_headers is not None
        else header_map.get("access-control-request-headers")
    )

    if not resolved_origin or not resolved_acrm:
        return None
    if not _origin_allowed(spec, resolved_origin):
        return 403, []
    if "*" not in spec.allow_methods and resolved_acrm.upper() not in {m.upper() for m in spec.allow_methods}:
        return 400, []

    wildcard = "*" in spec.allow_origins
    allow_origin = "*" if (wildcard and not spec.allow_credentials) else resolved_origin
    response_headers: list[tuple[str, str]] = [("access-control-allow-origin", allow_origin)]
    if spec.allow_credentials:
        response_headers.append(("access-control-allow-credentials", "true"))
    methods = (
        resolved_acrm or "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        if "*" in spec.allow_methods
        else ", ".join(spec.allow_methods)
    )
    response_headers.append(("access-control-allow-methods", methods))
    allow_hdrs = resolved_acrh or "*" if "*" in spec.allow_headers else ", ".join(spec.allow_headers)
    if allow_hdrs:
        response_headers.append(("access-control-allow-headers", allow_hdrs))
    response_headers.append(("access-control-max-age", str(spec.max_age)))
    response_headers.append(("vary", "Origin"))
    return 204, response_headers


def evaluate_cors_response_headers(
    spec: CorsOffloadSpec,
    headers: Sequence[tuple[str | bytes, str | bytes]] | Mapping[str | bytes, str | bytes] | None = None,
    *,
    origin: str | None = None,
) -> list[tuple[str, str]]:
    """Generate CORS response headers for a non-preflight request without mutating inputs."""
    resolved_origin = origin if origin is not None else _normalize_headers(headers).get("origin")
    if not resolved_origin or not _origin_allowed(spec, resolved_origin):
        return []
    wildcard = "*" in spec.allow_origins
    allow_origin = "*" if (wildcard and not spec.allow_credentials) else resolved_origin
    response_headers: list[tuple[str, str]] = [("access-control-allow-origin", allow_origin)]
    if spec.allow_credentials:
        response_headers.append(("access-control-allow-credentials", "true"))
    if spec.expose_headers:
        response_headers.append(("access-control-expose-headers", ", ".join(spec.expose_headers)))
    if allow_origin != "*":
        response_headers.append(("vary", "Origin"))
    return response_headers


def evaluate_openapi_cache_request(
    spec: OpenApiOffloadSpec,
    headers: Sequence[tuple[str | bytes, str | bytes]] | Mapping[str | bytes, str | bytes] | None = None,
    *,
    if_none_match: str | None = None,
) -> tuple[int, list[tuple[str, str]], bytes]:
    """Return a 304 Not Modified or 200 OK response for a cached OpenAPI payload."""
    response_headers = [
        ("content-type", spec.content_type),
        ("etag", spec.etag),
        ("cache-control", "public, max-age=0, must-revalidate"),
    ]
    resolved_inm = if_none_match if if_none_match is not None else _normalize_headers(headers).get("if-none-match")
    if resolved_inm is not None and _etag_matches(resolved_inm, spec.etag):
        return 304, response_headers, b""
    return 200, response_headers, spec.body


def _decode_header_part(value: str | bytes) -> str:
    return value.decode("latin-1") if isinstance(value, bytes) else value


def _normalize_headers(
    headers: Sequence[tuple[str | bytes, str | bytes]] | Mapping[str | bytes, str | bytes] | None,
) -> dict[str, str]:
    if headers is None:
        return {}
    items = headers.items() if isinstance(headers, Mapping) else headers
    normalized: dict[str, str] = {}
    for raw_key, raw_val in items:
        key = _decode_header_part(raw_key).strip().lower()
        val = _decode_header_part(raw_val).strip()
        if key:
            normalized[key] = val
    return normalized


def _normalize_etag(tag: str) -> str:
    cleaned = tag.strip()
    if cleaned.startswith(("W/", "w/")):
        cleaned = cleaned[2:].strip()
    if len(cleaned) >= 2 and cleaned[0] == '"' and cleaned[-1] == '"':
        cleaned = cleaned[1:-1]
    return cleaned


def _etag_matches(if_none_match: str, spec_etag: str) -> bool:
    target = _normalize_etag(spec_etag)
    if not target:
        return False
    for candidate in if_none_match.split(","):
        token = candidate.strip()
        if token == "*":
            return True
        if _normalize_etag(token) == target:
            return True
    return False


def _origin_allowed(spec: CorsOffloadSpec, origin: str) -> bool:
    if "*" in spec.allow_origins or origin in spec.allow_origins:
        return True
    return spec.allow_origin_regex is not None and re.fullmatch(spec.allow_origin_regex, origin) is not None
