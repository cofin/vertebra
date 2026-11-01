"""Middleware and static-files extraction from Django settings into Rust OffloadPlan."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from vertebra.extensions.litestar.offload import (
    CompressionOffloadSpec,
    CorsOffloadSpec,
    OffloadPlan,
    OpenApiOffloadSpec,
    StaticMountOffloadSpec,
)
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "CompressionOffloadSpec",
    "CorsOffloadSpec",
    "OffloadPlan",
    "OpenApiOffloadSpec",
    "StaticMountOffloadSpec",
    "offload_middlewares",
)


def offload_middlewares(
    settings: Any,
    *,
    openapi_schema: dict[str, Any] | None = None,
    openapi_path: str = "/openapi.json",
) -> OffloadPlan:
    """Extract Django middlewares (GZip, CORS, WhiteNoise) and STATIC_ROOT into an ``OffloadPlan``."""
    middlewares = tuple(str(m) for m in (getattr(settings, "MIDDLEWARE", None) or ()))
    compression_spec: CompressionOffloadSpec | None = None
    cors_spec: CorsOffloadSpec | None = None
    static_mounts: list[StaticMountOffloadSpec] = []
    openapi_spec: OpenApiOffloadSpec | None = None

    if any("GZipMiddleware" in m or "WhiteNoiseMiddleware" in m for m in middlewares):
        compression_spec = CompressionOffloadSpec(
            backend="gzip",
            minimum_size=int(getattr(settings, "GZIP_MINIMUM_SIZE", 200)),
            gzip_compress_level=int(getattr(settings, "GZIP_COMPRESS_LEVEL", 6)),
            brotli_quality=4,
            zstd_level=3,
            exclude_patterns=(),
            exclude_opt_key=None,
            excluded_paths=(),
        )

    if any("CorsMiddleware" in m for m in middlewares) or getattr(settings, "CORS_ALLOWED_ORIGINS", None):
        allow_all = bool(getattr(settings, "CORS_ALLOW_ALL_ORIGINS", False))
        raw_origins = getattr(settings, "CORS_ALLOWED_ORIGINS", ()) or ()
        origins = ("*",) if allow_all else tuple(str(o) for o in raw_origins)
        cors_spec = CorsOffloadSpec(
            allow_origins=origins,
            allow_methods=("DELETE", "GET", "OPTIONS", "PATCH", "POST", "PUT"),
            allow_headers=("*",),
            allow_credentials=bool(getattr(settings, "CORS_ALLOW_CREDENTIALS", False)),
            expose_headers=(),
            max_age=int(getattr(settings, "CORS_PREFLIGHT_MAX_AGE", 86400)),
            allow_origin_regex=None,
        )

    static_url = str(getattr(settings, "STATIC_URL", "/static/") or "/static/").rstrip("/") or "/static"
    directories: list[str] = []
    static_root = getattr(settings, "STATIC_ROOT", None)
    if static_root:
        directories.append(str(Path(static_root).resolve()))
    for extra_dir in getattr(settings, "STATICFILES_DIRS", ()) or ():
        resolved = str(Path(extra_dir).resolve())
        if resolved not in directories:
            directories.append(resolved)
    if directories:
        static_mounts.append(
            StaticMountOffloadSpec(
                path_prefix=static_url,
                directories=tuple(directories),
                html_mode=False,
                send_as_attachment=False,
                headers={},
            )
        )

    if openapi_schema is not None:
        schema_bytes = bytes(encode_json_bytes(openapi_schema))
        digest = hashlib.sha256(schema_bytes).hexdigest()[:16]
        openapi_spec = OpenApiOffloadSpec(
            schema_path=openapi_path,
            body=schema_bytes,
            etag=f'"{digest}"',
            content_type="application/json",
        )

    if settings is not None and hasattr(settings, "MIDDLEWARE"):
        retained = [
            m
            for m in middlewares
            if not any(tag in m for tag in ("GZipMiddleware", "CorsMiddleware", "WhiteNoiseMiddleware"))
        ]
        settings.MIDDLEWARE = retained

    return OffloadPlan(
        compression=compression_spec,
        cors=cors_spec,
        static_mounts=tuple(static_mounts),
        openapi=openapi_spec,
    )
