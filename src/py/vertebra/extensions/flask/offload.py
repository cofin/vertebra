"""Middleware and static-route extraction from Flask applications into Rust specs."""

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
    app: Any,
    *,
    openapi_schema: dict[str, Any] | None = None,
    openapi_path: str = "/openapi.json",
) -> OffloadPlan:
    """Extract Flask static folders, compression, CORS, and OpenAPI specs into Rust descriptors.

    Args:
        app: Flask application instance.
        openapi_schema: Optional OpenAPI schema dictionary to pre-render in Rust.
        openapi_path: URL path for the OpenAPI JSON endpoint.

    Returns:
        An ``OffloadPlan`` describing offloaded capabilities.
    """
    cors_spec: CorsOffloadSpec | None = None
    compression_spec: CompressionOffloadSpec | None = None
    static_mounts: list[StaticMountOffloadSpec] = []
    openapi_spec: OpenApiOffloadSpec | None = None

    static_folder = getattr(app, "static_folder", None)
    static_url_path = getattr(app, "static_url_path", "/static") or "/static"
    if static_folder:
        resolved = str(Path(static_folder).resolve())
        static_mounts.append(
            StaticMountOffloadSpec(
                path_prefix=str(static_url_path).rstrip("/") or "/static",
                directories=(resolved,),
                html_mode=False,
                send_as_attachment=False,
                headers={},
            )
        )

    config = getattr(app, "config", {}) or {}
    if config.get("COMPRESS_REGISTER") or "compress" in getattr(app, "extensions", {}):
        min_size = int(config.get("COMPRESS_MIN_SIZE", 500))
        level = int(config.get("COMPRESS_LEVEL", 6))
        compression_spec = CompressionOffloadSpec(
            backend="gzip",
            minimum_size=min_size,
            gzip_compress_level=level,
            brotli_quality=4,
            zstd_level=3,
            exclude_patterns=(),
            exclude_opt_key=None,
            excluded_paths=(),
        )

    if "cors" in getattr(app, "extensions", {}) or config.get("CORS_ORIGINS"):
        origins_raw = config.get("CORS_ORIGINS", ("*",))
        origins = (origins_raw,) if isinstance(origins_raw, str) else tuple(str(o) for o in origins_raw)
        cors_spec = CorsOffloadSpec(
            allow_origins=origins,
            allow_methods=("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"),
            allow_headers=("*",),
            allow_credentials=bool(config.get("CORS_SUPPORTS_CREDENTIALS", False)),
            expose_headers=(),
            max_age=600,
            allow_origin_regex=None,
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

    return OffloadPlan(
        cors=cors_spec,
        compression=compression_spec,
        static_mounts=tuple(static_mounts),
        openapi=openapi_spec,
    )
