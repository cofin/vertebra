"""First-class integration for litestar-asyncapi."""

from __future__ import annotations

from typing import Any

from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar_asyncapi import AsyncAPIConfig, AsyncAPIPlugin
    from litestar_asyncapi.config import DocsConfig
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-asyncapi",
        install_package="litestar-asyncapi",
        extra="asyncapi",
    ) from exc


def create_asyncapi_plugin(
    config: AsyncAPIConfig | None = None,
    *,
    title: str = "Vertebra AsyncAPI",
    version: str = "1.0.0",
    path: str = "/asyncapi",
    **kwargs: Any,
) -> AsyncAPIPlugin:
    """Create an AsyncAPIPlugin configured for Vertebra message and channel schemas."""
    ensure_module("litestar_asyncapi", extra="asyncapi")
    if config is not None:
        return AsyncAPIPlugin(config)

    resolved_config = AsyncAPIConfig(
        title=title,
        version=version,
        docs=DocsConfig(path=path),
        **kwargs,
    )
    return AsyncAPIPlugin(resolved_config)
