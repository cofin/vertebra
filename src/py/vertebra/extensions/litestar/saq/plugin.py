"""First-class integration for litestar-saq."""

from __future__ import annotations

from typing import Any

from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar_saq import QueueConfig, SAQConfig, SAQPlugin
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-saq",
        install_package="litestar-saq",
        extra="saq",
    ) from exc


def create_saq_plugin(
    config: SAQConfig | None = None,
    *,
    queue_configs: Any = None,
    redis_url: str | None = None,
    **kwargs: Any,
) -> SAQPlugin:
    """Create an SAQPlugin instance configured for Vertebra applications."""
    ensure_module("litestar_saq", package="litestar-saq is required for litestar saq integration", extra="saq")
    if config is not None:
        return SAQPlugin(config)

    resolved_queues = list(queue_configs or ())
    if not resolved_queues and redis_url:
        resolved_queues = [
            QueueConfig(
                dsn=redis_url,
            )
        ]
    resolved_config = SAQConfig(
        queue_configs=resolved_queues,
        **kwargs,
    )
    return SAQPlugin(resolved_config)
