"""Litestar ecosystem acceleration bridges (`litestar-vite`, `litestar-mcp`, `litestar-queues`, `litestar-security`)."""

from __future__ import annotations

from typing import Any

from vertebra.framework.a2a import A2AServer
from vertebra.framework.autowire import AutowireConfig, AutowireLoader
from vertebra.framework.email import EmailConfig, EmailService
from vertebra.framework.mcp import McpServer
from vertebra.framework.queues import QueueConfig, QueueService
from vertebra.framework.security import SecurityConfig
from vertebra.framework.ssr import SsrConfig

__all__ = (
    "LitestarEcosystemBridge",
    "attach_litestar_ecosystem_bridges",
)


class LitestarEcosystemBridge:
    """Unified acceleration bridge attaching Vertebra subdomains to a Litestar application."""

    __slots__ = (
        "a2a_server",
        "autowire_loader",
        "email_service",
        "mcp_server",
        "queue_service",
        "security_config",
        "ssr_config",
    )

    def __init__(
        self,
        mcp_server: McpServer | None = None,
        a2a_server: A2AServer | None = None,
        queue_service: QueueService | None = None,
        email_service: EmailService | None = None,
        autowire_loader: AutowireLoader | None = None,
        security_config: SecurityConfig | None = None,
        ssr_config: SsrConfig | None = None,
    ) -> None:
        self.mcp_server = mcp_server
        self.a2a_server = a2a_server
        self.queue_service = queue_service if queue_service is not None else QueueService()
        self.email_service = email_service if email_service is not None else EmailService()
        self.autowire_loader = autowire_loader
        self.security_config = security_config
        self.ssr_config = ssr_config

    @classmethod
    def create(
        cls,
        *,
        mcp_server: McpServer | None = None,
        a2a_server: A2AServer | None = None,
        queue_config: QueueConfig | None = None,
        email_config: EmailConfig | None = None,
        autowire_config: AutowireConfig | None = None,
        security_config: SecurityConfig | None = None,
        ssr_config: SsrConfig | None = None,
    ) -> LitestarEcosystemBridge:
        """Create a configured ``LitestarEcosystemBridge``."""
        loader = AutowireLoader(autowire_config) if autowire_config is not None else None
        if loader is not None and autowire_config is not None and autowire_config.domain_packages:
            loader.scan()
        return cls(
            mcp_server=mcp_server,
            a2a_server=a2a_server,
            queue_service=QueueService(queue_config),
            email_service=EmailService(email_config),
            autowire_loader=loader,
            security_config=security_config,
            ssr_config=ssr_config,
        )


def attach_litestar_ecosystem_bridges(
    app_or_config: Any,
    bridge: LitestarEcosystemBridge,
) -> LitestarEcosystemBridge:
    """Register ``LitestarEcosystemBridge`` services into a Litestar ``AppConfig`` or ``Litestar`` state."""
    state = getattr(app_or_config, "state", None)
    if isinstance(state, dict):
        state["vertebra_ecosystem"] = bridge
        state["queue_service"] = bridge.queue_service
        state["email_service"] = bridge.email_service
        if bridge.mcp_server is not None:
            state["mcp_server"] = bridge.mcp_server
        if bridge.a2a_server is not None:
            state["a2a_server"] = bridge.a2a_server
    return bridge
