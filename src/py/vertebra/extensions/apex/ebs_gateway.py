"""Gemini Enterprise Agent to Oracle E-Business Suite (EBS) and APEX Gateway."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from vertebra.extensions.apex.session import (
    ApexSessionContext,
    OraclePoolLike,
    OracleSessionManager,
)
from vertebra.framework.a2a import AgentCard, AgentSkill
from vertebra.framework.mcp import McpServer, McpTool

__all__ = (
    "EbsPolicyDecision",
    "EbsPolicyEvaluator",
    "EbsResponsibilityMapping",
    "EbsToolConfirmationGate",
    "GeminiEnterpriseEbsGateway",
)


@dataclass(frozen=True, slots=True)
class EbsResponsibilityMapping:
    """Maps an authenticated federated identity to an Oracle EBS responsibility and MOAC org."""

    principal_email: str
    ebs_user_id: int
    ebs_resp_id: int
    ebs_resp_appl_id: int
    ebs_org_id: int
    max_approval_amount: float = 0.0
    allowed_packages: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EbsPolicyDecision:
    """Outcome of evaluating an EBS agent tool invocation against responsibility policies."""

    allowed: bool
    requires_confirmation: bool
    reason: str


class EbsPolicyEvaluator:
    """Evaluates whether an EBS responsibility permits executing a package or ISG service."""

    def __init__(self, confirmation_threshold: float = 10000.0) -> None:
        self.confirmation_threshold = confirmation_threshold

    def evaluate(
        self,
        mapping: EbsResponsibilityMapping,
        package_or_service: str,
        *,
        is_mutating: bool = False,
        amount: float | None = None,
    ) -> EbsPolicyDecision:
        """Evaluate access and confirmation requirements for a tool call."""
        normalized = package_or_service.upper()
        if mapping.allowed_packages and normalized not in {pkg.upper() for pkg in mapping.allowed_packages}:
            return EbsPolicyDecision(
                allowed=False,
                requires_confirmation=False,
                reason=f"Package '{package_or_service}' is not permitted for resp_id={mapping.ebs_resp_id}",
            )
        if amount is not None and amount > mapping.max_approval_amount > 0:
            return EbsPolicyDecision(
                allowed=False,
                requires_confirmation=False,
                reason=(
                    f"Requested amount {amount:.2f} exceeds responsibility limit {mapping.max_approval_amount:.2f}"
                ),
            )
        needs_confirm = is_mutating or (amount is not None and amount >= self.confirmation_threshold)
        return EbsPolicyDecision(
            allowed=True,
            requires_confirmation=needs_confirm,
            reason="Permitted by EBS responsibility policy",
        )


@dataclass(slots=True)
class EbsToolConfirmationGate:
    """Human-in-the-loop confirmation gate for mutating Oracle EBS agent tool invocations."""

    _approved_tokens: set[str] = field(default_factory=set)

    def approve(self, confirmation_token: str) -> None:
        """Record human-in-the-loop approval for a specific confirmation token."""
        self._approved_tokens.add(confirmation_token)

    def is_approved(self, confirmation_token: str | None) -> bool:
        """Check whether the provided confirmation token has been approved."""
        if not confirmation_token:
            return False
        return confirmation_token in self._approved_tokens


class GeminiEnterpriseEbsGateway:
    """Bridges Gemini Enterprise A2A/MCP agent calls to Oracle EBS ISG REST and PL/SQL APIs."""

    def __init__(
        self,
        agent_name: str,
        endpoint_url: str,
        *,
        policy_evaluator: EbsPolicyEvaluator | None = None,
        confirmation_gate: EbsToolConfirmationGate | None = None,
        isg_transport: Callable[[str, dict[str, Any], dict[str, str]], Awaitable[dict[str, Any]]] | None = None,
    ) -> None:
        self.agent_name = agent_name
        self.endpoint_url = endpoint_url
        self.policy_evaluator = policy_evaluator or EbsPolicyEvaluator()
        self.confirmation_gate = confirmation_gate or EbsToolConfirmationGate()
        self._isg_transport = isg_transport
        self._mcp_server = McpServer(name=agent_name)
        self._skills: list[AgentSkill] = []

    def register_skill(
        self,
        skill: AgentSkill,
        tool_spec: McpTool,
    ) -> None:
        """Register an EBS/APEX capability as both an A2A skill and a Streamable HTTP MCP tool."""
        self._skills.append(skill)
        self._mcp_server.register_tool(tool_spec)

    def build_agent_card(self) -> AgentCard:
        """Build the A2A ``/.well-known/agent-card.json`` descriptor for Gemini Enterprise."""
        return AgentCard(
            name=self.agent_name,
            description="Vertebra Oracle E-Business Suite & APEX Gateway for Gemini Enterprise",
            url=self.endpoint_url,
            skills=tuple(self._skills),
        )

    def build_mcp_tools(self) -> dict[str, McpTool]:
        """Return Streamable HTTP MCP tool definitions for Gemini Enterprise."""
        return self._mcp_server.tools

    async def execute_plsql_package(
        self,
        pool: OraclePoolLike,
        mapping: EbsResponsibilityMapping,
        context: ApexSessionContext,
        package_procedure: str,
        binds: dict[str, Any],
        *,
        is_mutating: bool = False,
        amount: float | None = None,
        confirmation_token: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute an Oracle EBS PL/SQL package procedure inside a scoped APPS/MOAC/APEX session."""
        package_name = package_procedure.split(".", maxsplit=1)[0]
        decision = self.policy_evaluator.evaluate(
            mapping,
            package_name,
            is_mutating=is_mutating,
            amount=amount,
        )
        if not decision.allowed:
            raise PermissionError(decision.reason)
        if decision.requires_confirmation and not self.confirmation_gate.is_approved(confirmation_token):
            return {
                "status": "confirmation_required",
                "procedure": package_procedure,
                "reason": decision.reason,
            }
        enriched_context = ApexSessionContext(
            app_id=context.app_id,
            page_id=context.page_id,
            session_id=context.session_id,
            app_user=context.app_user,
            workspace_id=context.workspace_id,
            ebs_user_id=mapping.ebs_user_id,
            ebs_resp_id=mapping.ebs_resp_id,
            ebs_resp_appl_id=mapping.ebs_resp_appl_id,
            ebs_org_id=mapping.ebs_org_id,
            page_items=context.page_items,
        )
        bind_Clauses = ", ".join(f"{k} => :{k}" for k in binds)
        plsql = f"BEGIN {package_procedure}({bind_Clauses}); END;"
        async with OracleSessionManager.acquire_scoped(
            pool,
            enriched_context,
            trace_id=trace_id,
            module=self.agent_name,
            action=package_procedure,
        ) as conn:
            await conn.execute(plsql, binds)
        return {
            "status": "executed",
            "procedure": package_procedure,
            "ebs_resp_id": mapping.ebs_resp_id,
            "ebs_org_id": mapping.ebs_org_id,
        }

    async def execute_isg_rest(
        self,
        mapping: EbsResponsibilityMapping,
        service_alias: str,
        payload: dict[str, Any],
        *,
        bearer_token: str,
        is_mutating: bool = False,
        amount: float | None = None,
        confirmation_token: str | None = None,
    ) -> dict[str, Any]:
        """Execute an Oracle EBS Integrated SOA Gateway (ISG) REST endpoint with policy checks."""
        decision = self.policy_evaluator.evaluate(
            mapping,
            service_alias,
            is_mutating=is_mutating,
            amount=amount,
        )
        if not decision.allowed:
            raise PermissionError(decision.reason)
        if decision.requires_confirmation and not self.confirmation_gate.is_approved(confirmation_token):
            return {
                "status": "confirmation_required",
                "service": service_alias,
                "reason": decision.reason,
            }
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "X-EBS-Resp-Id": str(mapping.ebs_resp_id),
            "X-EBS-Resp-Appl-Id": str(mapping.ebs_resp_appl_id),
            "X-EBS-Org-Id": str(mapping.ebs_org_id),
        }
        if self._isg_transport is not None:
            return await self._isg_transport(service_alias, payload, headers)
        return {
            "status": "dispatched",
            "service": service_alias,
            "headers": headers,
            "payload": payload,
        }
