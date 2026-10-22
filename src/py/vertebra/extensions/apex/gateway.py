"""Oracle APEX & ORDS MCP tool and A2A skill gateway with ConfirmationGate."""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Awaitable, Callable
from typing import Any

import msgspec

from vertebra._typing import UnsupportedBackendError
from vertebra.extensions.apex.openapi import ApexSkillSpec, ApexToolSpec
from vertebra.extensions.apex.session import (
    ApexSessionContext,
    OraclePoolLike,
    OracleSessionManager,
)
from vertebra.utils.serializers import encode_json

__all__ = (
    "ApexAccessPolicy",
    "ApexAgentCard",
    "ApexGateway",
    "ApexPageToolSpec",
    "ApexPolicyDecision",
    "ApexPolicyEvaluator",
    "ConfirmationGate",
    "OrdsRestTransport",
)

_PLSQL_PROC_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,63}(\.[A-Za-z][A-Za-z0-9_$#]{0,63}){1,2}$")
_BIND_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,63}$")


class ApexAgentCard(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """A2A agent card descriptor for an Oracle APEX and ORDS gateway."""

    name: str
    description: str
    url: str
    version: str = "1.0.0"
    skills: tuple[ApexSkillSpec, ...] = ()


class ApexPageToolSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Describes an Oracle APEX page process or region action exposed as an MCP/A2A tool."""

    app_id: int
    page_id: int
    process_name: str
    package_procedure: str
    description: str = ""
    is_mutating: bool = False
    page_items: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not _PLSQL_PROC_RE.match(self.package_procedure):
            msg = f"Invalid PL/SQL package procedure identifier: {self.package_procedure!r}"
            raise ValueError(msg)

    def to_tool_spec(self) -> ApexToolSpec:
        """Build an ``ApexToolSpec`` for this APEX page process."""
        props = {item: {"type": "string"} for item in self.page_items}
        return ApexToolSpec(
            name=f"apex_p{self.page_id}_{self.process_name.lower()}",
            description=self.description or f"APEX Page {self.page_id} process {self.process_name}",
            input_schema={
                "type": "object",
                "properties": props,
                "required": list(self.page_items),
            },
        )

    def to_skill_spec(self) -> ApexSkillSpec:
        """Build an ``ApexSkillSpec`` for this APEX page process."""
        tool = self.to_tool_spec()
        return ApexSkillSpec(
            id=tool.name,
            name=self.process_name,
            description=tool.description,
            tags=("oracle", "apex", f"app-{self.app_id}", f"page-{self.page_id}"),
        )


class ApexAccessPolicy(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Maps an authenticated principal to Oracle APEX workspace, scheme, and package policies."""

    principal_email: str
    app_id: int
    workspace_id: int | None = None
    authorization_schemes: tuple[str, ...] = ()
    max_mutation_amount: float = 0.0
    allowed_targets: tuple[str, ...] = ()


class ApexPolicyDecision(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Outcome of evaluating an APEX or ORDS tool invocation against ``ApexAccessPolicy``."""

    allowed: bool
    requires_confirmation: bool
    reason: str


class ApexPolicyEvaluator:
    """Evaluates whether an ``ApexAccessPolicy`` permits executing a PL/SQL process or ORDS service."""

    __slots__ = ("confirmation_threshold",)

    def __init__(self, *, confirmation_threshold: float = 10000.0) -> None:
        self.confirmation_threshold = confirmation_threshold

    def evaluate(
        self,
        policy: ApexAccessPolicy,
        target_name: str,
        *,
        is_mutating: bool = False,
        amount: float = 0.0,
        required_scheme: str | None = None,
    ) -> ApexPolicyDecision:
        """Evaluate access policy, authorization scheme, and confirmation thresholds."""
        if required_scheme is not None and required_scheme not in policy.authorization_schemes:
            return ApexPolicyDecision(
                allowed=False,
                requires_confirmation=False,
                reason=f"Missing APEX authorization scheme '{required_scheme}'",
            )
        normalized = target_name.upper()
        allowed_upper = {pkg.upper() for pkg in policy.allowed_targets}
        if not allowed_upper or normalized not in allowed_upper:
            return ApexPolicyDecision(
                allowed=False,
                requires_confirmation=False,
                reason=f"Target '{target_name}' is not permitted for app_id={policy.app_id}",
            )
        if amount > 0.0 and policy.max_mutation_amount > 0.0 and amount > policy.max_mutation_amount:
            return ApexPolicyDecision(
                allowed=False,
                requires_confirmation=False,
                reason=f"Amount {amount} exceeds max_mutation_amount {policy.max_mutation_amount}",
            )
        needs_confirm = is_mutating or (amount >= self.confirmation_threshold)
        return ApexPolicyDecision(
            allowed=True,
            requires_confirmation=needs_confirm,
            reason="Permitted by APEX access policy",
        )


class ConfirmationGate:
    """Human-in-the-loop single-use confirmation gate for mutating Oracle APEX/ORDS tool invocations."""

    __slots__ = ("_approved_digests",)

    def __init__(self) -> None:
        self._approved_digests: dict[str, str] = {}

    @staticmethod
    def _compute_digest(target: str, payload: dict[str, Any]) -> str:
        serialized = encode_json({"target": target.upper(), "payload": payload})
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def approve(
        self,
        confirmation_token: str,
        *,
        target: str = "*",
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Record human-in-the-loop approval for ``confirmation_token`` bound to ``target`` and ``payload``."""
        digest = "*" if target == "*" and payload is None else self._compute_digest(target, payload or {})
        self._approved_digests[confirmation_token] = digest

    def is_approved(
        self,
        confirmation_token: str | None,
        *,
        target: str = "*",
        payload: dict[str, Any] | None = None,
    ) -> bool:
        """Return True if ``confirmation_token`` has been approved for ``target`` and ``payload``."""
        if not confirmation_token or confirmation_token not in self._approved_digests:
            return False
        stored = self._approved_digests[confirmation_token]
        if stored == "*":
            return True
        expected = self._compute_digest(target, payload or {})
        return hmac.compare_digest(stored, expected)

    def consume(
        self,
        confirmation_token: str | None,
        *,
        target: str = "*",
        payload: dict[str, Any] | None = None,
    ) -> bool:
        """Atomically verify and consume a single-use ``confirmation_token``."""
        if not self.is_approved(confirmation_token, target=target, payload=payload):
            return False
        assert confirmation_token is not None
        self._approved_digests.pop(confirmation_token, None)
        return True


type OrdsRestTransport = Callable[
    [str, dict[str, Any], dict[str, str]],
    Awaitable[dict[str, Any]],
]


def _validate_plsql_identifier(procedure_name: str, binds: dict[str, Any]) -> None:
    if not _PLSQL_PROC_RE.match(procedure_name):
        msg = f"Invalid PL/SQL package procedure identifier: {procedure_name!r}"
        raise ValueError(msg)
    for key in binds:
        if not _BIND_NAME_RE.match(key):
            msg = f"Invalid PL/SQL bind variable identifier: {key!r}"
            raise ValueError(msg)


class ApexGateway:
    """Exposes Oracle APEX page processes, regions, and ORDS services as MCP tools and A2A skills."""

    __slots__ = (
        "_ords_transport",
        "_page_tools",
        "_skills",
        "_tools",
        "agent_name",
        "confirmation_gate",
        "endpoint_url",
        "policy_evaluator",
    )

    def __init__(
        self,
        agent_name: str,
        endpoint_url: str,
        *,
        policy_evaluator: ApexPolicyEvaluator | None = None,
        confirmation_gate: ConfirmationGate | None = None,
        ords_transport: OrdsRestTransport | None = None,
    ) -> None:
        self.agent_name = agent_name
        self.endpoint_url = endpoint_url
        self.policy_evaluator = policy_evaluator or ApexPolicyEvaluator()
        self.confirmation_gate = confirmation_gate or ConfirmationGate()
        self._ords_transport = ords_transport
        self._skills: list[ApexSkillSpec] = []
        self._tools: dict[str, ApexToolSpec] = {}
        self._page_tools: dict[str, ApexPageToolSpec] = {}

    def register_skill(self, skill: ApexSkillSpec, tool_spec: ApexToolSpec) -> None:
        """Register an APEX/ORDS capability as both an A2A skill and an MCP tool."""
        self._skills.append(skill)
        self._tools[tool_spec.name] = tool_spec

    def register_page_tool(self, page_tool: ApexPageToolSpec) -> ApexToolSpec:
        """Register an Oracle APEX page process as both an MCP tool and an A2A skill."""
        tool_spec = page_tool.to_tool_spec()
        skill_spec = page_tool.to_skill_spec()
        self._page_tools[tool_spec.name] = page_tool
        self.register_skill(skill_spec, tool_spec)
        return tool_spec

    def build_agent_card(self) -> ApexAgentCard:
        """Publish the A2A v1.0 AgentCard for this Oracle APEX gateway."""
        return ApexAgentCard(
            name=self.agent_name,
            description="Vertebra Oracle APEX & ORDS Agent Gateway",
            url=self.endpoint_url,
            skills=tuple(self._skills),
        )

    def build_mcp_tools(self) -> dict[str, ApexToolSpec]:
        """Return the registered MCP tool specifications."""
        return dict(self._tools)

    async def execute_plsql_process(
        self,
        pool: OraclePoolLike,
        policy: ApexAccessPolicy,
        context: ApexSessionContext,
        package_procedure: str,
        binds: dict[str, Any],
        *,
        is_mutating: bool = False,
        amount: float = 0.0,
        required_scheme: str | None = None,
        confirmation_token: str | None = None,
        trace_id: str | None = None,
        span_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute an Oracle APEX PL/SQL package/process inside a scoped APEX session."""
        _validate_plsql_identifier(package_procedure, binds)
        pkg_name = package_procedure.split(".", 1)[0]
        decision = self.policy_evaluator.evaluate(
            policy,
            pkg_name,
            is_mutating=is_mutating,
            amount=amount,
            required_scheme=required_scheme,
        )
        if not decision.allowed:
            raise PermissionError(decision.reason)
        if decision.requires_confirmation and not self.confirmation_gate.consume(
            confirmation_token,
            target=package_procedure,
            payload=binds,
        ):
            return {
                "status": "confirmation_required",
                "reason": "Human-in-the-loop ConfirmationGate approval required",
                "package_procedure": package_procedure,
            }

        scoped_ctx = msgspec.structs.replace(
            context,
            app_id=policy.app_id,
            workspace_id=policy.workspace_id if policy.workspace_id is not None else context.workspace_id,
        )
        bind_clauses = ", ".join(f"{k} => :{k}" for k in binds)
        plsql = f"BEGIN {package_procedure}({bind_clauses}); END;"
        async with OracleSessionManager.acquire_scoped(
            pool,
            scoped_ctx,
            trace_id=trace_id,
            span_id=span_id,
            module="VertebraAPEX",
            action=package_procedure[:64],
        ) as conn:
            await conn.execute(plsql, binds)
        return {
            "status": "executed",
            "package_procedure": package_procedure,
            "app_id": policy.app_id,
            "workspace_id": scoped_ctx.workspace_id,
        }

    async def execute_ords_rest(
        self,
        policy: ApexAccessPolicy,
        service_alias: str,
        payload: dict[str, Any],
        *,
        bearer_token: str,
        context: ApexSessionContext | None = None,
        is_mutating: bool = False,
        amount: float = 0.0,
        required_scheme: str | None = None,
        confirmation_token: str | None = None,
    ) -> dict[str, Any]:
        """Execute an Oracle ORDS REST endpoint or 26ai Duality View action with policy checks."""
        if self._ords_transport is None:
            msg = "ApexGateway.execute_ords_rest requires an explicit ords_transport callable"
            raise UnsupportedBackendError(msg)
        decision = self.policy_evaluator.evaluate(
            policy,
            service_alias,
            is_mutating=is_mutating,
            amount=amount,
            required_scheme=required_scheme,
        )
        if not decision.allowed:
            raise PermissionError(decision.reason)
        if decision.requires_confirmation and not self.confirmation_gate.consume(
            confirmation_token,
            target=service_alias,
            payload=payload,
        ):
            return {
                "status": "confirmation_required",
                "service_alias": service_alias,
            }
        headers: dict[str, str] = {
            "Authorization": f"Bearer {bearer_token}",
            "X-APEX-App-Id": str(policy.app_id),
        }
        if policy.workspace_id is not None:
            headers["X-APEX-Workspace-Id"] = str(policy.workspace_id)
        if context is not None:
            headers["X-APEX-Session-Id"] = str(context.session_id)
            headers["X-APEX-User"] = context.app_user
        return await self._ords_transport(service_alias, payload, headers)
