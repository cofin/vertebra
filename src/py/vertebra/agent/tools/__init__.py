"""Typed tool adaptation and caller-bound confirmation for neutral providers."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from copy import deepcopy
from functools import wraps
from typing import Any, get_type_hints

from vertebra.agent.core import AgentContext, PermissionDeniedError, Tool
from vertebra.agent.runtime import ConfirmationGate
from vertebra.typing import ToolPolicyError, ToolProvider

__all__ = ("build_tools",)


def build_tools(provider: ToolProvider, *, confirmation_gate: ConfirmationGate | None = None) -> dict[str, Tool]:
    """Adapt named provider operations into validated tools sharing one confirmation gate.

    Raises:
        ValueError: If an operation declares reserved, positional-only, or variadic parameters.
    """
    gate = confirmation_gate if confirmation_gate is not None else ConfirmationGate()
    return {name: _build_tool(provider, gate, name, operation) for name, operation in provider.operations().items()}


def _build_tool(
    provider: ToolProvider,
    gate: ConfirmationGate,
    name: str,
    operation: Callable[..., Awaitable[Any]],
) -> Tool:
    original = inspect.signature(operation)
    unsupported = [
        key
        for key, param in original.parameters.items()
        if param.kind not in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    ]
    if unsupported:
        raise ValueError(
            f"Provider operation {name!r} requires named parameters; unsupported: {', '.join(unsupported)}"
        )
    hints = get_type_hints(operation, include_extras=True)
    reserved = {"confirmation_token", "context"}.intersection(original.parameters)
    if reserved:
        raise ValueError(f"Provider operation {name!r} declares reserved parameters: {', '.join(sorted(reserved))}")
    signature = original.replace(
        parameters=[
            param.replace(annotation=hints.get(key, param.annotation)) for key, param in original.parameters.items()
        ],
        return_annotation=hints.get("return", original.return_annotation),
    )
    parameters = list(signature.parameters.values())
    controls = [
        inspect.Parameter("confirmation_token", inspect.Parameter.KEYWORD_ONLY, default=None, annotation=str | None),
        inspect.Parameter("context", inspect.Parameter.KEYWORD_ONLY, default=None, annotation=AgentContext),
    ]
    parameters.extend(controls)

    @wraps(operation)
    async def invoke(**arguments: Any) -> Any:
        token = arguments.pop("confirmation_token", None)
        context = arguments.pop("context", None)
        bound = signature.bind(**arguments)
        bound.apply_defaults()
        bound.arguments = deepcopy(bound.arguments)
        canonical = dict(bound.arguments)
        try:
            reason = provider.confirmation_reason(name, canonical)
            if reason is not None:
                owner = context.identity.owner_key() if isinstance(context, AgentContext) else None
                ticket = gate.get_ticket(token) if token is not None else None
                if not (
                    ticket is not None
                    and ticket.status == "approved"
                    and ticket.tool_name == name
                    and ticket.arguments == canonical
                    and (ticket.owner_id is None or ticket.owner_id == owner)
                ):
                    pending = gate.request_confirmation(
                        tool_name=name,
                        call_id=f"call-{name}",
                        arguments=deepcopy(canonical),
                        owner_id=owner,
                        reason=reason,
                    )
                    raise PermissionDeniedError(
                        f"Statement blocked by ConfirmationGate; approval required (ticket={pending.token})"
                    )
            return await operation(*bound.args, **bound.kwargs)
        except ToolPolicyError as exc:
            raise PermissionDeniedError(str(exc)) from exc

    invoke.__dict__["__signature__"] = signature.replace(parameters=parameters)
    return Tool(invoke, name=name)
