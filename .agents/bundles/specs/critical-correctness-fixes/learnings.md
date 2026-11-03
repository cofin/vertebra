---
type: Reference
title: "Learnings: Silent Failure and Security Fixes"
description: Execution and review discoveries from critical-correctness-fixes across MCP, stores, embeddings, and event-loop lifecycle.
status: active
---

# Learnings: `critical-correctness-fixes`

## Execution & Architecture Discoveries

- **`GuardProtocol` subclass invocation (`src/py/vertebra/agent/core/guards.py`)**: `GuardProtocol` is a `typing.Protocol` that defines `async def __call__(self, ctx: AgentContext) -> None: ...`. Any guard class that subclasses `GuardProtocol` and implements `async def check(self, context: Any) -> None:` still has `callable(guard) is True` because it inherits the no-op `Protocol.__call__` stub. Both `Tool.invoke` and `GuardChain.execute` must check `check_fn = getattr(guard, "check", None)` before falling back to `guard(ctx)`.
- **`msgspec.Struct` docstring field attributes (`tools.docs.scan_public_docstrings`)**: `scan_undocumented_typeddicts` flags `Attributes:` sections in public `msgspec.Struct` class docstrings (`has_duplicate_class_attributes=True`). Document `msgspec.Struct` fields using inline per-field docstrings immediately below each field definition.
- **`ConfirmationGate` state mutation ordering (`src/py/vertebra/agent/runtime.py`)**: `ConfirmationGate.approve` and `reject` must validate ticket existence and `owner_id` before mutating `_approved_tokens`; mutating `_approved_tokens` first allowed a failed approval by another owner (or for an unknown token) to authorize the token.
- **Buffered ASGI mount and `async_native` batch dispatch (`src/py/vertebra/server/_dispatch.py`, `src/py/vertebra/server/worker.py`)**: `_AsgiMountRunner.__call__` and `dispatch_batch`'s `async_mode=False` `async_native` branch are buffered-only paths (production workers use `async_mode=True` and `spawn_batch`). Both resolve their timeout from `VERTEBRA_ASGI_BATCH_TIMEOUT` (default `30.0` s, `0`/`none` disables), propagate future cancellation to the shard asyncio task via `EventLoopPool.schedule_batch`, and return `504 Gateway Timeout` on expiry.

## Review Adjudication Summary (`0d178f2..633ef06`)

- **`quality-reviewer` (`quality-review-v1`)**: `0` findings (`findings: []`).
- **`code-reviewer`**: `0` Critical, `0` Important, `2` Minor findings (`CR-01` and `CR-02`), both adopted:
  - `CR-01` (`Minor`, `ADOPT`): Synchronized `spec.md` §4 task commit annotations (`[79166fc]`, `[670fd6a]`, `[1a339be]`), §5 continuity snapshot (`plan_commit: 1a339be`, `Last verified checkpoint: 79166fc`), and cleared `claimed_by: null` / `claimed_at: null` across `tasks/t01..t08`.
  - `CR-02` (`Minor`, `ADOPT`, commit `79166fc`): Moved `__all__` to the top of `src/py/vertebra/agent/mcp/service.py`, ordered public methods (`handle_tool_call`, `notify_resource_updated`, `handle_payload`) above private `_...` methods on `MCPService`, and merged `_task_to_dict` into `MCPService` as a `@staticmethod`.
