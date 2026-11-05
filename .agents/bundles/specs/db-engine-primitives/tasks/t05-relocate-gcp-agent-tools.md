---
type: Task
id: db-engine-primitives:t05
title: Implement adapter-owned database ToolProviders and a generic agent tool consumer
description: "Define the neutral callable-map ToolProvider and explicit ToolPolicyError in shared typing; implement BigQueryToolProvider and SpannerToolProvider on their native adapters; replace agent vendor modules and GCP builders with a generic build_tools consumer preserving typed invocation, per-call confirmation, native errors, budgets, and snapshots."
state: "in_progress"
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - db-engine-primitives:t03
  - db-engine-primitives:t04
files:
  - src/py/vertebra/_typing.py
  - src/py/vertebra/typing.py
  - src/py/vertebra/db/adapters/bigquery/tools.py
  - src/py/vertebra/db/adapters/bigquery/driver.py
  - src/py/vertebra/db/adapters/bigquery/__init__.py
  - src/py/vertebra/db/adapters/spanner/tools.py
  - src/py/vertebra/db/adapters/spanner/__init__.py
  - src/py/vertebra/agent/tools/__init__.py
  - src/py/vertebra/agent/tools/bigquery.py
  - src/py/vertebra/agent/tools/spanner.py
  - src/py/vertebra/agent/__init__.py
  - docs/reference/agent.md
  - docs/changelog/index.md
  - .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
tests:
  - src/py/tests/unit/agent/test_core_and_observability.py
  - src/py/tests/unit/agent/test_runtime_and_workflows.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/framework/test_typing.py
  - src/py/tests/unit/docs/test_examples.py
plan_revision: 10
plan_commit: "05bc06a5a53a1225d8c62495ac69162a15d935fc"
state_revision: 2
claimed_by: "/root/provider_t05"
claimed_at: "2026-11-05T04:51:23Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T21:49:00Z
updated_at: "2026-11-05T05:05:25Z"
commit: null
tags: [db, agent, tools, providers, confirmation, layering, clean-break]
---

## Objective

Native BigQuery and Spanner adapters own their tool operations and policies. The agent consumes a neutral provider contract, derives typed schemas from original bound methods, validates calls, and enforces caller-bound confirmation. Remove vendor modules and GCP-specific builders from `vertebra.agent.tools` and its root exports. Keep `vertebra.db.mcp_tools` absent and the DB upward-import allowlist empty.

## Context

- **Verification strategy (`behavior_tdd`)**: Observe failures for missing neutral contracts/providers, the old agent ownership, typed invocation and confirmation binding, native error identity, lazy resolution, and actual BigQuery request billing limits before implementing the changed behavior. Existing passing relocation checks do not establish provider acceptance.
- **Local checkpoint**: `adeade96558de5a48bed1e889a45f92f9ef45ee7` preserves the earlier relocation. It is a source checkpoint, not acceptance of this revised ownership. The proposed source anchor is `05bc06a5a53a1225d8c62495ac69162a15d935fc`; root verifies/stamps the actual execution baseline after independent local work is reconciled. The obsolete `db/mcp_tools.py` is already absent and is not a source edit target.
- **Ownership**: `_typing.py` defines the two small neutral types and `typing.py` exports them. Native adapter `tools.py` modules own concrete operation classes, native configuration/driver resolution, SQL policy classification, engine execution, byte limits, and snapshot staleness. `agent/tools/__init__.py` owns only generic callable adaptation, schema/signature preservation, confirmation, and translation of the explicit neutral policy exception. t07 exclusively edits `test_native_db_drivers.py` and RLS/SQLSpec/PostgreSQL/Oracle sources; t05 runs that existing test module without editing it. Root applies knowledge changes from worker cache diffs.
- **Dependency direction**: Providers import `ToolProvider` and `ToolPolicyError` through `vertebra.typing`, generic DB contracts/registry, and their owning adapter. They import no `vertebra.agent`, framework, server, extension, or other engine adapter. `agent.tools` imports no engine adapter and uses no vendor names, provider registry, or discovery switch.
- **Bounded work**: Add no descriptor DTO, `ToolSpec`, provider registry, automatic config-to-provider factory, parser framework, native result/wire API, or MCP-core rewrite. The future `mcp-webmcp-core` flow consumes the same neutral seam and owns transport/schema consolidation.

## Public Contracts

```python
@runtime_checkable
class ToolProvider(Protocol):
    def operations(self) -> Mapping[str, Callable[..., Awaitable[Any]]]: ...

    def confirmation_reason(
        self,
        operation_name: str,
        arguments: Mapping[str, Any],
    ) -> str | None: ...


class ToolPolicyError(VertebraError, PermissionError):
    """An adapter tool operation is denied by its explicit execution policy."""


class BigQueryToolProvider:
    def __init__(
        self,
        driver: AsyncBigQueryDriver | SyncBigQueryDriver | None = None,
        *,
        config: BigQueryConfig | str | None = None,
        registry: DatabaseRegistry | None = None,
        maximum_bytes_billed: int = 10 * 1024 * 1024 * 1024,
        enforce_dry_run_limit: bool = True,
    ) -> None: ...


class SpannerToolProvider:
    def __init__(
        self,
        driver: AsyncSpannerDriver | SyncSpannerDriver | None = None,
        *,
        config: SpannerConfig | str | None = None,
        registry: DatabaseRegistry | None = None,
        read_only_staleness_seconds: float | None = None,
    ) -> None: ...


def build_tools(
    provider: ToolProvider,
    *,
    confirmation_gate: ConfirmationGate | None = None,
) -> dict[str, Tool]: ...
```

Providers return maps of existing public operation names to their original typed async bound methods. Those methods accept the existing engine parameters except `confirmation_token`, which belongs to the generic bridge: BigQuery exposes `bigquery_dry_run(sql, params=None)`, `bigquery_query(sql, params=None, max_bytes_billed=None)`, and `bigquery_object_table_inspect(table_name, connection_id="us.gcs-conn", object_uri_prefix=None, limit=50)`; Spanner exposes `spanner_query(sql, params=None, staleness_seconds=None)` and `spanner_schema_inspect(table_name=None)`. Retain their established dictionary output shapes, parameter types, descriptions, SQL identifier checks, and sync/async native dispatch. `confirmation_reason` is side-effect free: it resolves no driver, executes no SQL, and returns a reason or raises only an explicit policy denial.

The bridge reserves `confirmation_token` and `context`; provider operations declaring those names fail registration with `ValueError`. It adds optional `confirmation_token: str | None = None` to each exposed input schema and injects context through the existing `Tool.invoke` mechanism. The injected signature parameter has the exact `AgentContext` annotation with default `None`; `AgentContext | None` is not used because the current schema stripper checks the annotation's type name. Providers and ticket arguments receive neither control value. Resolve signatures and type hints once at registration.

```python
from vertebra.agent import ConfirmationGate, build_tools
from vertebra.db import DatabaseRegistry
from vertebra.db.adapters.bigquery import BigQueryConfig, BigQueryToolProvider
from vertebra.db.adapters.spanner import SpannerConfig, SpannerToolProvider

gate = ConfirmationGate()
registry = DatabaseRegistry()
registry.register("analytics", BigQueryConfig(project_id="my-project", dataset_id="analytics"))
registry.register(
    "orders",
    SpannerConfig(project_id="my-project", instance_id="app", database_id="orders"),
)
bq_tools = build_tools(
    BigQueryToolProvider(config="analytics", registry=registry, maximum_bytes_billed=1_000_000),
    confirmation_gate=gate,
)
spanner_tools = build_tools(
    SpannerToolProvider(config="orders", registry=registry, read_only_staleness_seconds=2.5),
    confirmation_gate=gate,
)
rows = await bq_tools["bigquery_query"].invoke({"sql": "SELECT @id AS id", "params": {"id": 1}})
```

Applications combine or register these ordinary `Tool` dictionaries using existing agent/MCP APIs. Constructing providers and calling `build_tools` opens no connection and requires no unrelated default engine registration.

## Steps

1. **Write failing neutral-provider, invocation, policy, and native-request tests (RED)**:
   - Extend existing canonical files only; add no standalone test module.
   - In `test_core_and_observability.py` and `test_runtime_and_workflows.py`, use a small engine-free provider with typed bound methods. Through public `Tool.invoke`, assert required fields, concrete JSON property schemas, defaults, nested parameter conversion, invalid-type/missing-argument `ToolArgumentError` pointers, and rejection before policy or handler side effects. Verify `from __future__ import annotations` method signatures resolve correctly and the wrapper retains the original method schema plus only the optional confirmation token; context and `self` are absent from that schema.
   - In the same canonical agent tests, assert registration rejects reserved-name collisions. Exercise reads without confirmation and writes with missing, unknown, pending, rejected, wrong-tool, wrong-arguments, and wrong-owner tokens. A token approved for one SQL/parameter set cannot authorize another. Calls with omitted defaults and explicit equivalent defaults have the same canonical ticket arguments. The token and injected context are excluded from ticket arguments. Mutating the caller's nested parameter dictionary after a ticket is issued does not change the ticket or authorize changed binds. Context identity comes only from the actual `AgentContext`; a caller-supplied `context` argument cannot override it. A matching approved ticket executes once per invocation; no ticket-consumption or one-shot-token feature is added.
   - In `test_gcp_adapters_and_sqlalchemy.py`, replace old builder/policy imports with adapter providers plus generic `build_tools`. Keep existing operation output and SQL/state assertions, and call `Tool.invoke` instead of bypassing validation through `.fn`. Cover sync and async native drivers, `BaseDatabaseConfig` versus registry alias resolution only at invocation, and construction of one engine's tools without another engine's default config. Assert provider construction, operations discovery, schema registration, invalid typed arguments, and denied confirmation perform no registry resolution, connection creation, or RPC; the first authorized execution resolves only its own driver.
   - Preserve simple SELECT and schema inspection reads, mutation confirmation for DML/DDL/privilege/procedure operations, identifier validation, strict single-statement denial, bounded object-table inspection, and Spanner strong/explicit/default-staleness snapshot behavior over only `information_schema.tables` and `information_schema.columns`. Require confirmation conservatively for `WITH` and other unclassified executable SQL, including CTE-wrapped writes; this classification controls approval separately from execution routing, so an approved read CTE retains its row-returning native path. Do not add execution support beyond current native behavior or require optional SQLSpec/parsers in native providers.
   - In `test_bigquery.py`, extend the existing `_MockBigQueryRestServer` request assertions and native sync/async driver tests: explicit `maximum_bytes_billed` on `select` reaches the actual query request; omission preserves configured behavior. Through provider execution, assert query and approved DML carry `min(provider cap, per-call cap if present, native configured cap if present)` in the REST request. A higher caller cap never raises the policy/config cap; non-positive effective caps are denied before query execution; the existing dry-run over-budget refusal remains. Exercise enforcement with the dry-run guard disabled to prove the native request limit remains active. Use actual native client/REST execution rather than only recorded Python kwargs.
   - In agent/GCP canonical tests, provider-raised `ToolPolicyError` becomes `PermissionDeniedError`; native `DatabaseError`, ordinary native/OS `PermissionError`, cancellation, and other driver failures preserve the exact original exception instance. No blanket `except PermissionError` or broad error replacement is accepted.
   - Run `test_native_db_drivers.py` unchanged: its existing empty `_TEMPORARY_UPWARD_IMPORT_ALLOWLIST` and recursive DB upward-import gate include both new provider modules automatically. Place new generic-agent purity checks in the canonical agent/GCP files, proving generic `agent.tools` imports no engine adapters and root exports contain no vendor discriminator. Do not edit the t07-owned native test source. In `test_typing.py`, assert public/internal identity and usable structural conformance for `ToolProvider`, export identity for `ToolPolicyError`, and that the policy exception is both a `VertebraError` and `PermissionError`.
   - Update existing import/export assertions: adapter barrels export their provider classes; `vertebra.agent.tools` and lazy agent root expose `build_tools`/`tools`; old `GCPDatabaseToolPolicy`, `build_bigquery_tools`, `build_spanner_tools`, `register_gcp_database_tools`, and `is_mutating_sql` agent exports are absent. Both old agent vendor modules and `vertebra.db.mcp_tools` raise `ModuleNotFoundError`.
   - Run the complete focused pytest command in Verification and record observed failures attributable to this changed contract before production edits.

2. **Implement neutral contracts and adapter-owned operations (GREEN)**:
   - Define the runtime-checkable `ToolProvider` and narrow `ToolPolicyError(VertebraError, PermissionError)` in `_typing.py` with public facade exports in `typing.py`, following existing layout. Add no other protocol, descriptor, result class, or policy hierarchy.
   - Create `db/adapters/bigquery/tools.py::BigQueryToolProvider` and `db/adapters/spanner/tools.py::SpannerToolProvider`, with constructor-owned policy values and typed public async operation methods described above. Re-export each provider in its owning adapter barrel through existing lazy-export conventions and TYPE_CHECKING parity.
   - Resolve `driver` directly when supplied; otherwise call `provide_driver(config or "default", registry=registry)` inside invocation. Dispatch async native drivers via `isinstance(driver, AsyncDatabaseDriver)` and await their methods; offload synchronous native calls with `asyncio.to_thread`. Provider construction, callable discovery, schema generation, and confirmation classification perform no connection work.
   - Keep SQL validation, mutation/ambiguity classification, DDL versus DML routing, BigQuery Object Table SQL, and Spanner snapshot semantics on their owning adapter. For read-only BigQuery queries, preserve optional dry-run estimates and enforce the effective cap on native execution. For approved DML, forward the same effective cap to existing native `execute`. For ambiguity such as `WITH`, obtain approval without equating ambiguity with DML; preserve the existing native row/query path. Provider hard denials use only `ToolPolicyError`; native errors propagate unchanged.
   - Add optional keyword-only `maximum_bytes_billed: int | None = None` to BigQuery sync and async `select` and forward it to existing `_run_query_sync` / `_run_query_async`. Preserve `schema_type`, parameter packing, row hydration, configured defaults, and native errors. Existing `execute` already forwards this option; use it. Do not mutate a shared driver's config or introduce a native wire/result API.

3. **Implement the generic agent consumer and remove old ownership**:
   - Replace `agent/tools/__init__.py` with generic `build_tools`. Resolve original bound-method annotations once at registration using `typing.get_type_hints(..., include_extras=True)` and retain their names, defaults, descriptions, and return annotation. Build each wrapper's `inspect.Signature` from that resolved original signature, append optional reserved confirmation-token and injected-context parameters, and assign it as `__signature__` before creating `Tool`. Use the exact `AgentContext` annotation with default `None` for the injected parameter so the existing schema stripping and context injection apply; do not annotate it as `AgentContext | None`. Retain the original signature for default canonicalization instead of re-inspecting providers on every call. Do not place a supplied JSON schema around an untyped `**kwargs` callable and bypass `Tool.invoke` conversion; do not modify the global schema pipeline or use `eval` for this bridge.
   - `build_tools` accepts operations with named positional-or-keyword and keyword-only parameters. Reject positional-only, `*args`, and `**kwargs` signatures at registration with `ValueError`, before constructing schemas/tools or resolving drivers. These unsupported forms do not supply the concrete validated named fields and trusted context behavior required by this bridge. Keep the restriction local; do not modify global `Tool.invoke` or add variadic adaptation machinery. Canonical tests cover rejection for each unsupported kind and successful supported keyword-only methods.
   - Let `Tool.invoke` validate all exposed arguments first. Inside the wrapper, remove token/context, bind against the original method signature, apply defaults, and deep-copy the complete materialized BoundArguments into an invocation-owned snapshot. Use that same snapshot for policy, approval matching, and the actual awaited provider call, so caller mutation during an in-flight operation cannot alter approved binds. Pass those arguments to `confirmation_reason`, and use that same canonical representation for invocation and ticket binding. The default materialization is derived from the retained original signature, not a duplicate schema definition.
   - Use the supplied `ConfirmationGate`, or one gate shared across tools from that `build_tools` call. When a reason is present, accept only a real approved ticket whose `tool_name` and token-free canonical `arguments` match and whose owner is absent or matches `context.identity.owner_key()`. Derive the owner from the injected context only. Otherwise issue a pending ticket with a deep copy of these exact canonical arguments, the reason, and `call_id=f"call-{operation_name}"`, preserving existing direct-tool correlation without adding transport APIs. The deep copy isolates nested caller parameters from pending/approved ticket state. Raise agent `PermissionDeniedError` identifying the token. Do not authorize via `is_approved(token)` alone, and do not mark every query `requires_confirmation=True`, which would request approval for ordinary reads. Generic transport behavior remains unchanged; direct tool calls use the explicit optional argument, while future MCP metadata projection stays in `mcp-webmcp-core`.
   - Catch only neutral `ToolPolicyError` from provider classification/invocation and translate it to agent `PermissionDeniedError`. Preserve native/OS `PermissionError`, database exceptions, and cancellation. Do not suppress failures or wrap every provider exception.
   - Delete `agent/tools/bigquery.py` and `agent/tools/spanner.py`; remove old GCP policy/build/registration/SQL-classification exports from the package and `agent/__init__.py`. Add lazy `build_tools` and retain the `tools` submodule on the root with TYPE_CHECKING parity. Keep `db/mcp_tools.py` absent and its allowlist empty. Leave no compatibility aliases or vendor dispatch in the generic consumer.

4. **Synchronize public documentation and run gates**:
   - Update `docs/reference/agent.md` with the explicit provider/import/`build_tools` example and the optional confirmation token's role. Describe conservative approval for ambiguous SQL without claiming new native execution support. Explain the effective BigQuery cap and native request enforcement; retain existing doc size and public-docstring gates.
   - Draft root-owned knowledge diffs in a task-local cache outside the tracked bundle for `knowledge/domains/agent/mcp-and-a2a-protocol-harness.md`: current generic/adapter ownership, shared typing source resources and provenance, lazy construction, validated invocation, token-free argument/owner binding, and provider-owned policies. Remove source resources/links/provenance for deleted agent vendor modules; bounded truth blocks remain at most 40 lines. Preserve unrelated MCP protocol/runtime facts. Root applies reviewed knowledge changes to the live bundle; t05 does not directly write knowledge files.
   - In that cache, draft a bounded adapter-tool-provider subsection and source resources/provenance for the existing `knowledge/domains/data/database-drivers-and-dialers.md`. Describe the neutral downward contract, lazy resolution, native sync/async execution, conservative policy boundary, BigQuery actual request cap, and Spanner snapshot policy. Keep driver/registry/wheel facts accurate and leave the CDC/tenancy chapter to t07; introduce no new knowledge chapter. Root applies the reviewed diff before final gates.
   - Root applies the accompanying active-spec and future-charter amendments through revise/refine. These metadata/document changes authorize no provider registry, MCP-core rewrite, or future layout implementation.

## Verification

- **Initial (RED)**: Run and record the focused command before implementing revision 9. It fails for missing public neutral contracts/provider classes or the new ownership/invocation/billing assertions; prior old-path GREEN evidence does not satisfy this step.
- **Focused (GREEN)**:
  - `uv run pytest src/py/tests/unit/agent/test_core_and_observability.py src/py/tests/unit/agent/test_runtime_and_workflows.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/framework/test_typing.py src/py/tests/unit/docs/test_examples.py` -> exits `0`.
- **Final (GREEN)**:
  - Repeat the focused command after final scoped changes; it exits `0` with actual BigQuery client/REST cap assertions and public Tool.invoke acceptance exercised.
  - `uv run python -m tools.docs.scan_public_docstrings` -> exits `0`.
  - `make lint-all` -> exits `0`.
  - `make py-test` -> exits `0`; report passes, skips, and expected failures separately.
  - `make docs` -> exits `0`.
  - `make review-check` -> exits `0`.

## Acceptance Criteria

- [ ] R5: Shared typing exposes only the minimal callable-map `ToolProvider` and explicit `ToolPolicyError`; concrete providers live on their owning native adapters, satisfy the protocol, preserve named typed methods/output behavior, and resolve native drivers only at invocation.
- [ ] R5: The generic agent consumes arbitrary providers with zero vendor dispatch/imports; public `Tool.invoke` retains resolved typed schemas, validation, defaults, and trusted context injection. Registration/discovery/confirmation classification is pure and lazy, and invalid typed arguments or denied confirmation precede registry resolution and external execution. Old agent GCP exports/vendor modules and `db.mcp_tools` are absent without shims; DB upward imports and allowlist exemptions remain zero.
- [ ] R5: Confirmation is per validated call, binds the real ticket to tool/canonical token-free arguments/caller ownership, denies invalid approvals, and leaves established simple reads ungated. Ambiguous executable SQL including WITH requires approval without changing read/row routing or adding unsupported execution behavior. Only explicit `ToolPolicyError` is remapped; native exceptions retain identity.
- [ ] R5: BigQuery SELECT and approved DML enforce the minimum applicable provider/per-call/native configured billing cap in actual native REST requests, retain optional dry-run guard refusal and query output, and change no native result/wire API. Spanner keeps established DDL/DML, strong/stale snapshot, and schema-inspection behavior across sync/async drivers.
- [ ] R5: All declared canonical tests and final lint/Python/docs/review gates pass; public docs and knowledge match final ownership and behavior. Fresh correctness and quality reviews resolve scoped findings before task completion.

## Notes & Discoveries

<!-- Append timestamped implementation findings here. -->

- [2026-11-05] Public invocation probes reproduce caller-owned nested argument mutation after approval and context forgery through variadic handlers. Deterministic event-barrier regression verifies the invocation snapshot; registration rejects unsupported parameter kinds without changing the core Tool pipeline.

- [2026-11-05] Root owns the public changelog consumer reconciliation for removed GCP builders and the engine-primitives clean break; provider worker source ownership remains unchanged.

- [2026-11-05] Provider focused272 passed1 skipped; all scoped bridge findings resolved and independently reverified. Combined full make py-test2046 passed69 skipped170 xfailed; make lint-all, make docs and make review-check exit0. Documentation/metadata followup31 cases pass. Source is verified; mandatory whole-flow fresh correctness and quality review precede final task closure.
