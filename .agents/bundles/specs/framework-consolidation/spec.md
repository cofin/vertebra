---
flow_id: "framework-consolidation"
title: "Framework Consolidation"
state: closed
plan_revision: 2
plan_commit: ""
created_at: "2026-04-20T04:00:00Z"
updated_at: "2026-04-20T04:00:00Z"
parent_spec: "vertebra-design-quality"
chapter_index: 5
depends_on:
  - "ipc-and-protocol-hotpath"
findings:
  - "FW-07"
  - "FW-08"
  - "FW-13"
  - "FW-14"
  - "FW-15"
  - "FW-16"
  - "FW-17"
  - "FW-18"
  - "FW-19"
  - "FW-21"
  - "FW-25"
  - "FW-26"
  - "FW-27"
  - "FW-28"
  - "FW-29"
  - "FW-30"
  - "FW-31"
  - "FW-32"
  - "FW-33"
  - "FW-34"
  - "FW-35"
  - "FW-36"
  - "FW-37"
  - "FW-38"
  - "FW-41"
  - "FW-48"
  - "FW-49"
  - "FW-50"
  - "FW-53"
  - "MCP-01"
  - "MCP-02"
  - "MCP-03"
  - "MCP-04"
  - "MCP-05"
  - "MCP-06"
  - "MCP-08"
  - "MCP-09"
  - "MCP-10"
tasks:
  - id: "5.1"
    title: "Lazy imports and framework/extension layering"
    state: "closed"
    strategy: "regression_tdd"
    files:
      - "src/py/vertebra/__init__.py"
      - "src/py/vertebra/typing.py"
      - "src/py/vertebra/utils/module_loader.py"
      - "src/py/vertebra/framework/__init__.py"
      - "src/py/vertebra/framework/channels.py"
      - "src/py/vertebra/framework/channels/__init__.py"
      - "src/py/vertebra/framework/stores.py"
      - "src/py/vertebra/framework/stores/__init__.py"
      - "src/py/vertebra/extensions/litestar/__init__.py"
      - "src/py/vertebra/extensions/litestar/channels.py"
      - "src/py/vertebra/extensions/litestar/store.py"
      - "src/py/tests/unit/test_utils_init.py"
    depends_on: []
  - id: "5.2"
    title: "Dead framework and utils code"
    state: "closed"
    strategy: "characterization"
    files:
      - "src/py/vertebra/_typing.py"
      - "src/py/vertebra/utils/loop_policy.py"
      - "src/py/vertebra/utils/singleton.py"
      - "src/py/vertebra/utils/module_loader.py"
      - "src/py/vertebra/utils/portal.py"
      - "src/py/vertebra/utils/sync_tools.py"
      - "src/py/vertebra/utils/config_tools.py"
      - "src/py/vertebra/utils/uuids.py"
      - "src/py/vertebra/utils/type_guards.py"
      - "src/py/vertebra/utils/serializers/__init__.py"
      - "src/py/vertebra/utils/serializers/_schema.py"
      - "src/py/vertebra/utils/serializers/_numpy.py"
      - "src/py/vertebra/extensions/litestar/offload.py"
      - "src/py/tests/unit/framework/test_typing.py"
      - "src/py/tests/unit/utils/test_sync_tools.py"
    depends_on:
      - "5.1"
  - id: "5.3"
    title: "Msgpack codec in serializers"
    state: "closed"
    strategy: "characterization"
    files:
      - "src/py/vertebra/utils/serializers/__init__.py"
      - "src/py/vertebra/utils/serializers/_json.py"
      - "src/py/vertebra/utils/serializers/_msgpack.py"
      - "src/py/vertebra/framework/live/__init__.py"
      - "src/py/vertebra/extensions/alchemy/broker.py"
      - "src/py/vertebra/extensions/sqlspec/storage.py"
      - "src/py/vertebra/extensions/grpc/_decorator.py"
      - "src/py/tests/unit/test_utils_json.py"
    depends_on:
      - "5.2"
  - id: "5.4"
    title: "Native route, state, and field ids"
    state: "closed"
    strategy: "characterization"
    files:
      - "src/rs/vertebra-py/src/lib.rs"
      - "src/rs/vertebra-py/src/transport.rs"
      - "src/py/vertebra/_vertebra.pyi"
      - "src/py/vertebra/__init__.py"
      - "src/py/vertebra/framework/_app.py"
      - "src/py/vertebra/framework/app.py"
      - "src/py/vertebra/framework/_router.py"
      - "src/py/vertebra/framework/router.py"
      - "src/py/vertebra/extensions/litestar/router.py"
      - "src/py/tests/unit/framework/test_app.py"
    depends_on:
      - "5.1"
      - "5.2"
  - id: "5.5"
    title: "Inertia, SSR, and dev packages"
    state: "closed"
    strategy: "characterization"
    files:
      - "src/py/vertebra/framework/_types.py"
      - "src/py/vertebra/framework/_type_bridge.py"
      - "src/py/vertebra/framework/_dev_middleware.py"
      - "src/py/vertebra/framework/dev.py"
      - "src/py/vertebra/framework/_inertia.py"
      - "src/py/vertebra/framework/inertia/__init__.py"
      - "src/py/vertebra/framework/_ssr/__init__.py"
      - "src/py/vertebra/framework/_ssr/_backends.py"
      - "src/py/vertebra/framework/_ssr/_prerender.py"
      - "src/py/vertebra/framework/ssr/__init__.py"
      - "src/py/tests/unit/framework/test_dev_middleware.py"
      - "src/py/tests/unit/framework/test_inertia_middleware.py"
      - "src/py/tests/unit/framework/test_prerender.py"
      - "src/py/tests/unit/framework/test_ssr.py"
    depends_on:
      - "5.1"
      - "5.2"
      - "5.4"
  - id: "5.6"
    title: "Framework subdomain cores"
    state: "closed"
    strategy: "regression_tdd"
    files:
      - "src/py/vertebra/framework/__init__.py"
      - "src/py/vertebra/framework/di/__init__.py"
      - "src/py/vertebra/framework/di.py"
      - "src/py/vertebra/framework/security/__init__.py"
      - "src/py/vertebra/framework/security.py"
      - "src/py/vertebra/framework/procedures/__init__.py"
      - "src/py/vertebra/framework/procedures.py"
      - "src/py/vertebra/framework/htmx/__init__.py"
      - "src/py/vertebra/framework/htmx.py"
      - "src/py/vertebra/framework/live/__init__.py"
      - "src/py/vertebra/framework/live.py"
      - "src/py/vertebra/framework/openapi/__init__.py"
      - "src/py/vertebra/framework/openapi.py"
      - "src/py/vertebra/framework/asyncapi/__init__.py"
      - "src/py/vertebra/framework/asyncapi.py"
      - "src/py/vertebra/framework/autowire/__init__.py"
      - "src/py/vertebra/framework/autowire.py"
      - "src/py/tests/unit/framework/test_framework_subdomains.py"
      - "src/py/tests/unit/framework/test_live_state.py"
      - "src/ts/vertebra-client/tests/live.test.ts"
    depends_on:
      - "5.1"
      - "5.3"
      - "5.4"
  - id: "5.7"
    title: "Queues and email"
    state: "closed"
    strategy: "behavior_tdd"
    files:
      - "src/py/vertebra/_typing.py"
      - "src/py/vertebra/framework/__init__.py"
      - "src/py/vertebra/framework/queues/__init__.py"
      - "src/py/vertebra/framework/queues.py"
      - "src/py/vertebra/extensions/worker/__init__.py"
      - "src/py/vertebra/extensions/worker/runner.py"
      - "src/py/vertebra/framework/email/__init__.py"
      - "src/py/vertebra/framework/email.py"
      - "src/py/tests/unit/extensions/test_worker_lanes.py"
      - "src/py/tests/unit/framework/test_framework_subdomains.py"
    depends_on:
      - "5.1"
      - "5.2"
      - "5.6"
  - id: "5.8"
    title: "MCP and A2A delegated to litestar-mcp"
    state: "closed"
    strategy: "integration_acceptance"
    files:
      - "pyproject.toml"
      - "src/py/vertebra/framework/mcp/__init__.py"
      - "src/py/vertebra/framework/a2a/__init__.py"
      - "src/py/vertebra/extensions/litestar/__init__.py"
      - "src/py/vertebra/extensions/litestar/ecosystem.py"
      - "src/py/vertebra/extensions/litestar/config.py"
      - "src/py/vertebra/extensions/litestar/plugin.py"
      - "src/py/vertebra/cli/mcp.py"
      - "src/py/vertebra/extensions/apex/openapi.py"
      - "src/py/vertebra/extensions/apex/gateway.py"
      - "src/py/tests/unit/cli/test_mcp_cli.py"
      - "src/py/tests/unit/extensions/test_litestar_plugin.py"
      - "src/py/tests/unit/extensions/test_oracle_apex_and_gateway.py"
      - "src/py/tests/unit/framework/test_framework_subdomains.py"
    depends_on:
      - "5.1"
      - "5.7"
---

# Chapter 5 — Framework Consolidation

## Problem & Goal

`vertebra.framework` and `vertebra.utils` carry inverted dependency edges, eager package-root imports (`~118 ms` warm `import vertebra.framework`), duplicate Inertia/SSR/routing/worker implementations, unreferenced utility modules and typing stubs, ad-hoc `msgspec.msgpack` calls, and a custom MCP/A2A reimplementation that diverges from `litestar-mcp[a2a]`. This chapter consolidates `vertebra.framework` and `vertebra.utils` into lazy, one-way-layered, domain-named modules, unifies route/state/field hashing with `vertebra._vertebra`, implements real in-memory/SMTP and lane-scheduled queue/email services with explicit `UnsupportedBackendError` for unimplemented cloud providers, and delegates all MCP and A2A functionality to `litestar-mcp[a2a]>=0.15`.

## Scope

### In scope
- Lazy `__getattr__`/`__dir__` exports via `lazy_exports` in `vertebra.utils.module_loader` for `vertebra`, `vertebra.typing`, `vertebra.framework`, and `vertebra.extensions.litestar`, plus inverting `ChannelBus` and `ResponseCacheStore` so `vertebra.framework` owns the core primitives and `vertebra.extensions.litestar` depends on `vertebra.framework`.
- Deleting dead modules (`utils/loop_policy.py`, `utils/singleton.py`, `framework/_types.py`, `framework/mcp/`, `framework/a2a/`, `extensions/litestar/ecosystem.py`, `extensions/worker/`) and unreferenced symbols across `_typing.py`, `utils/*`, and `utils/serializers/*`.
- Centralizing MessagePack encoding/decoding (`MsgpackCodec`, `encode_msgpack`, `decode_msgpack`) in `vertebra.utils.serializers` and removing stdlib `json` fallbacks and `global` state in `utils/serializers/_json.py`.
- Exposing `handler_id`, `state_id`, and `field_id` from `vertebra-py` (`_vertebra`), consolidating `App`/`Router`/`LitestarRouterTable` route normalization and parameter converters.
- Merging split `_inertia.py` + `inertia/__init__.py`, `_ssr/*` + `ssr/__init__.py`, and `_type_bridge.py` + `_dev_middleware.py` into single canonical modules (`framework/inertia/__init__.py`, `framework/ssr/__init__.py`, `framework/dev.py`).
- Consolidating `di`, `security`, `procedures`, `htmx`, `live`, `openapi`, `asyncapi`, and `autowire` into flat domain modules with bounded `LiveState` delta queues and functional OpenAPI/AsyncAPI spec builders.
- Merging `extensions/worker` into `framework/queues.py`, implementing real SMTP and in-memory `EmailService` in `framework/email.py`, and raising `UnsupportedBackendError` for unimplemented provider strings (Decision D1).
- Delegating MCP and A2A to `litestar-mcp[a2a]>=0.15` (`vertebra[mcp]` extra) across `LitestarPlugin`, `vertebra.cli.mcp`, and `vertebra.extensions.apex` (Decision D2).

### Out of scope
- Rust CLI `vtb mcp` subcommand removal in `src/rs/vertebra-cli/` (covered by Chapter 3 for the Rust binary side of `MCP-03`).
- `pyproject.toml` Ruff per-file-ignores cleanup (`FW-48` config edits are executed in Task `7.9` after all code fixes in this chapter land).
- Database, storage, SQLAlchemy/SQLSpec/Advanced Alchemy, Wasm, and gRPC consolidation (covered by Chapter 6).

## Target Design

1. **One-way package layering & lazy exports (`5.1`)**: `vertebra.utils.module_loader.lazy_exports` builds cached `__getattr__` and `__dir__` functions that bind resolved attributes directly into `sys.modules[module_name].__dict__`. `vertebra.framework.channels` owns `ChannelBus` and `vertebra.framework.stores` owns `ResponseCacheStore` and `NamespacedStore`; `vertebra.extensions.litestar.channels` and `store` import from `vertebra.framework`.
2. **Zero dead utilities & centralized serializers (`5.2`, `5.3`)**: `_typing.py` retains only active sentinels (`Empty`, `EmptyType`, `UNSET`), protocols, and exceptions (including `UnsupportedBackendError`). `vertebra.utils.serializers` owns both JSON (`JSONSerializer`, `encode_json`, `encode_json_bytes`, `encode_json_into`, `format_json`, `decode_json`) and MessagePack (`MsgpackCodec`, `encode_msgpack`, `decode_msgpack`).
3. **Native FNV-1a hashing & unified routing (`5.4`)**: `vertebra._vertebra` exposes `handler_id(method, path)`, `state_id(name)`, and `field_id(name)` from `vertebra-core`. `framework/router.py` defines `RouteDef`, `ParamSpec`, `PARAM_CONVERTERS`, and `Router`; `framework/app.py` delegates route registration to `self.router._route`; `LitestarRouterTable.match` delegates to `_vertebra.Router`.
4. **Unified Inertia, SSR, and Dev middleware (`5.5`)**: `framework/dev.py` combines `TypeBridge` (callback-driven invalidation) and `DevMiddleware`. `framework/ssr/__init__.py` unifies `SsrConfig(mode="native" | "sidecar")`, `SsrResult`, `SsrClient`, and `render_page_shell` using argument-list `subprocess.Popen(..., start_new_session=True)` without `shell=True` or `preexec_fn`. `framework/inertia/__init__.py` holds `msgspec.Struct(frozen=True, gc=False)` prop types, `InertiaConfig(ssr: SsrConfig | None = None)`, and `InertiaMiddleware` using `hashlib.blake2b(..., digest_size=4)`.
5. **Flat framework subdomains (`5.6`, `5.7`)**: Single-file subpackages (`di`, `security`, `procedures`, `htmx`, `live`, `openapi`, `asyncapi`, `autowire`, `queues`, `email`) are flattened to `src/py/vertebra/framework/<domain>.py`. `LiveState` bounds and coalesces `_pending_deltas` per `field_id`. `QueueService` integrates `WorkerRunner` lane semaphores, retry policies, and cron matching while raising `UnsupportedBackendError` for non-`memory://` URLs. `EmailService` implements `memory` and `smtp` backends and raises `UnsupportedBackendError` for unimplemented third-party API providers.
6. **Delegated MCP & A2A (`5.8`)**: `vertebra[mcp]` installs `litestar>=2.15` and `litestar-mcp[a2a]>=0.15`. `LitestarConfig` and `LitestarPlugin` wire `LitestarMCP` and `LitestarA2A`. `vertebra.cli.mcp.run_mcp_stdio_bridge` delegates to `litestar_mcp.mcp.bridge.run_bridge`. `OrdsOpenApiBridge` emits executable `litestar_mcp.Tool` and `a2a.types.AgentSkill` objects with complete path/query/body `input_schema`.

## Task Index

| Task | Title | Strategy | Depends On | Findings |
|---|---|---|---|---|
| `5.1` | Lazy imports and framework/extension layering | `regression_tdd` | — | `FW-13`, `FW-14`, `FW-49`, `MCP-08` |
| `5.2` | Dead framework and utils code | `characterization` | `5.1` | `FW-21`, `FW-25`, `FW-26`, `FW-28`, `FW-48` |
| `5.3` | Msgpack codec in serializers | `characterization` | `5.2` | `FW-27` |
| `5.4` | Native route, state, and field ids | `characterization` | `5.1`, `5.2` | `FW-18`, `FW-19`, `FW-41` |
| `5.5` | Inertia, SSR, and dev packages | `characterization` | `5.1`, `5.2`, `5.4` | `FW-15`, `FW-16`, `FW-17`, `FW-36`, `FW-37`, `FW-38` |
| `5.6` | Framework subdomain cores | `regression_tdd` | `5.1`, `5.3`, `5.4` | `FW-29`, `FW-30`, `FW-31`, `FW-33`, `FW-34`, `FW-35` |
| `5.7` | Queues and email | `behavior_tdd` | `5.1`, `5.2`, `5.6` | `FW-07`, `FW-08` |
| `5.8` | MCP and A2A delegated to litestar-mcp | `integration_acceptance` | `5.1`, `5.7` | `MCP-01`, `MCP-02`, `MCP-03`, `MCP-04`, `MCP-05`, `MCP-06`, `MCP-09`, `MCP-10`, `FW-32`, `FW-50`, `FW-53` |

## Finding-to-task trace

| Finding | Task(s) | Resolution Summary |
|---|---|---|
| `FW-07` | `5.7` | Merge `extensions/worker` into `framework/queues.py`; wire `TaskRetryPolicy`, `CronSchedule`, `collections.deque`, per-lane semaphores, and raise `UnsupportedBackendError` for non-`memory://` backends |
| `FW-08` | `5.7` | Implement `"memory"` and `"smtp"` (`smtplib.SMTP` via `SyncPool`) in `framework/email.py` and raise `UnsupportedBackendError` for `"resend" \| "sendgrid" \| "mailgun" \| "ses"` |
| `FW-13` | `5.1` | Move `ChannelBus` into `framework/channels.py` and `ResponseCacheStore` into `framework/stores.py`; `extensions/litestar/{channels,store}.py` import from `vertebra.framework` |
| `FW-14` | `5.1` | Replace eager imports in `framework/__init__.py` and `extensions/litestar/__init__.py` with `lazy_exports` |
| `FW-15` | `5.5` | Merge `_inertia.py` + `inertia/__init__.py` and `_ssr/*` + `ssr/__init__.py` into single canonical packages |
| `FW-16` | `5.5` | Unify `SsrClient` protocol (`render`, `invalidate`, `stop`) and `SsrConfig(mode="native" \| "sidecar")`; delete unused `SsrFrame` |
| `FW-17` | `5.5` | Delete `framework/_types.py` (`Request`, `Response`) and use native `_vertebra.Request`/`Response` or ASGI tuples |
| `FW-18` | `5.4` | Expose `handler_id`, `state_id`, and `field_id` from `vertebra-py` (`_vertebra`) and remove Python `_fnv1a_*` duplicates |
| `FW-19` | `5.4` | Move `normalize_path` and `compile_template` onto `RouteDef` as `@staticmethod`s; `App._route` delegates to `self.router._route`; drop `_make_handler_id` export |
| `FW-21` | `5.2`, `5.4`, `5.5` | Delete `MiddlewareOffloadCompiler` (`5.2`), `LitestarRouteCompiler`/`extract_litestar_route_entries` (`5.4`), and the 6 Inertia prop factory wrappers (`5.5`); storage/sqlspec wrappers handled in `6.2`/`6.3` |
| `FW-25` | `5.2` | Delete `UnsetType`, `StructStub`, `AttrsInstanceStub`, `NumpyArrayStub`, `MSGSPEC_INSTALLED`, `ORJSON_INSTALLED`, and `Struct` fallback branch in `_typing.py`; simplify `type_guards.py` |
| `FW-26` | `5.2` | Delete `utils/loop_policy.py` and `utils/singleton.py`; remove unreferenced helpers across `module_loader.py`, `portal.py`, `sync_tools.py`, `config_tools.py`, `uuids.py`, and `utils/serializers/{_schema,_numpy}.py` |
| `FW-27` | `5.3` | Remove stdlib `json` fallback and `global` statements in `utils/serializers/_json.py`; add `MsgpackCodec`, `encode_msgpack`, `decode_msgpack` in `utils/serializers/_msgpack.py` and update call sites |
| `FW-28` | `5.2`, `5.5` | Replace private `vertebra.utils.serializers._json` imports with `vertebra.utils.serializers` in `extensions/litestar/offload.py` (`5.2`) and `framework/{dev,inertia,ssr}` (`5.5`) |
| `FW-29` | `5.6` | Add `OpenAPISpecBuilder.from_app(app)`, wire `App.enable_docs()` route, and use `vertebra.utils.serializers` in `AsyncAPISpecBuilder` |
| `FW-30` | `5.6` | Precompute `Provide.parameter_names`, return `MappingProxyType` on `DependencyGraph.providers`, and iterate `_order` in `Container.resolve` |
| `FW-31` | `5.6` | Consolidate `requires_*` into `Guard.requires(...)` and implement `CsrfMiddleware` and `RateLimiter` enforcement |
| `FW-32` | `5.8` | Superseded by Decision D2: delete `framework/mcp/__init__.py` and `framework/a2a/__init__.py` rather than retaining a custom JSON-RPC dispatcher |
| `FW-33` | `5.6` | Replace duplicate `procedure_query/mutation/subscription` factories with `ProcedureDef.decorator(kind)` and `ProcedureRouter.query/mutation/subscription` |
| `FW-34` | `5.6` | Allow `HtmxRequest.from_headers` to scan raw header pairs without dict copy and share `_tristate` helper on `HtmxResponse` |
| `FW-35` | `5.6` | Use `_vertebra.state_id`/`field_id`, cache field IDs in `LiveState.__init_subclass__`, bound and coalesce `_pending_deltas` per `field_id`, and share cross-language frame fixture with TS tests |
| `FW-36` | `5.5` | Merge `_type_bridge.py` + `_dev_middleware.py` into `framework/dev.py` with callback-based `TypeBridge(on_invalidate=...)`, `_ASSET_ROUTES` dict, and debug logging |
| `FW-37` | `5.5` | Replace `shell=True` and `preexec_fn=os.setsid` with argument-list `subprocess.Popen(..., start_new_session=True)` and add `render_async` via `SyncPool` |
| `FW-38` | `5.5` | Convert Inertia prop classes to `msgspec.Struct(frozen=True, gc=False)`, deduplicate partial-reload filtering and CSV parsing, and use `hashlib.blake2b(digest_size=4)` |
| `FW-41` | `5.4` | Share `PARAM_CONVERTERS` from `framework/router.py` and delegate `LitestarRouterTable.match` to native `_vertebra.Router` |
| `FW-48` | `5.2`, `5.5` | Fix underlying `RUF022`, `BLE001`, `PLW0603`, `S110`, `S324`, `S602`, `PLW1509` violations in framework/utils files so Task `7.9` can drop `per-file-ignores` |
| `FW-49` | `5.1` | Add `__dir__` and `globals()[name]` caching to `vertebra/__init__.py` and `vertebra/typing.py` via `lazy_exports` |
| `FW-50` | `5.8` | Delete `extensions/litestar/ecosystem.py` and wire `LitestarMCP` and `LitestarA2A` directly in `LitestarConfig` and `LitestarPlugin.on_app_init` |
| `FW-53` | `5.8` | Populate `OrdsOpenApiBridge` `input_schema` from OpenAPI `parameters[]` (path/query) plus `requestBody` schema |
| `MCP-01` | `5.8` | Delete `src/py/vertebra/framework/mcp/__init__.py` and delegate MCP server handling to `litestar_mcp` |
| `MCP-02` | `5.8` | Replace custom `urllib` loop in `src/py/vertebra/cli/mcp.py` with `litestar_mcp.mcp.bridge.run_bridge` |
| `MCP-03` | `5.8` | Delegate Python `vertebra.cli.mcp` stdio bridge to `litestar_mcp` (Rust `vtb mcp` binary removal handled in Chapter 3) |
| `MCP-04` | `5.8` | Delete `src/py/vertebra/framework/a2a/__init__.py` and delegate A2A support to `litestar_mcp[a2a]` |
| `MCP-05` | `5.8` | Delete `LitestarEcosystemBridge` (`extensions/litestar/ecosystem.py`) and register `LitestarMCP`/`LitestarA2A` in `LitestarPlugin` |
| `MCP-06` | `5.8` | Remove `McpServer`/`A2AServer` route-stub generators by deleting `framework/mcp` and `framework/a2a` and mounting `litestar_mcp` ASGI apps |
| `MCP-08` | `5.1` | Replace eager imports in `src/py/vertebra/framework/__init__.py` with `lazy_exports` and remove `mcp` and `a2a` from `__all__` |
| `MCP-09` | `5.8` | Refactor `OrdsOpenApiBridge` and `ApexGateway` in `extensions/apex/{openapi,gateway}.py` to emit executable `litestar_mcp.Tool` and `a2a.types.AgentSkill` definitions |
| `MCP-10` | `5.8` | Replace unit tests for deleted `McpServer`/`A2AServer` with integration tests exercising `litestar_mcp` plugins, bridge delegation, and ORDS tool execution |

## Verification

```bash
uv run pytest src/py/tests/unit/test_utils_init.py src/py/tests/unit/test_utils_json.py src/py/tests/unit/utils/ src/py/tests/unit/framework/ src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_worker_lanes.py src/py/tests/unit/extensions/test_oracle_apex_and_gateway.py src/py/tests/unit/cli/test_mcp_cli.py -q
bun run --cwd src/ts/vertebra-client test tests/live.test.ts
make lint
```
