---
type: Guide
title: "Route Codegen Targets & Inertia v3 Middleware"
description: "How vertebra.build.codegen turns App routes into TypeNode IR and TypeScript, Zod, Valibot, OpenAPI, Protobuf, and procedure client output, and how SyncInertiaMiddleware / AsyncInertiaMiddleware build Inertia v3 responses."
status: stable
tags: [architecture, frontend, codegen, typescript, inertia]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-build-codegen-init-py
    resource: src/py/vertebra/build/codegen/__init__.py
    title: "Python implementation module (src/py/vertebra/build/codegen/__init__.py)"
  - id: src-build-codegen-ir-py
    resource: src/py/vertebra/build/codegen/_ir.py
    title: "Python implementation module (src/py/vertebra/build/codegen/_ir.py)"
  - id: src-build-codegen-extractor-py
    resource: src/py/vertebra/build/codegen/_extractor.py
    title: "Python implementation module (src/py/vertebra/build/codegen/_extractor.py)"
  - id: src-build-codegen-typescript-py
    resource: src/py/vertebra/build/codegen/_typescript.py
    title: "Python implementation module (src/py/vertebra/build/codegen/_typescript.py)"
  - id: src-build-codegen-manifest-py
    resource: src/py/vertebra/build/codegen/_manifest.py
    title: "Python implementation module (src/py/vertebra/build/codegen/_manifest.py)"
  - id: src-build-codegen-zod-py
    resource: src/py/vertebra/build/codegen/zod.py
    title: "Python implementation module (src/py/vertebra/build/codegen/zod.py)"
  - id: src-build-codegen-valibot-py
    resource: src/py/vertebra/build/codegen/valibot.py
    title: "Python implementation module (src/py/vertebra/build/codegen/valibot.py)"
  - id: src-build-codegen-openapi-py
    resource: src/py/vertebra/build/codegen/openapi.py
    title: "Python implementation module (src/py/vertebra/build/codegen/openapi.py)"
  - id: src-build-codegen-protobuf-py
    resource: src/py/vertebra/build/codegen/protobuf.py
    title: "Python implementation module (src/py/vertebra/build/codegen/protobuf.py)"
  - id: src-build-codegen-procedures-py
    resource: src/py/vertebra/build/codegen/procedures.py
    title: "Python implementation module (src/py/vertebra/build/codegen/procedures.py)"
  - id: src-build-codegen-plugin-py
    resource: src/py/vertebra/build/codegen/plugin.py
    title: "Python implementation module (src/py/vertebra/build/codegen/plugin.py)"
  - id: src-framework-inertia-init-py
    resource: src/py/vertebra/framework/inertia/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/inertia/__init__.py)"
  - id: src-framework-router-py
    resource: src/py/vertebra/framework/_router.py
    title: "Python implementation module (src/py/vertebra/framework/_router.py)"
  - id: src-tests-unit-codegen-test-codegen-extractor-py
    resource: src/py/tests/unit/codegen/test_codegen_extractor.py
    title: "Python implementation module (src/py/tests/unit/codegen/test_codegen_extractor.py)"
---

# Route Codegen Targets & Inertia v3 Middleware

<!-- truth: start -->
- `TypeExtractor` lowers `App` route signatures and `msgspec.Struct` schemas into a canonical `TypeNode` IR shared across TypeScript, Zod, Valibot, OpenAPI, Protobuf v3, and procedure generators.
- `SyncInertiaMiddleware` and `AsyncInertiaMiddleware` implement Inertia v3 (`X-Inertia`, partial reloads, `303` redirect coercion on `PUT`/`PATCH`/`DELETE`, and `409 X-Inertia-Location` version mismatches).
<!-- truth: end -->

[`vertebra.build.codegen`](../../../../../src/py/vertebra/build/codegen/__init__.py)[^src-build-codegen-init-py] converts handler signatures on a Vertebra `App` into a msgspec-tagged IR, then renders client artifacts. `BuildOrchestrator` runs the TypeScript, Zod, and manifest generators (see [Bundler, HMR & SSR](bundler-hmr-and-ssr.md)); the remaining targets run through `CodegenPlugin.emit_all` or direct calls.

```mermaid
flowchart LR
    App["App.routes (Route)"] --> Collector["RouteCollector (_extractor.py)"]
    Collector --> Extractor["TypeExtractor (_extractor.py)"]
    Extractor --> IR["TypeNode IR + RouteMetadata"]
    IR --> TS["TypeScriptGenerator (types.d.ts)"]
    IR --> Zod["ZodGenerator (schemas.zod.ts)"]
    IR --> Man["ManifestGenerator (manifest.json)"]
    IR --> Plugin["CodegenPlugin.emit_all (Valibot, OpenAPI, Protobuf, procedures)"]
```

## IR and extraction (`_ir.py`, `_extractor.py`)

[`TypeNode`](../../../../../src/py/vertebra/build/codegen/_ir.py)[^src-build-codegen-ir-py] is `msgspec.Struct(frozen=True, tag_field="_type", tag=True)` with subclasses `PrimitiveType`, `LiteralType`, `ArrayType`, `TupleType`, `GenericType` (`Set`, `Record`), `UnionType`, `ObjectType` (`fields`, `name`, `optional_fields`), `NamedTypeRef` (recursive models), `AnnotatedType`, `StreamType`, `RawType` (TypeVars), and `UnknownType`. Each implements `to_typescript()` and `iter_children()`.

[`TypeExtractor.from_hint`](../../../../../src/py/vertebra/build/codegen/_extractor.py)[^src-build-codegen-extractor-py] maps:

- `int`/`float` → `number`, `str` → `string`, `bool` → `boolean`, `None` → `null`, `bytes`/`bytearray`/`memoryview` → `Uint8Array`, `Any`/`object` → `UnknownType`.
- `list`, `set`, `dict`, `tuple` (`tuple[T, ...]` → array), PEP 604 and `Union`, `Literal`, `Annotated`, and sync/async iterators and generators (`StreamType`).
- `msgspec.Struct` (`msgspec.structs.fields`; a default makes the field optional) and dataclasses, cached per class.

`TypedDict`, `NamedTuple`, Pydantic models, and `Enum` fall through to `UnknownType`. `extract(handler)` returns one `TypeNode` per non-`self`/`cls` parameter plus the return type; it does not classify path, query, or body locations.

[`RouteCollector.collect(app)`](../../../../../src/py/vertebra/build/codegen/_extractor.py) extracts `RouteMetadata` per route from any supported framework application (`vertebra.App`, Litestar, FastAPI, Starlette, Flask, and Django `urlpatterns`), filtering out DI-injected parameters (`Inject`, `NamedDependencyMarker`, or parameters matching the route/app `DependencyGraph`). `operation_id` is the lowercased method plus path segments joined by `.`, with `{id}` → `by_id`, `-` → `_`, and `root` for `/` (`GET /users/{id}` → `get.users.by_id`).

## Generators

| Generator | Module | Output |
| --- | --- | --- |
| `TypeScriptGenerator` | `_typescript.py` | `// DO NOT EDIT` header, `export interface <Name>` per named `ObjectType` (`?` for optional fields), and `export namespace Procedures { ... }` nested by `operation_id` segment, each leaf `export type <leaf> = (params: {...}) => Promise<Ret>;`. `format_typescript` trims trailing whitespace. |
| `ZodGenerator` | `zod.py` | `import { z } from "zod";` then `export const <operation_id>Params = z.object({...})` using `z.union`, `z.literal`, and `.optional()`. Also `generate_from_node`, `generate_from_schema` (JSON Schema), `emit_schema`. |
| `ManifestGenerator` | `_manifest.py` | `{"routes": {operation_id: {handlerName, method, paramsSchema, procedureId, responseSchema, route}}, "version": 1}`. |
| `ValibotGenerator` | `valibot.py` | `import * as v from 'valibot';` schemas with the same entry points as `ZodGenerator`. |
| `OpenAPIGenerator` | `openapi.py` | `generate` / `generate_json` producing an `"openapi": "3.1.0"` document. |
| `ProtobufGenerator` / `ProtoParser` | `protobuf.py` | Bidirectional `.proto` ↔ `TypeNode` IR + `RouteMetadata`: `ProtobufGenerator` emits `syntax = "proto3";` messages plus one `service` (`unary`, `server_streaming`, `client_streaming`, `bidi` RPCs and `google.api.http` annotations), while `ProtoParser` parses `.proto` files into `ProtoFileSpec`, `msgspec.Struct` stubs (`generate_python_stubs`), and `RouteMetadata` (`to_routes`). |
| `ProcedureClientGenerator` | `procedures.py` | Interfaces plus an `export interface ProcedureRpcClient` map from the `ProcedureRegistry` filled by `@procedure`. |

`procedures.py` also defines Litestar-style parameter markers (`Parameter`, `Body`, `FromPath`, `FromQuery`, `FromHeader`, `FromCookie`, `JSONBody`, `MsgPackBody`, `MultipartBody`, `URLEncodedBody`, `SkipValidation`, `NamedDependency`), `resolve_parameter_node` (used by the Zod, Valibot, and OpenAPI targets to place parameters), and `json_schema_to_type_node`.

[`CodegenPlugin(config, registry)`](../../../../../src/py/vertebra/build/codegen/plugin.py)[^src-build-codegen-plugin-py] stores `CodegenConfig` under `CODEGEN_STATE_KEY = "codegen"` (`install_state`, Litestar `on_app_init`) and `emit_all(routes, output_dir=None)` writes `schemas.valibot.ts`, `schemas.zod.ts`, `openapi.json`, `service.proto`, and `procedures.client.ts`. `CodegenConfig` defaults: `output_dir=src/generated`, Valibot/Zod/OpenAPI/procedures on, Protobuf off, `openapi_title="Vertebra API"`, `openapi_version="1.0.0"`.

## Watch helpers (`plugin.py`)

`CodegenWatcher(project_dir, callback, poll_interval=0.5)` reruns `callback` on `.py` changes, skipping `.venv`, `node_modules`, `build`, `dist`, `target`, and cache directories. `HmrNotifier(base_url).notify()` POSTs to `/__vertebra/hmr/signal` on the Rust server. `HmrEventRouter.route_event(files)` uses `classify_file` (`python`/`frontend`/`config`/`unknown`) to broadcast `full-reload` or `update` messages to subscribed WebSocket clients.

## Inertia v3 (`framework/inertia/__init__.py`)

[`SyncInertiaMiddleware(config=None, ssr_client=None)` and `AsyncInertiaMiddleware(config=None, ssr_client=None)`](../../../../../src/py/vertebra/framework/inertia/__init__.py)[^src-framework-inertia-init-py] turn an `InertiaResponse(component, props, encrypt_history, clear_history)` (helper `inertia()`) into a pure-Python `vertebra.framework._router.Response`.

- **Version**: `get_version()` returns `InertiaConfig.version`, else the first 8 hex chars of the MD5 of `manifest_path` (`dist/.vite/manifest.json`), else `"dev"`.
- **Conflict**: an `X-Inertia: true` `GET` whose `X-Inertia-Version` differs returns `409` with `X-Inertia-Location: <request.path>`.
- **XHR visit**: `200`, `Content-Type: application/json`, `X-Inertia: true`, `Vary: X-Inertia`, body is the page object.
- **Initial visit**: `200` HTML with `<div id="app">`, optional SSR `head`/`body` from `ssr_client.render(page)` when `ssr_enabled` (CSR fallback on error), and the page in `<script data-page="app" type="application/json">`.
- **Page object**: `component`, `props` (shared data merged first), `url`, `version`, optional `encryptHistory`/`clearHistory`, and metadata `deferredProps`, `mergeProps`, `prependProps`, `deepMergeProps`, `scrollProps`, `onceProps`, `resetProps`.
- **Prop wrappers**: `defer` (`DeferredProp`), `merge` (`MergeProp`, `prepend`), `deep_merge`, `once` (`expires_in`), `scroll`, `optional`. Partial reloads honor `X-Inertia-Partial-Component`, `X-Inertia-Partial-Data`, `X-Inertia-Partial-Except`, `X-Inertia-Reset`, and `X-Inertia-Except-Once-Props`.
- `should_redirect_303(method, status)` reports whether a `301`/`302` after `PUT`/`PATCH`/`DELETE` should become `303`.

`InertiaConfig` fields are listed in [Routing & App](../../domains/framework/routing-and-app.md).

Provenance sources: [^src-build-codegen-typescript-py], [^src-build-codegen-manifest-py], [^src-build-codegen-zod-py], [^src-build-codegen-valibot-py], [^src-build-codegen-openapi-py], [^src-build-codegen-protobuf-py], [^src-build-codegen-procedures-py], [^src-framework-router-py], [^src-tests-unit-codegen-test-codegen-extractor-py].

[^src-build-codegen-init-py]: `src/py/vertebra/build/codegen/__init__.py` — Python implementation module (src/py/vertebra/build/codegen/__init__.py).
[^src-build-codegen-ir-py]: `src/py/vertebra/build/codegen/_ir.py` — Python implementation module (src/py/vertebra/build/codegen/_ir.py).
[^src-build-codegen-extractor-py]: `src/py/vertebra/build/codegen/_extractor.py` — Python implementation module (src/py/vertebra/build/codegen/_extractor.py).
[^src-build-codegen-typescript-py]: `src/py/vertebra/build/codegen/_typescript.py` — Python implementation module (src/py/vertebra/build/codegen/_typescript.py).
[^src-build-codegen-manifest-py]: `src/py/vertebra/build/codegen/_manifest.py` — Python implementation module (src/py/vertebra/build/codegen/_manifest.py).
[^src-build-codegen-zod-py]: `src/py/vertebra/build/codegen/zod.py` — Python implementation module (src/py/vertebra/build/codegen/zod.py).
[^src-build-codegen-valibot-py]: `src/py/vertebra/build/codegen/valibot.py` — Python implementation module (src/py/vertebra/build/codegen/valibot.py).
[^src-build-codegen-openapi-py]: `src/py/vertebra/build/codegen/openapi.py` — Python implementation module (src/py/vertebra/build/codegen/openapi.py).
[^src-build-codegen-protobuf-py]: `src/py/vertebra/build/codegen/protobuf.py` — Python implementation module (src/py/vertebra/build/codegen/protobuf.py).
[^src-build-codegen-procedures-py]: `src/py/vertebra/build/codegen/procedures.py` — Python implementation module (src/py/vertebra/build/codegen/procedures.py).
[^src-build-codegen-plugin-py]: `src/py/vertebra/build/codegen/plugin.py` — Python implementation module (src/py/vertebra/build/codegen/plugin.py).
[^src-framework-inertia-init-py]: `src/py/vertebra/framework/inertia/__init__.py` — Python implementation module (src/py/vertebra/framework/inertia/__init__.py).
[^src-framework-router-py]: `src/py/vertebra/framework/_router.py` — Python implementation module (src/py/vertebra/framework/_router.py).
[^src-tests-unit-codegen-test-codegen-extractor-py]: `src/py/tests/unit/codegen/test_codegen_extractor.py` — Python implementation module (src/py/tests/unit/codegen/test_codegen_extractor.py).
