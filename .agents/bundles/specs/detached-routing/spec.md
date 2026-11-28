---
type: Spec
flow_id: detached-routing
title: "App-independent HTTP route setup"
description: "Make standalone route composition the default, isolate app bindings, and teach the framework through working applications and feature examples."
status: stable
state: completed
has_chapters: false
planning_quality: degraded
planning_degraded_reason: "Flow's Codex model inventory reports no eligible tool-calling model for the strong planning role."
planned_with_model: "active Codex session"
plan_revision: 4
plan_commit: d48d6c86464c83fa98b289216dafe28a190f439e
current_task: null
last_verified_checkpoint: 1b934b31942f637a62633a0b768ea2e1613b488b
last_reviewed_commit: 1b934b31942f637a62633a0b768ea2e1613b488b
generated:
  by: agent:codex
  at: "2026-11-28T00:00:00Z"
verified:
  - by: agent:codex
    at: "2026-11-28T04:09:18Z"
    commit: 1b934b31942f637a62633a0b768ea2e1613b488b
tags: [python, http, routing, dependency-injection, live-state, documentation]
sources:
  - id: litestar-app
    resource: "https://github.com/litestar-org/litestar/blob/v2.25.0/litestar/app.py#L445-L479"
    title: "Litestar application composition"
  - id: litestar-router
    resource: "https://github.com/litestar-org/litestar/blob/v2.25.0/litestar/router.py#L267-L308"
    title: "Litestar router registration and handler copying"
  - id: app
    resource: src/py/vertebra/framework/_app.py
    title: "Vertebra application registration"
  - id: router
    resource: src/py/vertebra/framework/_router.py
    title: "Vertebra route declarations and router composition"
  - id: dispatch
    resource: src/py/vertebra/server/_dispatch.py
    title: "Vertebra startup dispatch compilation"
  - id: litestar-guards
    resource: "https://docs.litestar.dev/2/usage/security/guards.html"
    title: "Litestar guards and routing layers"
  - id: guards
    resource: src/py/vertebra/framework/security.py
    title: "Native guards and principal context"
  - id: live-server
    resource: src/py/vertebra/framework/live.py
    title: "Live state, actions, and room publishing"
  - id: live-client
    resource: src/ts/vertebra-client/src/live/index.ts
    title: "Live state client subscriptions and delta application"
  - id: docs-tests
    resource: src/py/tests/unit/docs/test_examples.py
    title: "Current documentation example validation"
  - id: wasm-client
    resource: src/ts/vertebra-client/src/wasm.ts
    title: "Vertebra browser WebAssembly client"
  - id: scaffold
    resource: src/py/vertebra/cli/init.py
    title: "Vertebra starter application"
---

# App-independent HTTP route setup

<!-- truth: start -->
- Vertebra already exports standalone HTTP decorators and accepts their functions, `Route` values, routers, controllers, and procedures through `App(route_handlers=[...])`. [^app] [^router]
- The CLI starter uses `@app.get`. `App.add_route` can also write a bound route and DI container onto the original function, so a second app can inherit the first app's binding. [^scaffold] [^app] [^dispatch]
- Litestar defines handlers before app construction and assembles them explicitly through `route_handlers`. Its registration path copies handler declarations for mounted routes. [^litestar-app] [^litestar-router]
- Vertebra uses `Controller.path`, `Router(prefix=...)`, and layered `guards` on the app, routers, controllers, and handlers. Role and scope guards resolve a principal through a request or DI. [^router] [^guards] [^dispatch]
- `LiveStateRoom` publishes encoded deltas through an optional application callback. `LiveStateClient` consumes decoded messages and notifies subscribers through an application-provided transport. [^live-server] [^live-client]
- The docs suite requires `main()` on every example module and runs it, including examples suited to normal `module:app` server startup. [^docs-tests]
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

A domain module defines handlers, controllers, routers, and guards. The composition root imports them and builds an `App`. App factories and tests can assemble those declarations with their own dependencies and guards.

Readers can start the documented web apps with `vtb run`, interact over HTTP or a browser, and see the expected result. The examples use the same application code shape users can adopt in their projects.

### 1.2 Upstream Execution Baseline

Litestar 2.25.0's standalone decorators produce route handler objects. `Litestar` registers the configured handlers after applying app configuration. A `Router` groups handlers under a path and copies registered declarations for binding. Vertebra already exposes the same assembly shape while returning the original Python callable from standalone decorators. [^litestar-app] [^litestar-router] [^router]

A local worker compiler probe registers the same decorated `GET /` function into apps configured with marker values `one`, `two`, and a handler default of `default`. Its invokers currently return `one`, `two`, and `one`. The app unit suite passes before the change, so the probe identifies a missing regression case. [^app] [^dispatch]

### 1.3 Code Analysis & Interface Summary

| Area | Current seam | In-process consumers |
| --- | --- | --- |
| Declaration | `framework/_router.py` standalone decorators record `Route` values on the callable. `Router.register` applies a prefix and parent context. | Route modules and controller methods |
| Assembly | `framework/_app.py` `App.__init__` and `App.register` feed concrete routes into `App.add_route` and populate the worker handler table. | App factories, plugins, and autowiring |
| Dispatch | `server/_dispatch.py` `compile_handler_table` compiles DI, guards, path bindings, invocation, and status at worker startup. | Native Python worker |
| Introspection | `OpenAPISpecBuilder.from_app` and `RouteCollector.collect` read resolved routes from the app. | OpenAPI and TypeScript generation |
| Guards | `framework/security.py` provides `Guard` and policy helpers. Dispatch runs layers in order with short-circuit denial. | Standalone handlers, controllers, routers, and apps |
| Live state | `framework/live.py` produces deltas. The browser store accepts decoded messages from the transport. | Application transport and subscribed UI |
| First run | `cli/init.py` emits one starter app for both templates. | `vtb init`, `vtb dev app:app`, and `vtb run app:app` |
| Examples | The docs harness enforces `main()` and executes each module through it. | Snippet includes and CI |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Python framework and routing](../../knowledge/domains/framework/routing-and-app.md)
- [Framework subdomains and dependency injection](../../knowledge/domains/framework/subdomains.md)
- [Startup and request dispatch](../../knowledge/patterns/python/hot-path-and-async-dispatch.md)
- [Litestar integration boundary](../../knowledge/domains/extensions/litestar.md)
- [Live state synchronization](../../knowledge/domains/framework/live-state.md)
- [Fast Chapter Gate](../../knowledge/workflow.md)

## 2. Requirements, Concrete Contracts & Architecture

### R1. Standalone declarations and explicit assembly

`get`, `post`, `put`, `patch`, and `delete` keep their current parameters and return the original callable with its local `Route` declarations. This supports normal direct calls and stacked decorators. Use the existing top-level exports in examples.

```python
from vertebra import App, Router, get


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def create_app() -> App:
    api = Router(prefix="/api", route_handlers=[health])
    return App(route_handlers=[api])
```

`App(route_handlers=[...])` and explicit `app.register(target)` use the same assembly path. `Router(prefix=..., route_handlers=[...])`, direct `Route(method=..., path=..., handler=...)`, and controller classes or instances keep their existing forms. Existing `@app.get` and other bound HTTP decorators continue to register immediately and return the original callable. Lifecycle hooks, procedure registration, and sub-app mounts keep their existing APIs.

The native composition example defines a `Controller` with its own path and dependency, nests it inside a `Router` with a prefix and a guard, and registers that router plus a public standalone health route through `App(route_handlers=[...])`. Litestar and Vertebra support the same composition roles. Document Vertebra's `Router(prefix=...)` and native guard signatures directly. [^litestar-router] [^litestar-guards] [^router] [^dispatch]

Vertebra accepts `guards` on `App`, each `Router`, `Controller.guards`, and individual route declarations. Custom guards can accept a request or use a zero-argument callable. Built-in role and scope guards evaluate a `SecurityContext`. A guard returning `False` or raising `PermissionDeniedError` stops evaluation before handler invocation and produces the default HTTP 403.

### R2. App-local runtime bindings

- `App.add_route` stores the effective route on an app-owned callable using the existing private wrapper, including for a root route with default options. Apply the existing container-attachment predicate independently. A bare root and an empty provider graph use the compiler's local fallback for inferred `Inject` dependencies. Registration leaves the declared callable free of app-specific state.
- Worker startup reads the app-owned metadata and compiles the original effective route callable directly. Preserve the exact callable supplied by route or controller binding, including any user wrappers and synthetic controller signature. Keep the existing fallback for plain callable tables and integrations.
- Use the effective native route's sync flag for that registration, subject to async dispatch rules and an explicit compiler override. Retain the standalone marker for existing non-native consumers that use it as a callable default.
- Preserve route order, stable method-plus-path IDs, last registration lookup for duplicate method and path, the dev welcome route rules, and metadata consumed by OpenAPI and codegen.
- Dependency precedence is app, outer router, inner router, controller, then route, with inner layers overriding matching provider keys. The merged graph retains its existing name and type lookup rules. Guards run from outermost to innermost. Router/controller tags and route status continue to reach existing consumers.
- Each app uses its own runtime bindings. A caller-supplied stateful controller instance retains its lifetime, as do controller instances already bound into a reused router. App factories that need fresh controller instances construct their routers in the factory or pass controller classes directly. Injected controllers, their request scope, and RPC naming and prefixes retain their current behavior.

`route declaration → Router/Controller context → App.add_route → startup compile_handler_table → existing dispatch`

### R3. Starter and routing examples

`vtb init` emits a module-level `@get("/health")` declaration and `app = App(route_handlers=[health])` for the default and web templates. Retain `app:app` as the generated entry point. The quickstart and routing example publish an importable `app` and show a command plus an HTTP request. The routing guide covers all four guard layers and shows how authentication supplies a trusted principal for built-in role or scope enforcement. Keep `pytest` assertions in the dedicated Testing guide and use real public lookup and typed-path APIs there.

### R4. Live state and the UI

Teach `LiveState` and `@live_action` for mutations, `LiveStateRoom.broadcast_pending()` and `publish_fn(channel, bytes)` for outbound deltas, and `LiveStateClient.subscribe()` and `applyServerDelta()` for browser updates. An app owns the transport and codec. `LiveStateClient` accepts `connected` and `onSend` options.

Provide a runnable browser counter with native HTTP routes and server-sent events. One subscription owns one counter. The stream sends an initial snapshot and projects the room's changes into JSON for a plain browser page. The SDK guide shows how a separate transport passes decoded messages to the client, with state IDs and timestamps preserved as `bigint`. Explain the MessagePack room protocol and the distinction between a state update and Inertia's initial page props.

### R5. Application-shaped documentation examples

Rewrite all seven modules published on the examples page. Web examples expose an importable `app` and demonstrate a browser or protocol request. Queue and storage workflows can keep an executable entrypoint when that is how they are used. They show an observable result and the normal resource lifecycle. Application examples avoid `DummyRequest`, direct private dispatch, `run_*_demo` validation functions, and self-testing `assert` statements.

Update the docs test harness to exercise app and script shapes separately. Tests assert outcomes against the published example, while the example performs its application work. Preserve existing feature coverage in the corresponding tests and add checks for gaps revealed by the audit.

### R6. Browser WebAssembly example

Serve an importable native application with `GET /` for the interactive page and `GET /add.wasm` for a small version-1 WebAssembly module. The module exports `add(i32, i32) -> i32` and has no host imports. Keep readable WAT beside the binary representation in the documentation.

Use a same-origin fetch and the browser WebAssembly API to instantiate the module, call its export on two integer inputs, and render the result or a load failure. Serve the module as `application/wasm`. Use native forms and status feedback with accessible labels. The SDK guide shows how `WASMClient.instantiateBytes()` loads the same response. The app serves the page and module. Computation runs in the browser.

### Edge cases and acceptance scenarios

| Scenario | Expected outcome |
| --- | --- |
| The same handler is assembled into apps with different providers and a third app with defaults | Each worker dispatches using its own provider or the handler default. Compiling the apps in any order gives the same results. |
| A bare root infers an `Inject` provider with an empty app graph | The inferred provider stays local to that invoker, and a sibling using a plain annotation still receives its default. |
| One handler is mounted under different prefixes, with different guards or route options | Each request sees the selected registration's prefixes, parameter extraction, guard outcome, status, and sync scheduling. |
| Two decorators for one callable use different sync flags | Only the selected synchronous registration that requests offloading uses the executor. |
| A controller class is composed through nested routers | HTTP dispatch uses the existing controller binding and injection rules. Schema and codegen expose the same public parameter and route metadata. |
| A bound app decorator or direct route is registered | It reaches the same worker dispatch path. Input callable identity and direct invocation are preserved. |
| Two registrations claim the same method and path | The worker handler and handler index select the last registration. The route listing keeps registration order. |
| A guard denies on any routing layer | Guards run in layer order. A denial returns 403 before later guards and the handler execute. |
| The generated starter is imported | Its `app` exposes a working `GET /health` through the normal route table. |
| A browser opens the counter | The event stream sends the initial value and then pushes updates that the page renders. |
| A browser loads the WebAssembly demo and adds two integers | The page shows the result from the compiled module. A fetch or compile failure shows an error. |
| A reader copies an example | The documented command starts the app or executes a genuine workflow. Assertions are in the test suite or Testing guide. |

### Alternatives considered

| Interface | Code and caller impact |
| --- | --- |
| Existing standalone functions and explicit composition | Uses the current public API and repairs the binding seam shared by both declaration styles. Selected. |
| Litestar-style handler objects | Changes what decorators return and requires adapters for controller binding, direct calls, signature inspection, and codegen. |
| App-local side table for dispatch metadata | Requires a new metadata path through worker bootstrap and callable tables. Revisit if the existing wrapper cannot stay off the request path. |

## 3. Documentation & OKF Knowledge Plan

[26/26 files documented]

- [x] Routing and first run update the README, site landing page, quickstart, routing, framework features, frontend, and testing guides, two app examples, package docstring, and routing knowledge.
- [x] Live state updates the browser counter, realtime and TypeScript guides, and live-state knowledge.
- [x] Example cleanup updates the example index, queue, storage, MCP, and vector examples, and the affected storage, server, and embeddings guide sections.

- [x] Browser WebAssembly adds the standalone app, an entry in the examples catalog, SDK usage, and a WASM knowledge note.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Worksheet | Verification |
| --- | --- | --- |
| R1 and R2 | [t01](tasks/t01-isolate-route-bindings.md) | Dispatch cases, route metadata, and compatibility |
| R3 starter | [t02](tasks/t02-update-cli-starter.md) | Scaffold import and dispatch for both templates |
| R3 docs | [t03](tasks/t03-teach-explicit-composition.md) | Importable quickstart and guarded controller app |
| R4 | [t04](tasks/t04-show-live-state-in-the-browser.md) | Native event stream sends a snapshot and pushes updates |
| R5 | [t05](tasks/t05-rewrite-examples-as-applications.md) | Docs tests exercise app entry points and runnable workflows |
| R6 | [t06](tasks/t06-run-webassembly-in-the-browser.md) | Native MIME and browser export invocation |

## 5. Implementation Plan

- [x] [52eae484a96178167b8946b66979d0cc8ae472c8] t01 — [Isolate runtime route bindings](tasks/t01-isolate-route-bindings.md)
- [x] [03254052c80b20abb621167ece42c4e84a931cc4] t02 — [Update the CLI starter](tasks/t02-update-cli-starter.md)
- [x] [2c9779378fad228e079d90453787c7b4684a2661] t03 — [Teach explicit route composition](tasks/t03-teach-explicit-composition.md)
- [x] [a5c80d8cdcc047a41f34cac0a69507e053b9b57e] t04 — [Show live state in the browser](tasks/t04-show-live-state-in-the-browser.md)
- [x] [da8fdf79c1da0b3e5f14421d11e8f677d3b9c061] t05 — [Rewrite examples as applications](tasks/t05-rewrite-examples-as-applications.md)

- [x] [1b934b31942f637a62633a0b768ea2e1613b488b] t06 — [Run WebAssembly in the browser](tasks/t06-run-webassembly-in-the-browser.md)

Run this Fast Chapter Gate once during implementation. Run the broader docs and repository gates at finish.

```bash
uv run --no-sync ruff check \
  src/py/vertebra/framework/_app.py src/py/vertebra/server/_dispatch.py \
  src/py/vertebra/cli/init.py src/py/vertebra/__init__.py docs/examples \
  src/py/tests/unit/framework/test_app.py src/py/tests/unit/server/test_runner.py \
  src/py/tests/unit/cli/test_cli_commands.py src/py/tests/unit/docs/test_examples.py
uv run --no-sync pytest -q \
  src/py/tests/unit/framework/test_app.py src/py/tests/unit/server/test_routing.py \
  src/py/tests/unit/server/test_runner.py src/py/tests/unit/cli/test_cli_commands.py \
  src/py/tests/unit/docs/test_examples.py src/py/tests/unit/framework/test_live_state.py \
  src/py/tests/unit/server/test_asgi_sse.py
```

## 6. Current State & Remaining Plan Snapshot

Standalone routes, layered guard composition, the CLI starter, and the runnable examples are implemented on the working branch. The focused Python and SDK suites, documentation artifact build, and browser smoke pass. Browser smoke covers the Wasm result and failure state and the final live-state count.

[^litestar-app]: [Litestar v2.25.0 application assembly](https://github.com/litestar-org/litestar/blob/v2.25.0/litestar/app.py#L445-L479).
[^litestar-router]: [Litestar v2.25.0 handler registration](https://github.com/litestar-org/litestar/blob/v2.25.0/litestar/router.py#L267-L308).
[^app]: `src/py/vertebra/framework/_app.py` implements app registration and binding.
[^router]: `src/py/vertebra/framework/_router.py` implements standalone decorators and composition.
[^dispatch]: `src/py/vertebra/server/_dispatch.py` compiles worker handlers.
[^scaffold]: `src/py/vertebra/cli/init.py` emits the starter application.
[^litestar-guards]: [Litestar guards](https://docs.litestar.dev/2/usage/security/guards.html) describes layered authorization.
[^guards]: `src/py/vertebra/framework/security.py` defines guard and principal primitives.
[^live-server]: `src/py/vertebra/framework/live.py` produces and publishes live-state frames.
[^live-client]: `src/ts/vertebra-client/src/live/index.ts` applies decoded deltas and notifies browser subscribers.
[^docs-tests]: `src/py/tests/unit/docs/test_examples.py` checks example structure and execution.
[^wasm-client]: `src/ts/vertebra-client/src/wasm.ts` validates and instantiates guest WebAssembly modules in browser and worker runtimes.
