---
type: Task
id: t01
chapter_id: null
title: "Isolate runtime route bindings"
description: "Keep app-specific state on the registered route and compile the effective callable directly."
status: stable
state: completed
priority: high
verification_strategy: "Compiled native dispatch and app metadata regression tests"
worksheet_format: compact
depends_on: []
read_set:
  - src/py/vertebra/framework/_app.py
  - src/py/vertebra/framework/_router.py
  - src/py/vertebra/framework/di.py
  - src/py/vertebra/server/_dispatch.py
  - src/py/vertebra/server/worker.py
  - src/py/tests/unit/framework/test_app.py
  - src/py/tests/unit/server/test_runner.py
  - src/py/tests/unit/server/test_routing.py
files:
  - src/py/vertebra/framework/_app.py
  - src/py/vertebra/server/_dispatch.py
tests:
  - src/py/tests/unit/framework/test_app.py
  - src/py/tests/unit/server/test_runner.py
estimated_loc: 155
plan_revision: 4
plan_commit: d48d6c86464c83fa98b289216dafe28a190f439e
commit: 52eae484a96178167b8946b66979d0cc8ae472c8
sources:
  - id: bindings
    resource: src/py/vertebra/framework/_app.py
    title: "Native app route binding"
  - id: compiler
    resource: src/py/vertebra/server/_dispatch.py
    title: "Worker dispatch compiler"
tags: [python, routing, dependency-injection]
---

# Isolate runtime route bindings

## Objective & Scope Boundary

Implement [R1 and R2](../spec.md#2-requirements-concrete-contracts--architecture) at the existing binding seam. Keep the public decorator return values and registration APIs.

## Bounded Read Manifest

Read `App.add_route`, `App.register`, the private callable wrapper, `Route.with_context`, and `compile_handler_spec`. Inspect worker startup, controller signature binding, and the app registration and executor tests.

## Contracts & Edge Cases

1. In `App.add_route`, store the effective `Route` on a fresh private wrapper for every native registration. Attach its child container using the existing predicate based on route segments, dependencies, guards, status, and providers. A bare root with an empty graph keeps the compiler's route-local `Inject` fallback. Keep the development welcome handler on the existing callable path.
2. In `compile_handler_spec`, distinguish an app-bound wrapper from an undecorated callable or an unattached standalone declaration. In the native binding path use the stored effective callable for type inspection and invocation, retaining its exact controller signature. Resolve native thread offload from that registration's effective `Route`. Preserve explicit compiler options and fallback behavior on other callable tables.
3. Leave the effective route in `app.routes` for OpenAPI and codegen. Preserve existing registration and lookup behavior. Replace the unit assertion on private function identity with route lookup, callable invocation, and public declaration identity assertions.

## Test Matrix

| Input | Observable assertion |
| --- | --- |
| Reuse one standalone function in three apps | Batch dispatch produces the two distinct injected values and the third default. Register the app with a provider before the default app. |
| Bare root with inferred `Inject[T]` and an empty app graph | The root receives the inferred dependency, the app graph stays empty, and a sibling with a plain defaulted annotation receives its default. |
| Shared function registered under multiple prefixes | Typed parameters, guards, response status, and merged dependencies come from the selected route. |
| Stacked HTTP decorators with distinct sync flags | Extend executor coverage to assert which routes submit and which execute inline. |
| App, outer router, inner router, controller, and route guards | On the allowed path all guards execute once in layer order. A denied path returns 403 and stops before later guards and the handler. |
| Controller method and explicit app decorator | Existing schema and codegen assertions pass. Exercise compiled constructor DI and typed path binding through the worker dispatch path. |
| Standalone, direct `Route`, and bound decorator assembly | Handler IDs, lookup order, and direct function calls remain consistent. |

## Verification

Run the exact Fast Chapter Gate in the spec. The app, routing, and runner suites cover the route binding and dispatch cases. Use the normal dispatch compiler and response assertions for the new regression. Use independent app assembly with contrasting providers to expose cross-app contamination.

## Acceptance Criteria

- Every registered native route owns its bindings. Standalone functions remain reusable.
- Runtime invocation uses the original effective callable. Existing startup compilation and non-native callable tables retain their behavior.
- The regression fails on the baseline and passes after the change.

[^bindings]: `src/py/vertebra/framework/_app.py` stores the app's bound handlers.
[^compiler]: `src/py/vertebra/server/_dispatch.py` builds the compiled invoker.
