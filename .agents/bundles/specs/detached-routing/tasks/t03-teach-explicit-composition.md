---
type: Task
id: t03
chapter_id: null
title: "Teach explicit route composition"
description: "Use standalone route declarations across native application examples and record the app assembly contract."
status: stable
state: completed
priority: normal
verification_strategy: "Exercise public routes and layered guards from importable documentation apps"
worksheet_format: compact
depends_on: [t01]
read_set:
  - README.md
  - docs/index.md
  - docs/getting-started/quickstart.md
  - docs/usage/routing-and-handlers.md
  - docs/usage/framework-features.md
  - docs/usage/frontend-and-ssr.md
  - docs/usage/testing.md
  - docs/examples/quickstart_app.py
  - docs/examples/routing_and_dtos.py
  - src/py/vertebra/__init__.py
  - .agents/bundles/knowledge/domains/framework/routing-and-app.md
  - src/py/vertebra/framework/inertia/__init__.py
  - src/py/vertebra/framework/security.py
files:
  - README.md
  - docs/index.md
  - docs/getting-started/quickstart.md
  - docs/usage/routing-and-handlers.md
  - docs/usage/framework-features.md
  - docs/usage/frontend-and-ssr.md
  - docs/usage/testing.md
  - docs/examples/quickstart_app.py
  - docs/examples/routing_and_dtos.py
  - src/py/vertebra/__init__.py
  - .agents/bundles/knowledge/domains/framework/routing-and-app.md
tests: []
estimated_loc: 125
plan_revision: 4
plan_commit: d48d6c86464c83fa98b289216dafe28a190f439e
commit: 2c9779378fad228e079d90453787c7b4684a2661
sources:
  - id: routing-guide
    resource: docs/usage/routing-and-handlers.md
    title: "Public routing and handler guide"
  - id: quickstart
    resource: docs/examples/quickstart_app.py
    title: "Executable quickstart"
tags: [python, routing, documentation]
---

# Teach explicit route composition

## Objective & Scope Boundary

Implement the documentation portion of [R1](../spec.md#r1-standalone-declarations-and-explicit-assembly) and [R3](../spec.md#r3-starter-and-routing-examples).

## Bounded Read Manifest

Read the listed guides and both app examples. Check the guard and principal signatures in `framework/security.py` and the runtime resolution in `server/_dispatch.py`.

## Contracts & Edge Cases

- Use standalone HTTP declarations followed by explicit assembly across the listed first-run and usage examples. Each app example exposes a top-level `app: App` for the documented `vtb run module:app` command. The quickstart uses requests with expected status and response bodies in place of its test section.
- Ship a guarded inventory application in `routing_and_dtos.py`. Define standalone health and `ItemController` independently of the app, compose nested routers under `/api`, show typed path and request bodies, and configure role and scope guards. Provide a local example credential explicitly through the environment and derive the principal by verifying it. Requests with missing, wrong, and valid credentials demonstrate authorization. Describe app, router, controller, and handler guard placement and layer order in the guide.
- Show how to split a domain module from the composition root, build fresh router instances in a factory when needed, and call `App.register` during assembly. Cover immediate app decorators in a short compatibility note.
- Use the actual `Route(method=..., path=..., handler=...)` and `enable_docs(OpenAPIConfig(...))` signatures. Keep `pytest` examples in the Testing guide and use integer typed paths and `app.handler_index` there. Show CSRF through a wired `CsrfMiddleware.verify_request()` check and rate limiting through a configured `RateLimitGuard` when illustrating those policies.
- In the Inertia guide, make `inertia()` a call inside the standalone handler, pass its descriptor to `AsyncInertiaMiddleware.process(request, descriptor)`, and return the result as a native `(status, headers, body)` response. Use resolver callables for `MergeProp` and `ScrollProp` and make the required frontend assets explicit.
- Keep examples complete within the published named snippets, including imports. Show app configuration next to the code that uses it.

## Test Matrix

The docs harness in [t05](t05-rewrite-examples-as-applications.md) imports the quickstart and inventory apps, checks the assembled routes, and dispatches allowed and denied requests through the normal compiler. The route guide and Testing guide examples use public signatures. The app modules expose the names used by their startup commands.

## Verification

Run the chapter gate and confirm the quickstart and guarded inventory requests using the documented CLI commands during finish. Run `make docs` at finish.

## Acceptance Criteria

- Readers can start both apps directly and observe success and denial outcomes without adding test scaffolding.
- The routing guide documents controller, router, and guard behavior and the OKF chapter records the app binding boundary.

[^routing-guide]: `docs/usage/routing-and-handlers.md` introduces the public composition model.
[^quickstart]: `docs/examples/quickstart_app.py` is the runnable example.
