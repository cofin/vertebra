---
type: Task
id: t02
chapter_id: null
title: "Update the CLI starter"
description: "Generate the standalone-decorator route setup for both project templates."
status: stable
state: completed
priority: high
verification_strategy: "Import and invoke scaffolded native app for both templates"
worksheet_format: compact
depends_on: [t01]
read_set:
  - src/py/vertebra/cli/init.py
  - src/py/tests/unit/cli/test_cli_commands.py
files:
  - src/py/vertebra/cli/init.py
tests:
  - src/py/tests/unit/cli/test_cli_commands.py
estimated_loc: 35
plan_revision: 4
plan_commit: d48d6c86464c83fa98b289216dafe28a190f439e
commit: 03254052c80b20abb621167ece42c4e84a931cc4
sources:
  - id: scaffold
    resource: src/py/vertebra/cli/init.py
    title: "Starter app generator"
tags: [python, cli, routing]
---

# Update the CLI starter

## Objective & Scope Boundary

Implement the starter contract in [R3](../spec.md#r3-starter-and-routing-examples).

## Bounded Read Manifest

Read the generated `app_content` and the default and web template CLI tests.

## Contracts & Edge Cases

Use the shared starter body. Import `App, get`, declare the health handler at module level as `@get("/health")`, and assemble `app = App(route_handlers=[health])` after the function. Preserve the project config entry point and the web template's frontend entry.

## Test Matrix

Generate each template in a temporary directory. Replace the current source assertion for `@app.get` with assertions for the standalone `@get` and the explicit `App(route_handlers=[health])` assembly, then load the generated app module. Confirm that `app` publishes the registered health route and that its compiled invoker returns the expected payload. Keep the existing web asset and configuration checks.

## Verification

Run `uv run --no-sync pytest -q src/py/tests/unit/cli/test_cli_commands.py -k vtb_init` in the chapter gate.

## Acceptance Criteria

- Both starters declare the route with the standalone decorator and compose it explicitly.
- Importing the generated file exposes a dispatchable health route at `app:app`.

[^scaffold]: `src/py/vertebra/cli/init.py` owns the shared scaffold.
