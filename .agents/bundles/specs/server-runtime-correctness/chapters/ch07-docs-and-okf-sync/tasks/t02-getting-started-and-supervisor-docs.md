---
type: Task
id: "server-runtime-correctness:ch07:t02"
flow_id: server-runtime-correctness
chapter_id: ch07-docs-and-okf-sync
title: "Sync Installation, Getting-Started CLI, and Supervisor Reference Docs"
description: "Update installation platform matrix, getting-started CLI examples, and vertebra.supervisor API reference directives."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch07:t01"
files:
  - docs/getting-started/installation.md
  - docs/getting-started/cli.md
  - docs/reference/build-and-wasm.md
tests: []
read_set:
  - docs/getting-started/installation.md
  - docs/getting-started/cli.md
  - docs/reference/build-and-wasm.md
  - src/py/vertebra/supervisor/__init__.py
  - src/py/vertebra/supervisor/_supervisor.py
estimated_loc: 115
tags:
  - docs
  - installation
  - supervisor
  - cli
sources:
  - id: install-docs
    resource: docs/getting-started/installation.md
    title: Installation guide and platform matrix
  - id: build-wasm-docs
    resource: docs/reference/build-and-wasm.md
    title: Supervisor and packaging API reference
---

# Task `t02`: Sync Installation, Getting-Started CLI, and Supervisor Reference Docs

## Objective & Scope Boundary

Update `docs/getting-started/installation.md`, `docs/getting-started/cli.md`, and `docs/reference/build-and-wasm.md` per [`../spec.md` §2](../spec.md) [^install-docs] [^build-wasm-docs].

## Bounded Read Manifest

- `docs/getting-started/{installation,cli}.md` & `docs/reference/build-and-wasm.md` — target docs.
- `src/py/vertebra/supervisor/{__init__,_supervisor}.py` — public supervisor exports and `EmbeddedPythonRuntime`.

## Contracts & Edge Cases

- Use linked `pymdownx.tabbed` installation blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`) and include `linux-aarch64` plus shared-`libpython` guidance in `installation.md`.
- Reference public `vertebra.supervisor` exports (`run_supervisor`, `build_server_command`, `find_server_binary`, `find_repo_root`, `worker_main`) and `EmbeddedPythonRuntime` in `build-and-wasm.md`.

## Test Matrix

- Verified via `make docs` docstring/mkdocstrings resolution and link validation.

## Verification

```bash
make docs
```

## Acceptance Criteria

- [ ] Public `vertebra.supervisor` symbols render cleanly in `build-and-wasm.md`.
- [ ] `make docs` exits `0`.

[^install-docs]: `docs/getting-started/installation.md` — Installation guide and platform matrix.
[^build-wasm-docs]: `docs/reference/build-and-wasm.md` — Supervisor and packaging API reference.
