---
type: Task
id: "server-runtime-correctness:ch07:t01"
flow_id: server-runtime-correctness
chapter_id: ch07-docs-and-okf-sync
title: "Sync Server, Packaging, and CLI Reference Documentation"
description: "Document progressive streaming, fail-loud startup, worker-only mode, wheel-shipped vertebra-server, and --include-agent-skills in usage and CLI reference docs."
status: stable
state: completed
commit: d9475b72
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: []
files:
  - docs/usage/server-and-workers.md
  - docs/usage/packaging-and-deployment.md
  - docs/reference/cli.md
tests: []
read_set:
  - docs/usage/server-and-workers.md
  - docs/usage/packaging-and-deployment.md
  - docs/reference/cli.md
  - src/py/vertebra/cli/run.py
  - src/py/vertebra/cli/build.py
  - tools/docs/generate_cli_docs.py
estimated_loc: 160
tags:
  - docs
  - server
  - packaging
  - cli
sources:
  - id: server-docs
    resource: docs/usage/server-and-workers.md
    title: Server and workers usage documentation
  - id: packaging-docs
    resource: docs/usage/packaging-and-deployment.md
    title: Packaging and deployment documentation
  - id: cli-ref
    resource: docs/reference/cli.md
    title: Generated CLI reference page
---

# Task `t01`: Sync Server, Packaging, and CLI Reference Documentation

## Objective & Scope Boundary

Update `docs/usage/server-and-workers.md`, `docs/usage/packaging-and-deployment.md`, and `docs/reference/cli.md` in present tense per [`../spec.md` §2](../spec.md) [^server-docs] [^packaging-docs] [^cli-ref].

## Bounded Read Manifest

- `docs/usage/{server-and-workers,packaging-and-deployment}.md` & `docs/reference/cli.md` — target user docs.
- `src/py/vertebra/cli/{run,build}.py` & `tools/docs/generate_cli_docs.py` — CLI options and doc generator.

## Contracts & Edge Cases

- Document progressive ASGI/RSGI/native `App` streaming, `WorkerStartupError` + exit `78`, `WorkerRunner.resolve_queue_service` (`vertebra_queue_service`), and `vtb run --worker-only` `HealthProbeServer`.
- Document wheel-shipped `vertebra/bin/vertebra-server`, shared-`libpython` requirement, `make core-wheel-check`, and opt-in `vtb build --include-agent-skills` user skill staging.
- Regenerate `docs/reference/cli.md` via `uv run --no-sync python -m tools.docs.generate_cli_docs` so `--check` passes.

## Test Matrix

- Verified via `tools.docs.generate_cli_docs --check` and full Zensical site build (`make docs`).

## Verification

```bash
uv run --no-sync python -m tools.docs.generate_cli_docs --check && make docs
```

## Acceptance Criteria

- [ ] `docs/reference/cli.md` matches live Click options with zero drift.
- [ ] `make docs` exits `0` with zero broken links or missing docstrings.

[^server-docs]: `docs/usage/server-and-workers.md` — Server and workers usage documentation.
[^packaging-docs]: `docs/usage/packaging-and-deployment.md` — Packaging and deployment documentation.
[^cli-ref]: `docs/reference/cli.md` — Generated CLI reference page.
