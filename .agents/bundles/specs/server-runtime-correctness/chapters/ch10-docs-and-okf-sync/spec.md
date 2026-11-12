---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch10-docs-and-okf-sync
title: "Chapter 10: Server, CLI, Packaging Docs and Runtime OKF Chapters with Limitations"
description: Update the server, CLI, supervisor, packaging, installation, and getting-started docs and the bridge, runner, dispatch, worker, and lanes OKF chapters to the delivered behaviour, and add the server runtime limitations chapter.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch07-wheel-packaging-and-smoke
  - ch08-fastpath-cleanup
  - ch09-runtime-native-cleanup
replan_trigger: null
current_task: null
plan_revision: 2
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: server-docs
    resource: docs/usage/server-and-workers.md
    title: Server topologies, worker lanes, ASGI runners, and batch timeout docs
  - id: cli-ref
    resource: docs/reference/cli.md
    title: CLI option reference including --worker-only
  - id: worker-okf
    resource: .agents/bundles/knowledge/domains/server/worker-and-protocols.md
    title: Python server worker and protocol adapters OKF chapter
  - id: bridge-okf
    resource: .agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md
    title: Tiered rings and batch bridge architecture chapter
tags:
  - docs
  - okf
  - limitations
---

# Chapter 10: Server, CLI, Packaging Docs and Runtime OKF Chapters with Limitations

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Public docs describe `--worker-only` as "Run background worker lanes with health probes only" while the flag did nothing, describe ASGI SSE support without stating buffering, and say nothing about lifespan failure behaviour or the packaged server binary [^server-docs] [^cli-ref]. OKF chapters describe the bridge and SHM response paths as unary and cite `find_server_binary` freshness only for development targets [^worker-okf] [^bridge-okf]. This chapter rewrites those pages in present tense to match `ch01`–`ch09` and records every caveat in a dedicated limitations chapter.

### 1.2 Non-Goals

- Product docs, packaging OKF, gotchas, and indexes (`ch11`).
- API reference pages generated from docstrings (refreshed by `make docs`).

### 1.3 Upstream Baseline & Code Analysis

`docs/usage/server-and-workers.md` documents topologies, `vtb workers run app:queue`, `AsgiRunner`/`AsgiLifespan`, and `VERTEBRA_ASGI_BATCH_TIMEOUT`; `docs/reference/cli.md` lists `--worker-only`; `docs/reference/build-and-wasm.md` renders `::: vertebra.supervisor._supervisor.run_supervisor`. OKF chapters follow the v0.2 frontmatter (`type`, `title`, `description`, `status`, `tags`, `generated`, `verified`, `sources`) with matching `[^id]:` footnotes, as in `domains/storage/object-storage-limitations.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R18a** (`t01`): server, CLI, and supervisor reference docs.
- **R18b** (`t02`): packaging, installation, and getting-started CLI docs.
- **R18c** (`t03`): bridge, runner, and dispatch OKF chapters.
- **R18d** (`t04`): worker, lanes OKF chapters and the new limitations chapter.

### 2.2 Concrete Type & Signature Contracts

Documentation files only. Every OKF chapter keeps v0.2 frontmatter, refreshes `verified` to the implementing commit, cites code as `path::Symbol` sources with matching footnotes, and uses relative links that resolve under `make review-check`.

### 2.3 Caller -> Seam -> Collaborator Map

| Reader | Page | Source of truth |
| --- | --- | --- |
| App developer | `docs/usage/server-and-workers.md` | `AsgiProtocol`, `AsgiSend`, `WorkerStartupError`, `WorkerRunner.resolve_queue_service`, `HealthProbeServer` |
| Operator | `docs/usage/packaging-and-deployment.md`, `docs/getting-started/installation.md` | `build_server_binary.py`, `EmbeddedPythonRuntime`, release matrix |
| Maintainer agent | OKF chapters | `ResponseStreamRegistry`, `VRSF` frames, `Supervisor` startup stop |

### 2.4 Edge-Case & Failure-Mode Matrix

| Risk | Control |
| --- | --- |
| Docs claim behaviour not shipped | Each task's verification greps for the symbols it documents. |
| Broken relative links | `make review-check` and `make docs-check`. |
| Historical prose | Present tense only; no "previously" or change narration. |

### 2.5 Alternatives Considered

Folding limitations into each chapter was rejected; the repository convention keeps caveats in a dedicated limitations chapter (`domains/storage/object-storage-limitations.md` precedent) so they are discoverable from the domains index.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/12 files documented in ch10]`.

| File | Task |
| --- | --- |
| `docs/usage/server-and-workers.md` | `t01` |
| `docs/reference/cli.md` | `t01` |
| `docs/reference/build-and-wasm.md` | `t01` |
| `docs/usage/packaging-and-deployment.md` | `t02` |
| `docs/getting-started/installation.md` | `t02` |
| `docs/getting-started/cli.md` | `t02` |
| `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md` | `t03` |
| `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` | `t03` |
| `.agents/bundles/knowledge/domains/http/server-and-dispatch.md` | `t03` |
| `.agents/bundles/knowledge/domains/server/worker-and-protocols.md` | `t04` |
| `.agents/bundles/knowledge/domains/server/worker-lanes.md` | `t04` |
| `.agents/bundles/knowledge/domains/server/server-runtime-limitations.md` | `t04` |

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R18a | [t01](tasks/t01-server-cli-supervisor-docs.md) | `documentation_validation` | `make docs-check` |
| R18b | [t02](tasks/t02-packaging-install-docs.md) | `documentation_validation` | `make docs-check` |
| R18c | [t03](tasks/t03-transport-runner-dispatch-okf.md) | `documentation_validation` | `make review-check` |
| R18d | [t04](tasks/t04-worker-okf-and-limitations.md) | `documentation_validation` | `make review-check` |

## 5. Implementation Plan

- [ ] server-runtime-correctness:ch10:t01 — Document streaming, lifespan failure, worker-only, queue binding, and supervisor exports (tasks/t01-server-cli-supervisor-docs.md)
- [ ] server-runtime-correctness:ch10:t02 — Document the wheel-shipped server binary, shared-libpython requirement, and platform matrix (tasks/t02-packaging-install-docs.md)
- [ ] server-runtime-correctness:ch10:t03 — Sync bridge, runner, and dispatch OKF chapters to stream frames and supervisor stop (tasks/t03-transport-runner-dispatch-okf.md)
- [ ] server-runtime-correctness:ch10:t04 — Sync worker and lanes OKF chapters and record server runtime limitations (tasks/t04-worker-okf-and-limitations.md)

**Chapter Recovery Checkpoint:** `make docs-check && make review-check` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline**: `plan_commit: null`; chapter `state: planned`.
- **Remaining Tasks**: `t01`–`t04` (`0/4`); `[0/12 files documented in ch10]`.
- **Downstream Dependents**: `ch11:t01` indexes the limitations chapter.

[^server-docs]: `docs/usage/server-and-workers.md` — Server topologies, worker lanes, ASGI runners, and batch timeout docs.
[^cli-ref]: `docs/reference/cli.md` — CLI option reference including `--worker-only`.
[^worker-okf]: `.agents/bundles/knowledge/domains/server/worker-and-protocols.md` — Python server worker and protocol adapters OKF chapter.
[^bridge-okf]: `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md` — Tiered rings and batch bridge architecture chapter.
