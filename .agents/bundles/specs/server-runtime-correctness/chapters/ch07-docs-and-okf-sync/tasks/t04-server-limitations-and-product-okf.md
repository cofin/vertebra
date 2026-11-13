---
type: Task
id: "server-runtime-correctness:ch07:t04"
flow_id: server-runtime-correctness
chapter_id: ch07-docs-and-okf-sync
title: "Sync Worker Domain, Server Runtime Limitations, and Product OKF Chapters"
description: "Update worker-and-protocols.md, server-runtime-limitations.md, and product.md in place to reflect shipped runtime guarantees and remaining boundaries."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch07:t03"
files:
  - .agents/bundles/knowledge/domains/server/worker-and-protocols.md
  - .agents/bundles/knowledge/domains/server/server-runtime-limitations.md
  - .agents/bundles/product/product.md
tests: []
read_set:
  - .agents/bundles/knowledge/domains/server/worker-and-protocols.md
  - .agents/bundles/knowledge/domains/server/server-runtime-limitations.md
  - .agents/bundles/product/product.md
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/supervisor/_supervisor.py
  - src/rs/vertebra-core/src/id.rs
estimated_loc: 140
tags:
  - okf
  - server
  - limitations
  - product
sources:
  - id: worker-okf
    resource: .agents/bundles/knowledge/domains/server/worker-and-protocols.md
    title: Server worker and protocols domain chapter
  - id: limitations-okf
    resource: .agents/bundles/knowledge/domains/server/server-runtime-limitations.md
    title: Server runtime limitations chapter
  - id: product-okf
    resource: .agents/bundles/product/product.md
    title: Product definition and topologies
---

# Task `t04`: Sync Worker Domain, Server Runtime Limitations, and Product OKF Chapters

## Objective & Scope Boundary

Update `worker-and-protocols.md`, `server-runtime-limitations.md`, and `product.md` in place (`<= 200` lines each) per [`../spec.md` §2](../spec.md) [^worker-okf] [^limitations-okf] [^product-okf].

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/server/{worker-and-protocols,server-runtime-limitations}.md` & `.agents/bundles/product/product.md`.
- `src/py/vertebra/server/worker.py`, `supervisor/_supervisor.py`, & `src/rs/vertebra-core/src/id.rs`.

## Contracts & Edge Cases

- Update `worker-and-protocols.md` with `WorkerStartupError`, exit `78`, `vertebra.supervisor` lazy exports, `EmbeddedPythonRuntime`, `WorkerRunner.resolve_queue_service`, and `HealthProbeServer`.
- Rewrite `server-runtime-limitations.md` in present tense removing resolved gaps and recording remaining boundaries: `FrameReceiver` `Infallible` early-close on mid-stream error, per-worker SHM reader head-of-line backpressure under slow clients, SHM worker disconnect visibility via `notify_disconnect`, and fail-loud OS entropy panic.
- Update `product.md` with progressive mounted/native streaming, wheel-shipped `vertebra-server`, and opt-in standalone `.agents/skills` staging.

## Test Matrix

- Verified via `make review-check` and `make docs`.

## Verification

```bash
make docs && make review-check
```

## Acceptance Criteria

- [ ] `worker-and-protocols.md`, `server-runtime-limitations.md`, and `product.md` are `<= 200` lines with valid `verified.commit` and footnotes.
- [ ] `make docs && make review-check` exits `0`.

[^worker-okf]: `.agents/bundles/knowledge/domains/server/worker-and-protocols.md` — Server worker and protocols domain chapter.
[^limitations-okf]: `.agents/bundles/knowledge/domains/server/server-runtime-limitations.md` — Server runtime limitations chapter.
[^product-okf]: `.agents/bundles/product/product.md` — Product definition and topologies.
