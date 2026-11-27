---
type: Task
id: "runtime-perf-remediation:ch04:t07"
chapter_id: ch04-data-benchmarks-and-verification
title: Sync runtime, IPC, and HTTP OKF knowledge chapters
description: Update topologies-and-embedding.md, tiered-rings-and-batch-bridge.md, and batching-dispatch-and-backpressure.md in present-tense OKF v0.2 format matching verified ch02, ch03, and ch04:t01 implementations.
status: stable
state: completed
priority: P2
verification_strategy: documentation_validation
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch04:t06"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/spec.md
  - .agents/bundles/knowledge/architecture/runtime/topologies-and-embedding.md
  - .agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md
  - .agents/bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md
files:
  - .agents/bundles/knowledge/architecture/runtime/topologies-and-embedding.md
  - .agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md
  - .agents/bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md
tests: []
estimated_loc: 150
plan_revision: 3
plan_commit: "9dfbd408"
commit: "48938aee"
sources:
  - id: src-root-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/spec.md
    title: Root specification documentation plan for runtime, IPC, and HTTP chapters
  - id: src-ch04-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
    title: Chapter 4 OKF knowledge sync requirements
tags:
  - okf
  - docs
  - runtime
  - ipc
  - http-ingress
---

# Sync Runtime, IPC, and HTTP OKF Knowledge Chapters

## Objective & Scope Boundary

Rewrite the runtime topology, IPC bridge, and HTTP batching/backpressure OKF knowledge chapters in present tense to reflect the final verified implementations from `ch02`, `ch03`, `ch04:t01`, and `ch04:t06` [^src-root-spec] [^src-ch04-spec].

Preserve strict OKF v0.2 compliance across all three chapters without leaving historical notes, migration prose, or standalone `---` body rules.

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/spec.md`
- `.agents/bundles/knowledge/architecture/runtime/topologies-and-embedding.md`
- `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md`
- `.agents/bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md`

## Contracts & Edge Cases

- `topologies-and-embedding.md`: document the empirically verified 1-worker GIL Embedded Tokio thread default (`available_cpus.clamp(1, 2)`, honoring `VERTEBRA_TOKIO_THREADS`), consumer-aligned `RequestBatcher` shard sizing, and cgroup `cpu.max`-aware `EmbeddedSupervisor` budgeting (`1` Tokio thread per child).
- `tiered-rings-and-batch-bridge.md`: document `BatchResponseSink`, zero-copy `InProcessRequest` carrying `http::uri::PathAndQuery`, and owned `Vec<u8>` handoff in `ResponseSubmitter::submit_batch_flushed`.
- `batching-dispatch-and-backpressure.md`: document lazy `IdleTimeoutStream`, per-connection collector shard affinity, single-lock `BridgeCorrelation` batch allocation and `complete_many`, zero-Arc `AdmissionPermit` + coarse circuit clock, `InFlightRequests::insert_with_now`, and reusable `ShmDispatcher` scratch buffers.
- Keep every `<!-- truth: start --> ... <!-- truth: end -->` block at `<= 40` lines and match every frontmatter `sources:` entry 1:1 with `[^id]:` footnotes.
- Verify all relative links and file symbol references resolve against the live tree.

## Test Matrix

| Chapter | Required Coverage | OKF Invariant |
| :--- | :--- | :--- |
| `topologies-and-embedding.md` | 1w Tokio `clamp(1, 2)` default, shard sizing, cgroup `cpu.max` | `<= 40` line truth block, 1:1 `sources`/`[^id]:`, zero body `---` |
| `tiered-rings-and-batch-bridge.md` | `BatchResponseSink`, `PathAndQuery`, `submit_batch_flushed` | `<= 40` line truth block, 1:1 `sources`/`[^id]:`, zero body `---` |
| `batching-dispatch-and-backpressure.md` | `IdleTimeoutStream`, shard affinity, `BridgeCorrelation`, `BackpressureController`, `ShmDispatcher` | `<= 40` line truth block, 1:1 `sources`/`[^id]:`, zero body `---` |

## Verification

```bash
make review-check
```

## Acceptance Criteria

- All three OKF chapters accurately describe current runtime, IPC, and HTTP ingress behavior in present tense.
- Truth blocks, frontmatter `sources`, `[^id]:` footnotes, and relative links pass validation cleanly.

[^src-root-spec]: `.agents/bundles/specs/runtime-perf-remediation/spec.md` — Root specification documentation plan for runtime, IPC, and HTTP chapters.
[^src-ch04-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md` — Chapter 4 OKF knowledge sync requirements.
