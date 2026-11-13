---
type: Spec
flow_id: server-runtime-correctness
chapter_id: ch07-docs-and-okf-sync
title: "Chapter 07: Public Documentation and Runtime OKF Knowledge Synchronization"
description: "Update server, CLI, and packaging docs, compact pyo3-ffi-and-batch-dispatch.md to <= 200 lines, and synchronize IPC, runner, worker, limitations, and product OKF chapters."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch04-wheel-smoke-and-ci
  - ch05-fastpath-consolidation
  - ch06-runtime-native-cleanup
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: null
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - docs
  - okf
  - server
  - packaging
sources:
  - id: server-docs
    resource: docs/usage/server-and-workers.md
    title: "Server topologies, streaming, and worker lanes guide"
  - id: packaging-docs
    resource: docs/usage/packaging-and-deployment.md
    title: "Wheel, standalone, and onefile packaging guide"
  - id: bridge-okf
    resource: .agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md
    title: Tiered rings and batch bridge architecture chapter
  - id: pyo3-ffi-okf
    resource: .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
    title: PyO3 FFI and batch dispatch chapter currently at 212 lines
  - id: worker-okf
    resource: .agents/bundles/knowledge/domains/server/worker-and-protocols.md
    title: Server worker and protocol domain chapter
  - id: limitations-okf
    resource: .agents/bundles/knowledge/domains/server/server-runtime-limitations.md
    title: Server runtime limitations chapter
  - id: product-okf
    resource: .agents/bundles/product/product.md
    title: Product capabilities and topology overview
---

# Chapter 07: Public Documentation and Runtime OKF Knowledge Synchronization

<!-- truth: start -->
- Public `docs/` pages accurately document progressive ASGI/RSGI/native streaming, `WorkerStartupError` + exit `78`, `vertebra.supervisor` exports, `vtb workers run` and `vtb run --worker-only`, wheel-shipped `vertebra-server` (`linux-aarch64` included), and `vtb build --include-agent-skills` [^server-docs] [^packaging-docs].
- `pyo3-ffi-and-batch-dispatch.md` (`212` lines -> `<= 200` lines), `tiered-rings-and-batch-bridge.md`, `asgi-and-wsgi-runners.md`, and `worker-and-protocols.md` reflect `submit_stream_frame`, native `_dispatch.py` streaming, `CompiledRouteBase`, and `EmbeddedPythonRuntime` with `<= 200` lines and `<= 40`-line truth blocks [^bridge-okf] [^pyo3-ffi-okf] [^worker-okf].
- `server-runtime-limitations.md` and `product.md` update in place to remove closed gaps and record remaining architectural boundaries (mid-stream `Infallible` truncation, per-worker SHM reader head-of-line backpressure, and fail-loud OS entropy panic) [^limitations-okf] [^product-okf].
<!-- truth: end -->

## 1. Specification, Context & Provenance

After `ch01`–`ch06` land, public documentation and OKF chapters still describe buffered mounted streaming, swallowed lifespan errors, missing wheel server binaries, and legacy FastPath aliases, while `pyo3-ffi-and-batch-dispatch.md` sits at `212` lines (above the `200`-line OKF target) [^server-docs] [^pyo3-ffi-okf] [^limitations-okf].

## 2. Requirements, Concrete Contracts & Architecture

- **`R10a` (`t01`)**: Update `docs/usage/server-and-workers.md`, `docs/usage/packaging-and-deployment.md`, and `docs/reference/cli.md` (regenerating CLI reference via `tools.docs.generate_cli_docs` if CLI flags changed for `--include-agent-skills` / `--worker-only`).
- **`R10b` (`t02`)**: Update `docs/getting-started/installation.md`, `docs/getting-started/cli.md`, and `docs/reference/build-and-wasm.md` (`vertebra.supervisor` exports, `EmbeddedPythonRuntime`, shared-libpython requirement, `linux-aarch64` wheel/binary matrix).
- **`R10c` (`t03`)**: Sync `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md`, `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (compacting `212L -> <= 200L` while adding `submit_stream_frame`, `max_stream_frame_payload`, and direct `_zero_copy` bindings), and `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` (`CompiledRouteBase` + `__vertebra_fastpath__`), refreshing `verified.commit`.
- **`R10d` (`t04`)**: Sync `.agents/bundles/knowledge/domains/server/worker-and-protocols.md`, `.agents/bundles/knowledge/domains/server/server-runtime-limitations.md` (updating in place with residual boundaries only), and `.agents/bundles/product/product.md` (`<= 200` lines each, `read_set <= 6`).

## 3. Documentation & OKF Knowledge Plan

Every documentation deliverable in this flow is executed and validated in `t01`–`t04` (`12` files total across `4` tasks).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R10a` | `t01` | `docs/usage/{server-and-workers,packaging-and-deployment}.md`, `docs/reference/cli.md` | `make docs` |
| `R10b` | `t02` | `docs/getting-started/{installation,cli}.md`, `docs/reference/build-and-wasm.md` | `make docs` |
| `R10c` | `t03` | `tiered-rings-and-batch-bridge.md`, `pyo3-ffi-and-batch-dispatch.md`, `asgi-and-wsgi-runners.md` | `make review-check` + line count `<= 200` |
| `R10d` | `t04` | `worker-and-protocols.md`, `server-runtime-limitations.md`, `product.md` | `make docs && make review-check` |

## 5. Implementation Plan

- [ ] [t01-server-cli-and-packaging-docs.md](tasks/t01-server-cli-and-packaging-docs.md) — Sync `server-and-workers.md`, `packaging-and-deployment.md`, and `docs/reference/cli.md`
- [ ] [t02-getting-started-and-supervisor-docs.md](tasks/t02-getting-started-and-supervisor-docs.md) — Sync `installation.md`, `getting-started/cli.md`, and `build-and-wasm.md`
- [ ] [t03-ipc-ffi-and-runner-okf.md](tasks/t03-ipc-ffi-and-runner-okf.md) — Sync `tiered-rings-and-batch-bridge.md`, compact `pyo3-ffi-and-batch-dispatch.md` (`<= 200L`), and sync `asgi-and-wsgi-runners.md`
- [ ] [t04-server-limitations-and-product-okf.md](tasks/t04-server-limitations-and-product-okf.md) — Sync `worker-and-protocols.md`, `server-runtime-limitations.md`, and `product.md`

## 6. Current State & Remaining Plan Snapshot

- **Fast Chapter Gate (`< 35s`)**: `make docs && make review-check`.

[^server-docs]: `docs/usage/server-and-workers.md` — Server topologies, streaming, and worker lanes guide.
[^packaging-docs]: `docs/usage/packaging-and-deployment.md` — Wheel, standalone, and onefile packaging guide.
[^bridge-okf]: `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md` — Tiered rings and batch bridge architecture chapter.
[^pyo3-ffi-okf]: `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — PyO3 FFI and batch dispatch chapter currently at 212 lines.
[^worker-okf]: `.agents/bundles/knowledge/domains/server/worker-and-protocols.md` — Server worker and protocol domain chapter.
[^limitations-okf]: `.agents/bundles/knowledge/domains/server/server-runtime-limitations.md` — Server runtime limitations chapter.
[^product-okf]: `.agents/bundles/product/product.md` — Product capabilities and topology overview.
