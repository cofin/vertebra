---
type: Spec
flow_id: knowledge-resync
title: Knowledge Bundle Resynchronization and Final Roadmap Review
state: closed
plan_revision: 2
plan_commit: ""
state_revision: 1
current_task: null
last_operation: finish
operation_targets: []
last_verified_checkpoint: 2026-10-22T17:45:00Z
created_at: 2026-10-22T03:30:00Z
updated_at: 2026-10-22T17:45:00Z
description: Resynchronize all OKF v0.2 knowledge, product, and workflow documentation with the post-refactor codebase layout and execute the final 8-area + Flow correctness/quality review over the full roadmap range.
tags: [docs, okf, knowledge]
parent_prd: vertebra-design-quality
research: [../vertebra-design-quality/findings.md]
---

# Flow: Knowledge Bundle Resynchronization and Final Roadmap Review

**Flow ID:** `knowledge-resync`

## Specification

### Parent Context

Parent PRD: [`vertebra-design-quality`](../vertebra-design-quality/spec.md) (Chapter 10 of 10).
- **Depends on:** [`test-suite-consolidation`](../test-suite-consolidation/spec.md) (Chapter 9) and Chapters 1–8. Every file move, symbol rename, stub replacement, MCP/A2A delegation to `litestar-mcp`, hot-path optimization, and test-layout consolidation has landed before this chapter runs.
- **Unblocks:** Roadmap completion and archival of `vertebra-design-quality`.
- **Roadmap Requirements Owned:**
  - **R13** — Knowledge resync: update `.agents/bundles/knowledge/**`, `.agents/bundles/product/**`, `.agents/bundles/index.md`, `.agents/bundles/log.md`, `AGENTS.md`, and `README.md` so every `sources:` frontmatter path resolves on disk and every documented symbol, module layout, and architecture description matches the final implementation.
  - **R2 (Final Gate)** — Re-run all 8 area reviewers (`runtime-ipc-reviewer`, `http-ingress-reviewer`, `pyo3-bridge-reviewer`, `python-framework-reviewer`, `typescript-client-reviewer`, `performance-reviewer`, `tests-reviewer`, `docs-okf-reviewer`) plus Flow `code-reviewer` and `quality-reviewer` over `<roadmap base>..HEAD`, producing `.agents/bundles/specs/knowledge-resync/research/final-ledger.md` with zero open `critical` or `important` findings.

### Code Analysis Summary

1. **Architecture and Domain Knowledge Drift (`HTTP-27`, `MCP-07`, Chapters 2–7 Moves):**
   - `.agents/bundles/knowledge/architecture/http/ingress-and-dispatch.md` (`HTTP-27`) still describes a 256-shard `InFlightRequests` map, an HTTP/1.1-only accept loop, and `ShutdownHandle::trigger` (which did not notify the accept loop) rather than the 8-shard cache-line-padded `InFlightRequests`, `hyper_util::server::conn::auto::Builder` h2c prior-knowledge listener, and `CancellationToken` graceful drain from Chapter 3.
   - `.agents/bundles/knowledge/architecture/ipc/shm-and-rings.md`, `architecture/runtime/pyo3-and-freethreaded.md`, `architecture/runtime/platform-and-lifecycle.md`, `architecture/http/protocol-and-offload-modules.md`, `architecture/worker/asgi-wsgi-rsgi.md`, and `architecture/frontend/ssr-and-bundler.md` reference pre-split file paths (`vertebra-core/src/{ring.rs,shm.rs,platform.rs,protocol.rs}`, `vertebra-http/src/{server.rs,dispatcher.rs,backpressure.rs,ws_hub.rs,mcp_bridge.rs,worker.rs}`, `vertebra-py/src/{lib.rs,request.rs,wsgi.rs,rsgi.rs}`, and pre-split `vertebra/server` worker modules) instead of the post-Chapter-2/4/5/6/7 layout (`ring/{mod,spsc,mpmc,slot}.rs`, `transport.rs`, `server/{worker,_dispatch}.py`), and the deleted two-decoder `ResponseSlotHeader` / `ResponseFrameView` split.
   - `.agents/bundles/knowledge/domains/{framework,server,ipc,http,frontend,data,extensions,storage,telemetry}/` reference removed packages (`vertebra.framework.mcp`, `vertebra.framework.a2a`, `vertebra.extensions.worker`, `vertebra.extensions.litestar._bridge.LitestarEcosystemBridge`, `src/rs/vertebra-db`) instead of first-party `litestar-mcp` (`LitestarMCP`, `MCPConfig`, `LitestarA2A`, `A2AConfig`) (`MCP-07`) and the post-refactor module layouts from Chapters 2–7.
2. **Patterns, Gotchas, and Testing Chapters:**
   - `.agents/bundles/knowledge/patterns/{python,rust,typescript}/`, `.agents/bundles/knowledge/gotchas/`, and `.agents/bundles/knowledge/testing/` must capture the durable patterns established in Chapters 2–9:
     - Single `ResponseSlot` wire decoder in `vertebra_core::protocol::response` and shared `park_until` / `publish_request` / `publish_response` helpers.
     - Explicit `UnsupportedBackendError` policy (never return fabricated stub data when an optional driver or runtime is unconfigured).
     - Sharded `EventLoopPool` (`VERTEBRA_LOOP_SHARDS`) and bounded `BatchBufferPool` in `vertebra.server`.
     - `MsgpackCodec` in `vertebra.utils.serializers`, `SqlIdentifier.validate` in `vertebra.data._sql_ident`, `ReconnectScheduler` in `src/ts/vertebra-client/src/client/reconnect.ts`, and `QueryController` in `src/ts/vertebra-client/src/query/index.ts`.
     - Canonical 1:1 module-mirrored test layout, single-binary Rust `tests/it/main.rs` integration suites, strong-vs-weak assertion rules, branch-coverage floor in `pyproject.toml`, and `make bench-check BASELINE=pre-refactor`.
3. **Product Docs, Root Index, Workflow, `AGENTS.md`, and `README.md`:**
   - `.agents/bundles/product/{product,product-guidelines,tech-stack}.md`, `.agents/bundles/knowledge/workflow.md`, `.agents/bundles/index.md`, `AGENTS.md`, and `README.md` must reflect the final crate list (`vertebra-core`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`, `vertebra-storage`; `vertebra-db` removed), optional extras (`mcp = ["litestar-mcp>=0.3.0"]`, `wasm = ["wasmtime>=27.0"]`), `Makefile` targets (`make review-check`, `make bench-save`, `make bench-check`), and canonical domain symbol table.

### Relevant Patterns

- **OKF v0.2 Frontmatter & Link Integrity:** Every `.agents/bundles/**/*.md` file carries `type`, `updated_at`, `description`, `tags`, and `sources` (with existing repo-relative paths), and passes `uv run python -m tools.scripts.review_setup validate --rules bundles,privacy`.
- **Zero-Slop Technical Prose:** Direct, factual descriptions of current module responsibilities and invariants without historical changelog commentary, revision callouts, or count-prefixed labels.

### Requirements

- **C10-R1 (`HTTP-27`, `MCP-07`, `R13`):** Update all `.agents/bundles/knowledge/architecture/**` and `.agents/bundles/knowledge/domains/**` chapters and `.agents/bundles/knowledge/index.md` so every `sources:` entry resolves on disk and every architectural/domain description matches the post-Chapters 2–8 codebase (including 8-shard `InFlightRequests`, h2c `AutoBuilder`, `CancellationToken` drain, and `litestar-mcp` delegation).
- **C10-R2 (`R13`):** Update `.agents/bundles/knowledge/patterns/**`, `.agents/bundles/knowledge/gotchas/**`, and `.agents/bundles/knowledge/testing/**` with the post-refactor Rust/Python/TypeScript patterns, `UnsupportedBackendError` policy, `EventLoopPool`, single-binary `tests/it/main.rs` layout, coverage floor, and benchmark regression gate.
- **C10-R3 (`R13`):** Update `.agents/bundles/product/{product,product-guidelines,tech-stack}.md`, `.agents/bundles/index.md`, `.agents/bundles/log.md`, `.agents/bundles/knowledge/workflow.md`, `AGENTS.md`, and `README.md` to match the final crate topology, symbol names, extras, and `Makefile` commands.
- **C10-R4 (`R2`):** Execute all 8 area reviewers plus Flow `code-reviewer` and `quality-reviewer` over `<roadmap base>..HEAD`, record the consolidated ledger at `.agents/bundles/specs/knowledge-resync/research/final-ledger.md`, and verify zero open `critical` or `important` findings and green `make review-check` + `make check`.

### Target Layout

```text
.agents/bundles/
  index.md
  log.md
  product/
    product.md
    product-guidelines.md
    tech-stack.md
  knowledge/
    index.md
    workflow.md
    architecture/
      runtime/
        pyo3-and-freethreaded.md
        platform-and-lifecycle.md
      ipc/
        shm-and-rings.md
      http/
        ingress-and-dispatch.md
        protocol-and-offload-modules.md
      worker/
        asgi-wsgi-rsgi.md
      frontend/
        ssr-and-bundler.md
    domains/
      framework/
        routing-and-app.md
      server/
        worker-and-lifecycle.md
      ipc/
        wire-protocol.md
      http/
        server-and-middleware.md
      frontend/
        ts-client-and-inertia.md
      data/
        gateways-and-analytics.md
      extensions/
        litestar-alchemy-sqlspec.md
      storage/
        object-store-and-signing.md
      telemetry/
        tracing-metrics-logging.md
    patterns/
      python/
        code-organization.md
        serialization-and-utils.md
      rust/
        unsafe-and-atomics.md
      typescript/
        client-conventions.md
    testing/
      polyglot-verification.md
    gotchas/
      shm-and-event-loops.md
AGENTS.md
README.md
```

### Risks

- **Stale `sources:` Frontmatter After Multi-Chapter Splits:** If any knowledge file retains a `sources:` entry pointing to a split or deleted file (`ring.rs`, `server.rs`, `vertebra-db`, `framework/mcp`), `make review-check` fails. Mitigation: Run `uv run python -m tools.scripts.review_setup validate --rules bundles,privacy` after each task (`10.1`, `10.2`, `10.3`) and grep `.agents/bundles/` for every deleted module name.
- **Late Findings Surfaced in `10.4` Final Review:** Running all 8 area reviewers across the full roadmap diff may surface minor lint, docstring, or documentation discrepancies. Mitigation: Task `10.4` validates the merged ledger via `tools.scripts.review_setup ledger` and requires resolving any `critical` or `important` finding before the chapter gate closes.

## Implementation Plan

### Phase 1: Knowledge Bundle Resynchronization

- [x] `10.1` Resynchronize architecture and domain knowledge chapters with post-refactor module paths (`documentation_validation`)
- [x] `10.2` Update patterns, gotchas, and testing knowledge chapters (`documentation_validation`, depends on `10.1`)
- [x] `10.3` Update product docs, root bundle index, workflow guide, `AGENTS.md`, and `README.md` (`documentation_validation`, depends on `10.2`)

### Phase 2: Final Full-Range Roadmap Review

- [x] `10.4` Run all 8 area reviewers plus Flow code and quality reviewers over `<roadmap base>..HEAD` and validate `final-ledger.md` (`integration_acceptance`, depends on `10.3`)
- [x] Chapter gate: `make review-check` and `make check` exit 0, `uv run python -m tools.scripts.review_setup ledger --path .agents/bundles/specs/knowledge-resync/research/final-ledger.md` exits 0 with 0 open `critical`/`important` findings, and `docs-okf-reviewer` reports 0 open findings.

## Requirement-to-task trace

| Requirement | Tasks |
| :--- | :--- |
| `C10-R1` (`R13`, `HTTP-27`, `MCP-07` architecture & domain chapters) | `10.1` |
| `C10-R2` (`R13` patterns, gotchas, and testing chapters) | `10.2` |
| `C10-R3` (`R13` product docs, `workflow.md`, `AGENTS.md`, `README.md`, `log.md`) | `10.3` |
| `C10-R4` (`R2` final 8-area + Flow correctness/quality review) | `10.4` |

## Finding-to-task trace

| Finding ID | Task | Resolution Summary |
| :--- | :--- | :--- |
| `HTTP-27` | `10.1` | Update `architecture/http/ingress-and-dispatch.md` to document the 8-shard cache-line-padded `InFlightRequests` map, `AutoBuilder` h2c prior-knowledge support, and `CancellationToken` graceful drain. |
| `MCP-07` | `10.1` | Update `domains/extensions/litestar-alchemy-sqlspec.md`, `domains/framework/routing-and-app.md`, and `architecture/http/protocol-and-offload-modules.md` to reflect deletion of `vertebra.framework.{mcp,a2a}` and `mcp_bridge.rs` in favor of first-party `litestar-mcp` (`LitestarMCP`, `MCPConfig`, `LitestarA2A`, `A2AConfig`). |

## Continuity Snapshot

- **State:** `planned` — 4 sequential tasks (`10.1` -> `10.2` -> `10.3` -> `10.4`).
- **Next Action:** Execute `knowledge-resync:10.1` once Chapter 9 (`test-suite-consolidation`) passes its chapter gate.
