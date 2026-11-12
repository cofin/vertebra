---
type: Task
id: "server-runtime-correctness:ch11:t02"
chapter_id: ch11-product-okf-and-release-gate
title: Sync tech stack, product docs, and the knowledge index
description: Update tech-stack.md for the maturin server binary include, release smoke matrix, and CLI worker-only behaviour; update product.md for incremental mounted streaming, fail-loud startup, and the wheel-shipped embedded server binary; list the limitations chapter in the root knowledge index.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "server-runtime-correctness:ch11:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/spec.md
  - .agents/bundles/product/tech-stack.md
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/index.md
  - .agents/bundles/product/product-guidelines.md
files:
  - .agents/bundles/product/tech-stack.md
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/index.md
tests: []
estimated_loc: 80
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Distribution & Import Namespace and CLI Entrypoint statements
  - id: product
    resource: .agents/bundles/product/product.md
    title: Runtime Topologies and Embedded Topology statements
  - id: knowledge-index
    resource: .agents/bundles/knowledge/index.md
    title: Root knowledge index
tags:
  - product
  - okf
---

# Task t02: Sync Tech Stack, Product Docs, and the Knowledge Index

## Objective & Scope Boundary

Update two product docs and the root knowledge index in present tense. Out of scope: `product-guidelines.md` (read-only reference) and coordinator-owned indexes.

## Bounded Read Manifest

- [Root spec](../../../spec.md) — truth block.
- `.agents/bundles/product/tech-stack.md` — "Distribution & Import Namespace" and "CLI Entrypoint" bullets [^tech-stack].
- `.agents/bundles/product/product.md` — "Runtime Topologies" and "Embedded Topology" bullets [^product].
- `.agents/bundles/knowledge/index.md` — "Server & Worker Runtime (`domains/server/`)" list [^knowledge-index].
- `.agents/bundles/product/product-guidelines.md` — naming and prose rules.

## Symbol & Signature Contracts

- `tech-stack.md`: the maturin `include` list adds `src/py/vertebra/bin/*` (wheel only); a distribution sentence states each core wheel ships `vertebra/bin/vertebra-server` per CPython ABI on Linux x86_64/aarch64, macOS x86_64/arm64, and Windows AMD64, verified by `make core-wheel-check` and the release `test_wheels` smoke; the CLI entrypoint bullet adds `vtb run --worker-only` and `vtb workers run` lifetime.
- `product.md`: Embedded and SHM topology bullets state that mounted ASGI/RSGI responses stream incrementally and that worker startup failures stop the server; a capability bullet states `vtb run` works from a wheel install with a shared-libpython interpreter.
- `knowledge/index.md`: add `  - [Server Runtime Limitations](domains/server/server-runtime-limitations.md)` under the server list.

## Edge Cases & Error Semantics

- No version bumps or changelog edits.
- Statements cite only symbols that exist after `ch01`–`ch07`.

## Public-Seam Test Blueprint

- **Verification category**: `documentation_validation`.

| Check | Concrete Input | Expected Output |
| --- | --- | --- |
| Review gate | `make review-check` | Exit `0` |
| Docs build | `make docs-check` | Exit `0` |
| Index link | `git grep -n "domains/server/server-runtime-limitations.md" -- .agents/bundles/knowledge/index.md` | One match |
| Include glob | `git grep -n "src/py/vertebra/bin" -- .agents/bundles/product/tech-stack.md` | One match |

## Verification

- Initial baseline: `git grep -n "src/py/vertebra/bin" -- .agents/bundles/product/tech-stack.md`
- Final:
  ```bash
  make review-check
  make docs-check
  ```
- Planned local commit: `docs(product): describe streaming, startup failure, and wheel server binary`

## Acceptance Criteria

- [ ] Product and tech-stack docs match the delivered runtime and packaging (R18).
- [ ] The root knowledge index lists the limitations chapter (R18).

## Notes & Discoveries

[^tech-stack]: `.agents/bundles/product/tech-stack.md` — Distribution & Import Namespace and CLI Entrypoint statements.
[^product]: `.agents/bundles/product/product.md` — Runtime Topologies and Embedded Topology statements.
[^knowledge-index]: `.agents/bundles/knowledge/index.md` — Root knowledge index.
