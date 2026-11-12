---
type: Task
id: "server-runtime-correctness:ch11:t01"
chapter_id: ch11-product-okf-and-release-gate
title: Sync packaging OKF, PyO3 runtime gotchas, and the domains index
description: Update packaging-and-pgo.md with the server binary build script, maturin include, repair exclusions, and core-wheel-check smoke gate; update pyo3-and-python-runtime.md with relocatable rpath and shared-libpython gotchas; list server-runtime-limitations.md in the domains index.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "server-runtime-correctness:ch10:t04"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch06-relocatable-server-binary/spec.md
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch07-wheel-packaging-and-smoke/spec.md
  - .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md
  - .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
  - .agents/bundles/knowledge/domains/index.md
files:
  - .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md
  - .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
  - .agents/bundles/knowledge/domains/index.md
tests: []
estimated_loc: 110
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
  - id: packaging-okf
    resource: .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md
    title: Packaging and PGO domain chapter
  - id: pyo3-gotchas
    resource: .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
    title: PyO3 runtime gotchas
  - id: domains-index
    resource: .agents/bundles/knowledge/domains/index.md
    title: Domains index
tags:
  - okf
  - packaging
---

# Task t01: Sync Packaging OKF, PyO3 Runtime Gotchas, and the Domains Index

## Objective & Scope Boundary

Update two OKF chapters and the domains index. Out of scope: product docs and the root knowledge index (`t02`).

## Bounded Read Manifest

- [ch06 spec](../../ch06-relocatable-server-binary/spec.md), [ch07 spec](../../ch07-wheel-packaging-and-smoke/spec.md).
- `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` [^packaging-okf].
- `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` — "`PYO3_PYTHON`, Root `maturin develop` & Binary Freshness" bullet [^pyo3-gotchas].
- `.agents/bundles/knowledge/domains/index.md` — "Server & Worker Runtime (`server/`)" list [^domains-index].

## Symbol & Signature Contracts

- `packaging-and-pgo.md`: section "Server Binary in Core Wheels" — `tools/scripts/build_server_binary.py` (`ServerBinaryTarget`, `ServerBinaryBuilder.select_build_python`, `repair_command`), `VERTEBRA_SERVER_RELOCATABLE`, maturin `include` entry, cibuildwheel `before-build` and repair hooks, `make core-wheel-check`, CI `core-wheel-smoke`, release `test_wheels` matrix; `sources` for each symbol.
- `pyo3-and-python-runtime.md`: rewrite the binary-freshness bullet to cover packaged-first resolution, `VERTEBRA_SERVER_RELOCATABLE` suppressing the absolute rpath, manylinux images lacking libpython (build against python-build-standalone), macOS `@rpath/libpython…dylib` rewrite plus ad-hoc `codesign`, and `EmbeddedPythonRuntime` exporting `PYTHONHOME` and library paths.
- `domains/index.md`: add `- [Server Runtime Limitations](server/server-runtime-limitations.md) — Streaming, SHM disconnect, backpressure, buffered batch path, and packaged binary interpreter caveats.` and refresh the worker-lanes line to mention `resolve_queue_service` and `--worker-only`.

## Edge Cases & Error Semantics

- Keep implementation diagrams in OKF only.
- No numbered buzzword headings.

## Public-Seam Test Blueprint

- **Verification category**: `documentation_validation`.

| Check | Concrete Input | Expected Output |
| --- | --- | --- |
| Review gate | `make review-check` | Exit `0` |
| Index link | `git grep -n "server/server-runtime-limitations.md" -- .agents/bundles/knowledge/domains/index.md` | One match |
| Packaging symbols | `git grep -n "build_server_binary\|core-wheel-check" -- .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` | Both present |
| Gotcha | `git grep -n "VERTEBRA_SERVER_RELOCATABLE" -- .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` | One match |

## Verification

- Initial baseline: `git grep -n "server-runtime-limitations" -- .agents/bundles/knowledge`
- Final:
  ```bash
  make review-check
  git grep -n "server/server-runtime-limitations.md" -- .agents/bundles/knowledge/domains/index.md
  ```
- Planned local commit: `docs(okf): document wheel server binary packaging and runtime gotchas`

## Acceptance Criteria

- [ ] Packaging and PyO3 gotcha chapters describe the relocatable binary pipeline (R18).
- [ ] The domains index lists the limitations chapter (R18).

## Notes & Discoveries

[^packaging-okf]: `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` — Packaging and PGO domain chapter.
[^pyo3-gotchas]: `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` — PyO3 runtime gotchas.
[^domains-index]: `.agents/bundles/knowledge/domains/index.md` — Domains index.
