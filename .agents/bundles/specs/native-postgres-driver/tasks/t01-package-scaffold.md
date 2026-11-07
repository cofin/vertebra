---
type: Task
id: "native-postgres-driver:t01"
title: "Register the standalone optional PostgreSQL wheel"
description: "Register the standalone optional PostgreSQL wheel for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on: []
files:
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/vertebra-postgres/pyproject.toml
  - src/rs/vertebra-postgres/build.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/py/vertebra/_postgres.pyi
  - pyproject.toml
  - uv.lock
  - Makefile
  - .github/workflows/release.yaml
  - .agents/skills/review-core/references/areas.toml
tests: []
plan_revision: 2
plan_commit: cdb978d3
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T04:15:00Z"
commit: null
tags:
  - postgres
  - connection
  - native
---

# Register the standalone optional PostgreSQL wheel

## Objective

Register the standalone optional PostgreSQL wheel, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 230 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Inspect workspace version, root bumpversion files, sibling vertebra-spanner Cargo/Maturin/build.rs, every Makefile extension list, release wheel/sdist/combined-install list and review area ownership. `git status --short -b`, `git rev-parse HEAD`, `uv lock --check` establish the baseline. Static packaging uses no artificial behavioral RED test.
2. Add workspace member vertebra-postgres, lib name _postgres, cdylib+rlib, default features empty, extension-module=[pyo3/extension-module], pyo3-build-config, pyo3/bytes/tokio/thiserror and vertebra-db[pyo3]. Pin postgres-protocol="=0.6.12" and tokio-postgres="=0.7.18" dev-only; use tokio dev test-util. Do not add tokio-postgres as production client. The module declares gil_used=false and exports only truthful FREE_THREADED until real facade tasks register classes. Stub has only implemented exports.
3. Follow sibling optional wheel python-source/module-name/include/exclude pattern so only _postgres binary/stub share the core namespace. Register root postgres extra, exact-version all/dev references, uv editable source, root dependency version replacements and per-wheel version bump. Regenerate Cargo/uv locks without broad upgrades; uv lock --offline may use normal uv lock if cache lacks dependencies.
4. Extend all Makefile dev/release build exclusions and maturin develops, py-test stubtest and both optional stub-discovery tuples. Register package in release.yaml platform wheel matrix with exports FREE_THREADED, sdist list, combined wheel no-deps list, selected extras list and namespace/version dictionary. t26 replaces exports with actual clients. Register crate/stub in owning runtime/PyO3 review areas without blanket globs.
5. Build/import/stubtest scaffold, inspect wheel zip contains only own module/stub/metadata and no duplicated Python sources, and inspect sdist has buildable required workspace source. Commit build(postgres): register standalone optional wheel.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo check --manifest-path src/rs/Cargo.toml -p vertebra-postgres
uv lock --check
uv build src/rs/vertebra-postgres --wheel --sdist --out-dir /tmp/vertebra-postgres-scaffold-dist
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
make review-check
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R1: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
