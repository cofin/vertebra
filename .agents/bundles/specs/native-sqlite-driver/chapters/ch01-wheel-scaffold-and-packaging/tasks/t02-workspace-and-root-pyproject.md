---
type: Task
id: "native-sqlite-driver:ch01:t02"
flow_id: native-sqlite-driver
chapter_id: ch01-wheel-scaffold-and-packaging
title: "Register vertebra-sqlite in Cargo Workspace and Root pyproject.toml"
description: "Add vertebra-sqlite to Cargo workspace, [project.optional-dependencies].sqlite/all, uv.sources, maturin.exclude, and bumpversion."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch01:t01"
files:
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - pyproject.toml
tests: []
read_set:
  - src/rs/Cargo.toml
  - pyproject.toml
estimated_loc: 75
tags:
  - sqlite
  - packaging
  - cargo
sources:
  - id: cargo-ws
    resource: src/rs/Cargo.toml
    title: "Rust workspace member manifest"
  - id: root-pyproject
    resource: pyproject.toml
    title: "Root vertebra-core extras, uv sources, maturin excludes, and bumpversion"
---

# Task `t02`: Register vertebra-sqlite in Cargo Workspace and Root pyproject.toml

## Objective & Scope Boundary

Wire `vertebra-sqlite` into `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and `pyproject.toml` per [`../spec.md` §2](../spec.md) [^cargo-ws] [^root-pyproject].

## Bounded Read Manifest

- `src/rs/Cargo.toml` — Workspace `members` list.
- `pyproject.toml` — `optional-dependencies`, `uv.sources`, `maturin.exclude`, and `bumpversion`.

## Contracts & Edge Cases

- Add `"vertebra-sqlite"` to `src/rs/Cargo.toml` workspace members and generate lockfile entries.
- Add `sqlite = ["vertebra-sqlite==0.3.0"]`, include in `all`, add `vertebra-sqlite = { workspace = true }` / path source in `[tool.uv.sources]`, exclude `src/py/vertebra/_sqlite.pyi` from root wheel, and add `bumpversion` entries.
- **Edge Case**: Keep `bench` dependencies untouched so `3.14t` test environments stay lightweight.

## Test Matrix

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite` compiles and passes.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite
```

## Acceptance Criteria

- [ ] Workspace Cargo build and root `pyproject.toml` extras include `vertebra-sqlite` cleanly.
- [ ] Zero regressions in the chapter fast verification gate.

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
