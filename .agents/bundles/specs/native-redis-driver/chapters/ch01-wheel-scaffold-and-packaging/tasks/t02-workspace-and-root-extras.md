---
type: Task
id: "native-redis-driver:ch01:t02"
flow_id: native-redis-driver
chapter_id: ch01-wheel-scaffold-and-packaging
title: "Wire vertebra-redis Workspace Member and [redis]/[valkey] Extras"
description: "Add vertebra-redis to Cargo workspace and root pyproject.toml [redis], [valkey], [all], uv.sources, maturin.exclude, and bumpversion."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch01:t01"
files:
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - pyproject.toml
tests: []
read_set:
  - src/rs/Cargo.toml
  - pyproject.toml
estimated_loc: 85
tags:
  - redis
  - valkey
  - packaging
sources:
  - id: cargo-ws
    resource: src/rs/Cargo.toml
    title: "Rust workspace member manifest"
  - id: root-pyproject
    resource: pyproject.toml
    title: "Root vertebra-core extras, uv sources, maturin excludes, and bumpversion"
---

# Task `t02`: Wire vertebra-redis Workspace Member and [redis]/[valkey] Extras

## Objective & Scope Boundary

Register `vertebra-redis` in `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and `pyproject.toml` per [`../spec.md` §2](../spec.md) [^cargo-ws] [^root-pyproject].

## Bounded Read Manifest

- `src/rs/Cargo.toml` — Workspace members list.
- `pyproject.toml` — Extras, `uv.sources`, `maturin.exclude`, and `bumpversion`.

## Contracts & Edge Cases

- Add both `redis = ["vertebra-redis==0.3.0"]` and `valkey = ["vertebra-redis==0.3.0"]` plus `all` entry in `[project.optional-dependencies]`.
- Exclude `src/py/vertebra/_redis.pyi` from the root `vertebra-core` wheel and add `bumpversion` entries.
- **Edge Case**: Deduplicate `vertebra-redis==0.3.0` in `[project.optional-dependencies].all`.

## Test Matrix

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis` passes.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis
```

## Acceptance Criteria

- [ ] `vertebra-core[redis]` and `vertebra-core[valkey]` both resolve to `vertebra-redis`.
- [ ] Zero regressions in the chapter fast verification gate.

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
