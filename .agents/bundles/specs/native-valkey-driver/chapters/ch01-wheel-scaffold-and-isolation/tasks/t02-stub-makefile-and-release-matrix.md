---
type: Task
id: native-valkey-driver:ch01-wheel-scaffold-and-isolation:t02
flow_id: native-valkey-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Author _valkey.pyi Stub and Wire Makefile and Release Workflows
description: Create src/py/vertebra/_valkey.pyi and wire vertebra-valkey into Makefile (dev, build, lint, lint-all, py-test, valkey-test, valkey-wheel-check) and .github/workflows/release.yaml.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch01-wheel-scaffold-and-isolation:t01
files:
  - src/py/vertebra/_valkey.pyi
  - Makefile
  - .github/workflows/release.yaml
tests: []
read_set:
  - src/py/vertebra/_oracle.pyi
  - Makefile
  - .github/workflows/release.yaml
estimated_loc: 210
tags:
  - pyo3
  - stubtest
  - makefile
  - ci
  - valkey
sources:
  - id: makefile
    resource: Makefile
    title: Root Makefile targets
  - id: release-wf
    resource: .github/workflows/release.yaml
    title: GitHub release wheel build matrix
verification_strategy: implementation_first
---

# Task `t02`: Author _valkey.pyi Stub and Wire Makefile and Release Workflows

## Objective & Scope Boundary

Create `src/py/vertebra/_valkey.pyi` declaring the scaffold surface that `src/rs/vertebra-valkey/src/lib.rs` exports in this chapter (`__version__`, `FREE_THREADED`, `GIL_USED`, `crc16_xmodem`, `valkey_hash_slot`, `parse_cluster_redirect`) and wire `vertebra-valkey` into `Makefile` and `.github/workflows/release.yaml` per [`../spec.md` §2](../spec.md) [^makefile] [^release-wf].

## Bounded Read Manifest

- `src/py/vertebra/_oracle.pyi` — Canonical `.pyi` stub formatting and docstrings.
- `Makefile` — `dev`, `build`, `lint`, `lint-all`, `py-test`, `*-test`, and `*-wheel-check` recipes.
- `.github/workflows/release.yaml` — Extension wheel build matrix.

## Contracts & Edge Cases

- `src/py/vertebra/_valkey.pyi` declares `__version__`, `FREE_THREADED`, `GIL_USED`, `crc16_xmodem`, `valkey_hash_slot`, and `parse_cluster_redirect`; `ValkeyClientConfig` and the client, pipeline, and listener classes (with the Valkey 9.0 `hexpire` family) are added to the stub by the Chapter 02 tasks that implement them.
- `Makefile` adds `vertebra-valkey` to `dev`, `build`, `lint`, `lint-all`, and `py-test`, plus dedicated `valkey-test` and `valkey-wheel-check` targets.
- **Edge Case**: Every class and function in `src/rs/vertebra-valkey/src/lib.rs` matches `src/py/vertebra/_valkey.pyi` so `mypy.stubtest vertebra._valkey --concise` exits `0`.

## Test Matrix

- Run `make dev && uv run --no-sync python -m mypy.stubtest vertebra._valkey --concise`.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._valkey --concise
```

## Acceptance Criteria

- [ ] `mypy.stubtest vertebra._valkey --concise` passes with zero errors.
- [ ] `Makefile` and `.github/workflows/release.yaml` include `vertebra-valkey`.
- [ ] Zero regressions in the fast verification gate.

[^makefile]: `Makefile` — Root Makefile targets.
[^release-wf]: `.github/workflows/release.yaml` — GitHub release wheel build matrix.
