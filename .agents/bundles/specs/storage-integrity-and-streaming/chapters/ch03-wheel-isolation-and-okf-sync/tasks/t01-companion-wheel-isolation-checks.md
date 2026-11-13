---
type: Task
id: "storage-integrity-and-streaming:ch03:t01"
flow_id: storage-integrity-and-streaming
chapter_id: ch03-wheel-isolation-and-okf-sync
title: "Add Companion Wheel Isolation Verifier and Makefile/Release Gates"
description: "Author tools/scripts/verify_storage_wheel.py supporting storage, bigquery, spanner, and bundler, add *-wheel-check targets to Makefile, and harden release.yaml _vertebra assertions."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "storage-integrity-and-streaming:ch02:t02"
  - "storage-integrity-and-streaming:ch02:t03"
files:
  - tools/scripts/verify_storage_wheel.py
  - Makefile
  - .github/workflows/release.yaml
tests:
  - src/py/tests/unit/db/test_optional_driver_wheels.py
read_set:
  - tools/scripts/verify_oracle_wheel.py
  - Makefile
  - .github/workflows/release.yaml
  - src/rs/vertebra-storage/pyproject.toml
  - src/rs/vertebra-bundler/pyproject.toml
  - src/py/tests/unit/db/test_optional_driver_wheels.py
estimated_loc: 230
tags:
  - storage
  - packaging
  - makefile
  - ci
sources:
  - id: verify-ora-wheel
    resource: tools/scripts/verify_oracle_wheel.py
    title: "Reference wheel/sdist/isolated-venv verifier"
  - id: makefile
    resource: Makefile
    title: "Existing oracle-wheel-check and postgres-wheel-check recipes"
  - id: release-yml
    resource: .github/workflows/release.yaml
    title: "test_wheels companion wheel assertions"
---

# Task `t01`: Add Companion Wheel Isolation Verifier and Makefile/Release Gates

## Objective & Scope Boundary

Create `tools/scripts/verify_storage_wheel.py` covering `vertebra-storage`, `vertebra-bigquery`, `vertebra-spanner`, and `vertebra-bundler` (`G-12`), add `storage-wheel-check`, `bigquery-wheel-check`, `spanner-wheel-check`, and `bundler-wheel-check` to `Makefile`, and tighten `.github/workflows/release.yaml` per [`../spec.md` §2](../spec.md) [^verify-ora-wheel] [^makefile] [^release-yml]. Out of scope: user docs and OKF chapters (`t02`–`t03`).

## Bounded Read Manifest

- `tools/scripts/verify_oracle_wheel.py` — isolated wheel/sdist/venv verifier pattern [^verify-ora-wheel].
- `Makefile` — `oracle-wheel-check` and `postgres-wheel-check` recipes [^makefile].
- `.github/workflows/release.yaml` — `test_wheels` job assertions [^release-yml].
- `src/rs/vertebra-storage/pyproject.toml` and `src/rs/vertebra-bundler/pyproject.toml` — maturin `module-name` config.
- `src/py/tests/unit/db/test_optional_driver_wheels.py` — synthetic wheel archive unit tests.

## Contracts & Edge Cases

- `tools/scripts/verify_storage_wheel.py`: non-executable (`0644`, zero shebang), uses `sys.stdout.write`/`sys.stderr.write` (zero `print`), zero inline `#` comments, `msgspec.Struct` per-package specs (`storage`, `bigquery`, `spanner`, `bundler`), `--package` CLI flag defaulting to `storage`, checking: (1) `vertebra_core-*.whl` excludes companion `_<pkg>` binaries/stubs, (2) `vertebra_<pkg>-*.whl` contains solely `vertebra/_<pkg>.pyi` + `1` `vertebra/_<pkg>*.{so,pyd,dylib}` with zero `vertebra/*.py` or sibling stubs, (3) sdist contains `Cargo.toml`, `src/lib.rs`, and `vertebra/_<pkg>.pyi`, and (4) isolated outside-checkout venv raises `MissingDependencyError` (`vertebra-core[<pkg>]`) pre-install and imports expected native exports (`GIL_USED is False` where exposed) post-install.
- `Makefile`: add `.PHONY` targets `storage-wheel-check`, `bigquery-wheel-check`, `spanner-wheel-check`, and `bundler-wheel-check`.
- `.github/workflows/release.yaml`: add `assert not hasattr(_vertebra, "SyncObjectStore")` and `assert not hasattr(_vertebra, "Bundler")` in `test_wheels`.

## Test Matrix

- Add parametrized synthetic zip/tar archive tests in `src/py/tests/unit/db/test_optional_driver_wheels.py` across `("storage", "bigquery", "spanner", "bundler")`: valid wheel/sdist archives pass `verify_companion_wheel_contents` / `verify_companion_sdist_contents`; leaked `vertebra/__init__.py`, sibling `.pyi`, missing stub, or duplicate `.so` raises `RuntimeError`; verify `make -n {storage,bigquery,spanner,bundler}-wheel-check` resolves cleanly.

## Verification

```bash
uv run --no-sync ruff check tools/scripts/verify_storage_wheel.py src/py/tests/unit/db/test_optional_driver_wheels.py && uv run --no-sync mypy tools/scripts/verify_storage_wheel.py && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q && make -n storage-wheel-check bigquery-wheel-check spanner-wheel-check bundler-wheel-check >/dev/null
```

## Acceptance Criteria

- [ ] `verify_storage_wheel.py`, `Makefile`, and `release.yaml` enforce payload isolation across `storage`, `bigquery`, `spanner`, and `bundler` (`R5`).
- [ ] Zero regressions in `test_optional_driver_wheels.py`, Ruff, Mypy, and `make review-check`.

[^verify-ora-wheel]: `tools/scripts/verify_oracle_wheel.py` — Reference wheel/sdist/isolated-venv verifier.
[^makefile]: `Makefile` — Existing `oracle-wheel-check` and `postgres-wheel-check` recipes.
[^release-yml]: `.github/workflows/release.yaml` — `test_wheels` companion wheel assertions.
