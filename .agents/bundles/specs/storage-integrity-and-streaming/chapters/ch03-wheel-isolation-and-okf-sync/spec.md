---
type: Spec
flow_id: storage-integrity-and-streaming
chapter_id: ch03-wheel-isolation-and-okf-sync
title: "Chapter 03: Companion Wheel Payload Isolation, User Docs, and Storage OKF Sync"
description: "Adds verify_storage_wheel.py and Makefile/release.yaml payload isolation checks across storage, bigquery, spanner, and bundler, updates storage user docs and changelog, and synchronizes object-storage OKF chapters within 200 lines."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch02-streaming-arrow-writes-and-integrations
plan_commit: 18baadcd
updated_at: "2026-11-12T23:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:55:00Z"
    commit: 18baadcd
tags:
  - storage
  - packaging
  - docs
  - okf
sources:
  - id: verify-ora-wheel
    resource: tools/scripts/verify_oracle_wheel.py
    title: "Reference standalone wheel payload, sdist, and isolated-venv fail-loud verifier"
  - id: makefile
    resource: Makefile
    title: "Companion wheel isolation check targets"
  - id: release-yml
    resource: .github/workflows/release.yaml
    title: "Release workflow test_wheels namespace and isolation assertions"
  - id: docs-storage
    resource: docs/usage/storage.md
    title: "Object storage user guide"
  - id: docs-parquet
    resource: docs/usage/storage-parquet-and-orm.md
    title: "Parquet, Arrow, and ORM storage user guide"
  - id: okf-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: "Object storage OKF chapter"
  - id: okf-limitations
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: "Object storage limitations OKF chapter"
---

# Chapter 03: Companion Wheel Payload Isolation, User Docs, and Storage OKF Sync

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `Makefile` verifies wheel isolation for `duckdb`, `mysql`, `mssql`, `oracle`, and `postgres`, leaving `vertebra-storage`, `vertebra-bigquery`, `vertebra-spanner`, and `vertebra-bundler` without local `*-wheel-check` gates [^makefile], while `.github/workflows/release.yaml` omits `SyncObjectStore` and `Bundler` from its `_vertebra` non-leakage assertions [^release-yml]. Storage user docs and OKF chapters do not yet describe content-integrity receipts, verified reads, direct Arrow streaming, or presign/Django caveats [^docs-storage] [^okf-limitations].
- **Target Architecture**: `tools/scripts/verify_storage_wheel.py` (`--package {storage,bigquery,spanner,bundler}`) validates wheel archive layout, sdist contents, and isolated-venv `MissingDependencyError` contracts [^verify-ora-wheel], wired into `Makefile` (`{storage,bigquery,spanner,bundler}-wheel-check`) and `release.yaml`. User docs, changelog, `object-storage.md` (`<= 200L`), `object-storage-limitations.md` (`<= 200L`), and `research/upstream-sqlspec-direct-arrow-write.md` record all shipped contracts [^docs-parquet] [^okf-storage].
- **Scope & Boundaries**: Covers `R5` (`G-12`) and `R6` across `tools/scripts/`, `Makefile`, `release.yaml`, `docs/`, and `.agents/bundles/` (`3` compact tasks).
<!-- truth: end -->

## 1. Specification, Context & Provenance

Every optional Rust PyO3 extension (`vertebra-storage`, `vertebra-bigquery`, `vertebra-spanner`, `vertebra-bundler`) ships as an isolated PEP 420 companion wheel containing only its single native library (`vertebra/_<pkg>*.{so,pyd,dylib}`) and type stub (`vertebra/_<pkg>.pyi`) with zero `vertebra/*.py` files [^verify-ora-wheel] [^release-yml]. Authoring one parameterized verifier (`tools/scripts/verify_storage_wheel.py`) closes `G-12` across all four companion wheels simultaneously without duplicating 400-line scripts, while `t02` and `t03` synchronize public user documentation, `object-storage.md` (`<= 200L`), `object-storage-limitations.md` (`<= 200L`), and the upstream SQLSpec direct Arrow write proposal [^okf-storage] [^okf-limitations].

## 2. Requirements, Concrete Contracts & Architecture

```python
# tools/scripts/verify_storage_wheel.py
def verify_companion_wheel_contents(wheel_path: Path, *, package: Literal["storage", "bigquery", "spanner", "bundler"]) -> None: ...
def verify_companion_sdist_contents(sdist_path: Path, *, package: Literal["storage", "bigquery", "spanner", "bundler"]) -> None: ...
def verify_isolated_venv_contract(core_wheel: Path, companion_wheel: Path, *, package: Literal["storage", "bigquery", "spanner", "bundler"]) -> None: ...
def main(argv: Sequence[str] | None = None) -> int: ...
```

- **Wheel Isolation & Release Contracts (`t01`)**:
  - `verify_storage_wheel.py`: defaults to `--package storage` and accepts `--package {storage,bigquery,spanner,bundler}` (or `--all`) plus optional `--core-wheel`, `--companion-wheel`, and `--companion-sdist` paths; verifies `vertebra_core-*.whl` excludes optional `vertebra/_<pkg>*` binaries/stubs, verifies `vertebra_<pkg>-*.whl` contains solely `vertebra/_<pkg>.pyi` + `1` `vertebra/_<pkg>*.{so,pyd,dylib}` (`GIL_USED is False`), verifies `vertebra_<pkg>-*.tar.gz` contains `Cargo.toml`, `src/lib.rs`, and `vertebra/_<pkg>.pyi`, and verifies an outside-checkout isolated venv raises `MissingDependencyError` (`vertebra-core[<pkg>]`) before installing the companion wheel and imports cleanly afterward.
  - `Makefile`: adds PHONY targets `storage-wheel-check`, `bigquery-wheel-check`, `spanner-wheel-check`, and `bundler-wheel-check` invoking `tools/scripts/verify_storage_wheel.py --package <pkg>`.
  - `.github/workflows/release.yaml`: adds `assert not hasattr(_vertebra, "SyncObjectStore")` and `assert not hasattr(_vertebra, "Bundler")` to `test_wheels`.
- **Docs, OKF (`<= 200L`), & Upstream SQLSpec Note (`t02`–`t03`)**:
  - `docs/usage/storage.md`, `docs/usage/storage-parquet-and-orm.md`, and `docs/changelog/index.md` (omitting redundant version-bump entries) document CRC32C/MD5/SHA-256 receipts, verified reads, `vertebra.typing.IntegrityError`, create-only S3/B2/R2 copy, `supports_direct_arrow_write`, and companion wheel checks.
  - `object-storage.md` (`<= 200L`) and `object-storage-limitations.md` (`<= 200L`, `<= 40`-line truth block) document offline `verify_presigned_url` `NotImplementedError` on `gcs`/`azure`/`oci`, `DjangoStorage.get_created_time` / `get_accessed_time` `NotImplementedError`, non-atomic cloud `rename`, final-poll stream verification, and SQLSpec `<= 0.71.1` pipeline buffering, accompanied by `research/upstream-sqlspec-direct-arrow-write.md` (promoted to `.agents/bundles/research/upstream-sqlspec-direct-arrow-write.md` before `/flow:archive`).

## 3. Documentation & OKF Knowledge Plan

- `docs/usage/storage.md`, `docs/usage/storage-parquet-and-orm.md`, `docs/changelog/index.md` (`t02`).
- `.agents/bundles/knowledge/domains/storage/object-storage.md` (`<= 200L`), `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` (`<= 200L`), and `.agents/bundles/specs/storage-integrity-and-streaming/research/upstream-sqlspec-direct-arrow-write.md` (`t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R5` (`G-12`: `storage`/`bigquery`/`spanner`/`bundler` wheel checks) | `t01` | `tools/scripts/verify_storage_wheel.py`, `Makefile`, `.github/workflows/release.yaml` | `pytest test_optional_driver_wheels.py` & `make -n storage-wheel-check bigquery-wheel-check spanner-wheel-check bundler-wheel-check` |
| `R6` (User docs & changelog sync) | `t02` | `docs/usage/{storage,storage-parquet-and-orm}.md`, `docs/changelog/index.md` | `make docs` |
| `R6` (`object-storage*.md <= 200L` & upstream SQLSpec note) | `t03` | `.agents/bundles/knowledge/domains/storage/{object-storage,object-storage-limitations}.md`, `research/upstream-sqlspec-direct-arrow-write.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-companion-wheel-isolation-checks.md](tasks/t01-companion-wheel-isolation-checks.md) — Create `tools/scripts/verify_storage_wheel.py` (`storage`, `bigquery`, `spanner`, `bundler`) and wire `*-wheel-check` targets into `Makefile` and `.github/workflows/release.yaml`.
- [ ] [t02-storage-user-docs-and-changelog.md](tasks/t02-storage-user-docs-and-changelog.md) — Update `docs/usage/storage.md`, `docs/usage/storage-parquet-and-orm.md`, and `docs/changelog/index.md`.
- [ ] [t03-storage-okf-limitations-and-upstream-note.md](tasks/t03-storage-okf-limitations-and-upstream-note.md) — Synchronize `object-storage.md` (`<= 200L`), `object-storage-limitations.md` (`<= 200L`), and `research/upstream-sqlspec-direct-arrow-write.md`.

## 6. Current State & Remaining Plan Snapshot

- **Fast Chapter Gate (`< 45s`)**:
  `uv run --no-sync ruff check tools/scripts/verify_storage_wheel.py && uv run --no-sync mypy tools/scripts/verify_storage_wheel.py && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q && make docs && make review-check`

[^verify-ora-wheel]: `tools/scripts/verify_oracle_wheel.py` — Reference standalone wheel payload, sdist, and isolated-venv fail-loud verifier.
[^makefile]: `Makefile` — Companion wheel isolation check targets.
[^release-yml]: `.github/workflows/release.yaml` — Release workflow `test_wheels` namespace and isolation assertions.
[^docs-storage]: `docs/usage/storage.md` — Object storage user guide.
[^docs-parquet]: `docs/usage/storage-parquet-and-orm.md` — Parquet, Arrow, and ORM storage user guide.
[^okf-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — Object storage OKF chapter.
[^okf-limitations]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Object storage limitations OKF chapter.
