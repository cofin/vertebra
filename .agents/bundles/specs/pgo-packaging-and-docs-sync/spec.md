---
type: Spec
flow_id: pgo-packaging-and-docs-sync
title: Two-Phase Rust PGO Pipeline, Python 3.15 Packaging, and Documentation Sync
state: completed
plan_revision: 1
plan_commit: null
state_revision: 10
current_task: null
last_operation: 20261025T160855Z-flow-executor-complete-spec-00
operation_targets: []
last_verified_checkpoint: ffa4d20
created_at: 2026-10-25T15:21:20Z
updated_at: 2026-10-25T16:08:55Z
description: Wire the two-phase Rust PGO instrumentation and profile-use pipeline across vertebra-http and all PyO3 extensions, extend standalone/onefile packaging and vtb build for Python 3.15/3.15t and SQLAlchemy dialects, and synchronize AGENTS.md and OKF v0.2 documentation.
tags: [feature, pgo, packaging, python-315, docs]
parent_prd: null
research: []
---

# Flow: Two-Phase Rust PGO Pipeline, Python 3.15 Packaging, and Documentation Sync

**Flow ID:** `pgo-packaging-and-docs-sync`

## Specification

### Code Analysis Summary

- `src/py/vertebra/build/packaging/pgo.py` and `tools/pgo/rust_pgo.py`: Duplicate 250 lines of Rust PGO helpers, ignore `CARGO_TARGET_DIR` (`Makefile:26`), build only `_vertebra` instead of all three PyO3 extensions (`_vertebra`, `_bundler`, `_storage`), skip `validate_profdata` in `build_optimized*`, lack an end-to-end two-phase generate/train/merge/validate/use pipeline and CLI entrypoint (`python -m tools.pgo.rust_pgo`), and leave `tools/pgo` outside `Makefile` `ruff`/`mypy` gates and `.profiles/` outside `.gitignore` / `make clean`.
- `src/py/vertebra/build/packaging/{targets,standalone,onefile}.py`, `src/py/vertebra/cli/build.py`, and `pyproject.toml`: `BuildTarget` lacks `3.15` / `3.15t` in `_PATCH_VERSIONS`, `linux-x86_64-musl` / `linux-aarch64-musl` in `KNOWN_TARGETS`, and `SUPPORTED_PYTHON_VERSIONS` validation; `StandaloneBuilder` and `vtb build` lack `--profdata` / `--server-binary` staging and full `PythonBuildInfo` manifest enrichment; `pgo.py:255` and `onefile.py:80` carry unused symbol aliases (`resolve_llvm_profdata = find_llvm_profdata`, `extract_archive = extract_to`); `pyproject.toml:22-30` `[project.entry-points."sqlalchemy.dialects"]` still points to renamed `*VtbDialect` classes and omits `mssql` and `duckdb`.
- `AGENTS.md` and `.agents/bundles/**`: `AGENTS.md` omits `3.15`/`3.15t`, `domains/agent`, `make build-pgo`, `make bench-all`, and `make bench-report`, while four OKF chapters contain minor symbol drift (`Cargo.toml` `[profile.release]`, `BatchDispatchPlan.sync_native`, `state.health_endpoint.try_intercept(req_path)`, and `drive_asgi_batch(single_root_asgi.dispatch_app, batch, ...)`).

### Relevant Patterns

- `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`: Zero inline `#` comments, top-level grouped imports, explicit `__all__`, PEP 585/604/695 typing, `msgspec.Struct(..., frozen=True, gc=False)` value objects, and zero backward-compatibility aliases (`PY-07`, `PY-08`).
- `.agents/bundles/knowledge/patterns/python/serialization-and-utils.md`: Route all JSON formatting and decoding through `vertebra.utils.serializers` (`format_json`, `decode_json`) and optional imports through `vertebra.utils.module_loader` (`import_optional`).

### Requirements

1. **R1 — Two-Phase Rust PGO Pipeline & Lint Gate**: Deduplicate `tools/pgo/rust_pgo.py` onto `vertebra.build.packaging.pgo`, honor `CARGO_TARGET_DIR`, build all three PyO3 extensions (`vertebra-py` via root `pyproject.toml`, `vertebra-bundler` and `vertebra-storage` via `-m`), validate `.profdata` via `validate_profdata`, expose `run_pgo_pipeline` (`PgoPipelineResult`) and `python -m tools.pgo.rust_pgo` (`make build-pgo`), ignore/clean `.profiles/` and `*.profraw`/`*.profdata`, and add `tools/pgo` (`tools/pgo/__init__.py`) to `ruff` and `mypy --strict`.
2. **R2 — Python `3.15`/`3.15t` Packaging, Binary/PGO Staging & Dialect Entry Points**: Support `SUPPORTED_PYTHON_VERSIONS = ("3.12", "3.13", "3.14", "3.14t", "3.15", "3.15t")` and Linux `musl` targets in `BuildTarget` and `vtb build`, add `--profdata` and `--server-binary` staging (`bin/<name>`, `VERTEBRA_SERVER_BINARY` bootstrap, `python_build` and `.profdata` manifest metadata) to `StandaloneBuilder` and `vtb build`, remove `resolve_llvm_profdata` and `extract_archive` aliases, and fix all 12 `[project.entry-points."sqlalchemy.dialects"]` entries in `pyproject.toml`.
3. **R3 — `AGENTS.md` & OKF v0.2 Documentation Precision Sync**: Update `AGENTS.md` and the affected OKF v0.2 product and knowledge chapters so every command, Python runtime target, PGO workflow, and code symbol matches the repository verbatim with zero broken links or missing `sources:` paths.

### Requirement-to-Task Traceability

| Requirement | Task | Verification Strategy | Target Evidence |
| --- | --- | --- | --- |
| R1 | `1.1` | `behavior_tdd` | `uv run pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q` + `make lint-all` |
| R2 | `1.2` | `behavior_tdd` | `uv run pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q` + `make lint-all` |
| R3 | `2.1` | `documentation_validation` | `make review-check` + OKF bundle audit + `git diff --check` |

## Implementation Plan

### Phase 1: Two-Phase Rust PGO Pipeline & Python 3.15 Packaging
- [x] Task 1.1: Wire two-phase Rust PGO pipeline, multi-extension CARGO_TARGET_DIR builds, and tools/pgo lint gates (`23d0be5`)
- [x] Task 1.2: Add Python 3.15/3.15t and musl packaging targets, PGO/binary staging, and SQLAlchemy dialect entry points (`774d920`)

### Phase 2: AGENTS.md & OKF v0.2 Documentation Sync
- [x] Task 2.1: Synchronize AGENTS.md quick commands and OKF v0.2 bundle symbol and PGO references (`2dffe05`)

## Continuity Snapshot

- **Active Flow / Lifecycle**: `pgo-packaging-and-docs-sync` (`completed`)
- **Current Task & Claimant**: `none (all tasks closed)`
- **Last Verified Checkpoint**: `ffa4d20`
- **Decisions**:
  - Deduplicate `tools/pgo/rust_pgo.py` onto `vertebra.build.packaging.pgo` while keeping `tools/pgo/python_pgo.py` self-contained so monkeypatch unit tests against both modules continue to exercise independent sysconfig/JIT probes.
  - Omit `-m` when invoking `maturin develop` for `vertebra-py` from repository root so root `pyproject.toml` (`module-name = "vertebra._vertebra"`) applies, while passing `-m` for `vertebra-bundler` and `vertebra-storage` which carry crate-local `pyproject.toml` files.
  - Remove unused `resolve_llvm_profdata = find_llvm_profdata` and `OnefileBuilder.extract_archive = extract_to` symbol aliases.
- **Recent Discoveries**:
  - `src/py/vertebra/build/packaging/pgo.py` needed `Path(__file__).resolve().parents[5]` (not `parents[4]`) after relocation under `build/packaging/`, and `run_pgo_pipeline` / `build_optimized_extensions` now validate `.profdata` once per batch rather than spawning `llvm-profdata show` per crate.
  - `vtb build` gained `--entrypoint`, `--profdata`, and `--server-binary`, `BuildTarget` validates `SUPPORTED_PYTHON_VERSIONS` (`3.12`–`3.15`, `3.14t`, `3.15t`) and resolves Linux `musl` triples, and all 12 `pyproject.toml` SQLAlchemy dialect entry points now resolve to `{Postgres,Oracle,Mysql,Sqlite,Mssql,Duckdb}Dialect`.
  - Synchronized `AGENTS.md` and 10 OKF v0.2 bundle chapters (`2dffe05`) with zero broken relative links and zero missing `sources:` paths across all 62 bundle markdown files.
- **Blockers & Unblock Conditions**: `none`
- **Next Exact Step**: `Archive completed spec via disposable worktree (/flow:archive)`
- **Plan Identity**: `plan_revision: 1`, `plan_commit: null`
- **State Identity**: `revision: 10`, `last_operation: 20261025T160855Z-flow-executor-complete-spec-00`, `operation_targets: []`
- **Relevant Rule / Knowledge Paths**:
  - `AGENTS.md`
  - `.agents/bundles/knowledge/workflow.md`
  - `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md`
  - `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`
  - `.agents/bundles/knowledge/patterns/python/serialization-and-utils.md`

## Verification Evidence

- Phase 1 Checkpoint (`774d920`): `uv run pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q` (`13 passed in 0.73s`), `make lint-all` (`passed`).
- Phase 2 Checkpoint (`2dffe05`): `make review-check` (`passed`), OKF bundle audit (`62 files, 0 errors`), `git diff --check` (`passed`).
- Flow Completion Checkpoint (`ffa4d20`): `make lint-all` (`passed`), `uv run pytest -q` (`860 passed`), `code-reviewer` (`Ready to proceed`, `0` findings on `2f110d8..ffa4d20`), `quality-reviewer` (`APPROVED`, `0` findings on `2f110d8..ffa4d20`).
