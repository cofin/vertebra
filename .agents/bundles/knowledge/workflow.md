---
type: Guide
title: Project Workflow
description: Canonical polyglot build, Fast Chapter Gate, full release gate, and Flow OKF v0.2 chapter-batch execution rules for Vertebra.
status: stable
tags:
  - workflow
  - commands
  - verification
  - testing
  - ci
generated:
  by: flow/0.30.1
  at: "2026-11-06T18:28:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-27T22:10:00Z"
    commit: f3b624a2
  - by: agent:flow-executor
    at: "2026-11-17T08:15:00Z"
    commit: 5a833832
  - by: agent:flow-archive
    at: "2026-11-16T15:16:00Z"
    commit: 11020d84
  - by: agent:flow-archive
    at: "2026-11-14T22:49:00Z"
    commit: ff9f78cb
sources:
  - id: src-makefile
    resource: Makefile
    title: "Canonical polyglot build, test, lint, docs, and benchmark targets"
  - id: src-tools-scripts-setup-env-sh
    resource: tools/scripts/setup-env.sh
    title: "Repository tooling module (tools/scripts/setup-env.sh)"
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: "Python package metadata, dependencies, and tool configuration"
  - id: src-cargo-toml
    resource: src/rs/Cargo.toml
    title: "Rust crate manifest (src/rs/Cargo.toml)"
  - id: src-package-json
    resource: src/ts/vertebra-client/package.json
    title: "TypeScript package manifest (src/ts/vertebra-client/package.json)"
---

# Project Workflow

<!-- truth: start -->
- Specifications (`specs/<flow_id>/spec.md`), chapter specs (`chapters/<ch_id>/spec.md`), and task worksheets (`tasks/<id>-<slug>.md`) are the sole authority for Flow state and commits.
- Batch edits across the active chapter (`1..4` tasks) in one pass, then validate once with the **Fast Chapter Gate** (`< 60s`: `make lint-all` plus targeted unit tests or `cargo test -p <crate>`); reserve **Full Release Gate** runs (`make check`, `> 60s`, or Docker-backed suites) for `/flow:finish` or CI.
- Rebuild touched PyO3 extensions via `make dev` (or `uv run --no-sync maturin develop --uv -m src/rs/<crate>/Cargo.toml`) after any Rust edit before running `pytest` or `stubtest`.
- Test through public seams in canonical module test files; never mock the system under test, write tautological assertions, or create standalone `test_*remediation*.py` / `test_*audit*.py` files.
- In linked `git worktree` checkouts, reuse the symlinked `.venv` via `uv run --no-sync` without running `make install`; edit worktree `.agents/` directly when `ignore_policy` is `shared`.
- Subdirectory `index.md` files and `log.md` never carry YAML frontmatter, bundle bodies never use standalone `---` rules, chapters/tasks are never marked `deferred` without `deferred_by: "user"`, Git tags are never created or mutated, and all work lands on dedicated branches with signed conventional commits (`commit.gpgsign=true`).
<!-- truth: end -->

## Canonical Verification Tiers & `Makefile` Commands

[`tools/scripts/setup-env.sh`](../../../tools/scripts/setup-env.sh)[^src-tools-scripts-setup-env-sh] (`make setup-env`, called by `make install` and `make upgrade`) restores public PyPI, npm, and crates.io registries on hosts whose mirrors override them and is a no-op elsewhere.

```bash
# 1. Bootstrap & Native PyO3 Extension Rebuild (primary checkout; symlinked in linked worktrees)
make setup-env                                                        # Configure public registries where needed; no-op elsewhere
make install                                                          # destroy + clean + setup-env, pin Python 3.12, uv sync --all-extras --dev --group doc, ts-install/build, dev
make dev                                                              # cargo build non-extension crates + maturin develop across all 13 PyO3 extensions
make dev-ext EXT=postgres                                             # Rebuild one native extension in debug mode (core|bundler|storage|mysql|bigquery|mssql|spanner|duckdb|oracle|postgres|sqlite|redis|valkey)
make upgrade                                                          # uv lock --upgrade + uv sync, cargo update, bun update, pre-commit autoupdate
make clean                                                            # Remove Python/Rust/TS build artifacts and caches (keeps .venv)
make release bump=patch                                               # Non-main branch only: check metadata, bump version, relock uv/cargo, build docs
make release                                                          # Finalize the current X.Y.Z-(alpha|beta|rc).N pre-release as X.Y.Z
make pre-release version=0.4.0-alpha.1                                # Start or advance a pre-release

# 2. Fast Chapter Gate (< 60s — run once per chapter batch) & Focused Suites
make lint-all                                                         # Read-only CI lint: Biome, tsc, Ruff, Mypy, Pyright, check_extras, check_package_metadata, 13-ext stubtest, rustfmt, Clippy
make lint                                                             # Auto-fix Biome, Ruff, rustfmt + Clippy, then run tsc, Mypy, Pyright, check_extras, check_package_metadata, and 13-ext stubtest
make lint-ts                                                          # Read-only TypeScript Biome + tsc check (also make lint-py, make lint-rs)
make stubtest                                                         # Run 13-extension fail-loud mypy.stubtest against already-built extensions
make review-check                                                     # Validate review capability manifests, skills, agents, OKF bundles, and privacy constraints
make review-route                                                     # Route changed files to owning review areas and shards (BASE=origin/main)
uv run --no-sync pytest src/py/tests/unit/server/test_runner.py -q    # Targeted Python unit test file
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core         # Targeted Rust crate test suite
make ts-test                                                          # Vitest suite with isolated --localstorage-file

# 3. Scoped Engine, Wheel & Documentation Gates
make sqlite-test     # vertebra-sqlite Rust tests, stubtest, and Python suites (make sqlite-wheel-check for wheel isolation)
make duckdb-test     # vertebra-duckdb Rust tests, stubtest, and Python suites (make duckdb-wheel-check)
make mysql-test      # vertebra-mysql Rust tests, stubtest, and Python suites (make mysql-wheel-check)
make mssql-test      # vertebra-mssql Rust tests, stubtest, and Python suites (make mssql-wheel-check)
make oracle-test     # vertebra-oracle Rust tests, stubtest, and Python Oracle suites (make oracle-wheel-check)
make postgres-test   # vertebra-postgres Rust tests and _postgres stubtest (make postgres-wheel-check)
make redis-test      # vertebra-redis Rust tests, stubtest, and Python Redis/queue suites (make redis-wheel-check)
make valkey-test     # vertebra-valkey Rust tests, stubtest, and Python Valkey/queue suites (make valkey-wheel-check)
make core-wheel-check # Build relocatable vertebra-server + vertebra-core wheel and smoke test vtb run (plus {storage,bigquery,spanner,bundler}-wheel-check)
make db-conformance  # Live database conformance suite (ENGINE=<duckdb|mysql|mariadb|mssql|oracle|redis|sqlite|valkey> [VERSION=<v>])
make postgres-bench-check # Criterion postgres_native test mode + Ruff/Mypy + comparator tests (make postgres-bench for live run)
make docs            # Scan public docstrings, check diagrams and CLI docs, build Zensical site, run postbuild, and verify artifacts
make docs-check      # make docs + live zensical serve HTTP 200 check (make docs-serve, make docs-clean, make docs-rust)

# 4. Full Release Gate (run once at /flow:finish or CI, never per-step) & Benchmarks
make rs-test                                                          # Clean /dev/shm/{test-*,*vertebra*}, then cargo test --workspace --features tls,http3,compression,gcp
make py-test                                                          # make dev, mypy.stubtest across every extension, then pytest src/py/tests -v
make test                                                             # rs-test + py-test + ts-test
make coverage                                                         # pytest --cov -> coverage.xml + htmlcov/
make check                                                            # lint + test + coverage
make build                                                            # ts-build, release cargo build, maturin develop -r across extensions, uv build (make build-pgo for PGO)
make bench-tools                                                      # Check and install missing benchmark load drivers (oha, h2load, ghz, k6)
make bench                                                            # make build + bench-run (also bench-run, bench-all, bench-all-run, bench-ft, rs-bench, bench-micro, bench-baseline, bench-check, bench-report)
```

## Polyglot Rebuild & Hygiene Rules

- **Rust to Python FFI Rebuild**: Use `uv run --no-sync` to preserve installed extras in an existing `.venv` or linked `git worktree`. After editing any `.rs` file in `vertebra-core`, `vertebra-db`, `vertebra-resp`, or any of the 13 extension crates, rebuild the touched extension (or run `make dev`) before running `pytest`, `pyright`, or `mypy.stubtest`.
- **Wheel Isolation & Release Guards**: [`pyproject.toml`](../../../pyproject.toml)[^src-pyproject-toml] publishes `vertebra-core` (`module-name = "vertebra._vertebra"`). Each of the 12 companion packages pins `vertebra-core==<version>`, includes only its own `.pyi` stub, and excludes `vertebra/*.py` files (verified by `make <name>-wheel-check` and `tools/scripts/check_package_metadata.py`). `.github/workflows/release.yaml` runs `validate_release_manifest` (failing if `[tool.uv.sources]` carries git pins or tags mismatch versions), checks artifacts with `twine check --strict`, builds sdists for core plus all 12 companions, and uploads to PyPI only on `release` events.
- **TypeScript Bundle Sync**: `make ts-build` compiles all 17 `@vertebra/client` entrypoints ([`package.json`](../../../src/ts/vertebra-client/package.json)[^src-package-json]) and copies `dist/` into `src/py/vertebra/server/_js/bundle/`.
- **Shared-Memory Cleanup**: `make rs-test` clears `/dev/shm/test-*` and `/dev/shm/*vertebra*`. Parallel agents must run targeted `cargo test -p <crate>` commands instead so concurrent SHM segments are not unlinked.
- **Dependency Group Isolation**: Keep upstream `duckdb>=1.0; python_version < '3.14'` in `[dependency-groups].duckdb` and `bench`, HTTP benchmark tools in `bench`, PostgreSQL comparators in `postgres-bench`, and Zensical packages in `doc` ([`pyproject.toml`](../../../pyproject.toml)[^src-pyproject-toml]). `tools/scripts/check_extras.py` enforces optional-import parity with `[project.optional-dependencies]`.
- **Docs, Secrets & Git Config**: Keep `navigation.indexes` disabled in `zensical.toml`; use `"mock-oauth-token"` in GCS/IAM tests (`git-secrets` blocks real OAuth2 prefixes); keep `extensions.worktreeConfig` and `core.hooksPath` unset in `.git/config`.

## GitHub Actions CI Tiers & Caching

- **Single Native Build per Python Version**: `.github/workflows/ci.yml` builds wheels once in `build-native` via `tools/scripts/build_ci_wheels.py build` (`maturin build --strip --auditwheel skip --profile dev`) and shares them (`retention-days: 1`) across `test-python` (`-n auto --dist loadscope`), `verify-wheels`, and database conformance jobs (`uv sync --frozen --no-install-local`).
- **Reusable Actions & Profiles**: `.github/actions/setup-rust-build` configures fast linkers, `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_INCREMENTAL=0`, optional remote `sccache`, and `Swatinem/rust-cache@v2` (`save-if` on `main`); `.github/actions/setup-python-env` handles Python setup. `src/rs/Cargo.toml` defines `[profile.ci]` (`opt-level = 2`, `lto = false`, `codegen-units = 16`, `strip = "symbols"`) and `[profile.release]` (`lto = "thin"`, `codegen-units = 4`).
- **PR vs. Full Matrix**: Lightweight lint, TypeScript, docs, and cleanup jobs run on `ubuntu-slim`. Pull requests run a lean tier (`3.12`, `3.14t`, `3.15t`; PostgreSQL `14` and `18`; representative DB rows). Add label `full-ci` for the full Python/database/macOS matrix or `macos` for macOS Rust tests (`push` to `main` and nightly runs execute the full matrix). `.github/workflows/cache-cleanup.yml` prunes closed-PR caches.

## Direct Markdown Continuity & Chapter-Batch Loop

1. Resolve `.agents/setup-state.json` (`bundle_root: ".agents/bundles"`, `ignore_policy: "shared"`), read [`.agents/bundles/index.md`](../index.md), and load only the active chapter's `spec.md` and `tasks/*.md`.
2. Run preflight checks (`chapter_dependencies_closed_and_reviewed`, `chapter_planning_status_complete`, `dependencies_closed`, `worksheet_complete`, `verification_strategy_declared`, `plan_identity_matches`, `target_symbols_exist`).
3. Implement the chapter's `1..4` vertical-slice tasks in one cohesive pass so shared files are edited once.
4. Run the **Fast Chapter Gate** (`exit 0`), commit exact task-owned code/test paths first as signed commit `<CODE_SHA>`, and update `tasks/*.md`, `chapters/<ch_id>/spec.md`, `chapters/index.md`, root `spec.md`, and affected docs in present tense before running Zero-Context Chapter Review (`@code-reviewer` + `@quality-reviewer`).
5. Run `/flow:learn` to reconcile drift and refresh `knowledge/**`, `.agents/skills/`, and `.agents/agents/`. Archive completed specs on the working branch via `/flow:archive` (`make review-check` rejects resident `state: completed` root specs).

Provenance sources: [`Makefile`](../../../Makefile)[^src-makefile], [`src/rs/Cargo.toml`](../../../src/rs/Cargo.toml)[^src-cargo-toml].

[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
[^src-tools-scripts-setup-env-sh]: `tools/scripts/setup-env.sh` — Repository tooling module (tools/scripts/setup-env.sh).
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
[^src-cargo-toml]: `src/rs/Cargo.toml` — Rust crate manifest (src/rs/Cargo.toml).
[^src-package-json]: `src/ts/vertebra-client/package.json` — TypeScript package manifest (src/ts/vertebra-client/package.json).
