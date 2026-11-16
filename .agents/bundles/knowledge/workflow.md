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
  - by: agent:flow-archive
    at: "2026-11-14T22:49:00Z"
    commit: ff9f78cb
  - by: agent:flow-executor
    at: "2026-11-14T22:27:00Z"
    commit: ff9f78cb
  - by: agent:flow-setup
    at: "2026-11-14T18:43:00Z"
    commit: 1b191388
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
- Batch edits across the active chapter (`1..4` tasks) in a single warm pass so shared files are edited once, then validate once with the **Fast Chapter Gate** (`< 60s`: `make lint-all` + targeted unit test file or `cargo test -p <crate>`); reserve **Full Release Gate** runs (`make check`, `> 60s` or Docker-backed suites) for `/flow:finish` or CI.
- Rebuild PyO3 extensions (`_vertebra`, `_bundler`, `_storage`, `_mysql`, `_bigquery`, `_mssql`, `_spanner`, `_duckdb`, `_oracle`, `_postgres`, `_sqlite`) via `make dev` (or `uv run --no-sync maturin develop --uv -m src/rs/<crate>/Cargo.toml`) after any Rust edit before running `pytest` or `stubtest`.
- Test through confirmed public seams in existing canonical module test files; never mock the System Under Test, write tautological assertions, or create standalone `test_*remediation*.py` / `test_*audit*.py` files.
- In linked `git worktree` checkouts, reuse the symlinked `.venv` via `uv run --no-sync` without running `make install`; edit worktree `.agents/` directly when `ignore_policy` is `shared` (or the primary checkout's `.agents/` when `local-only`).
- Subdirectory `index.md` files and `log.md` never carry YAML frontmatter, Markdown bodies never use standalone `---` horizontal rules, chapters/tasks are never marked `deferred` without `deferred_by: "user"`, Git tags are never created or mutated, and all changes land on dedicated branches with signed conventional commits (`commit.gpgsign=true`).
<!-- truth: end -->

## Canonical Verification Tiers & `Makefile` Commands

[`tools/scripts/setup-env.sh`](../../../tools/scripts/setup-env.sh)[^src-tools-scripts-setup-env-sh] (`make setup-env`, invoked by `make install` and `make upgrade`) configures public PyPI, npm, and crates.io registries automatically on internal Linux workstations whose mirrors replace them and is a no-op elsewhere.

```bash
# 1. Bootstrap & Native PyO3 Extension Rebuild (primary checkout; symlinked in linked worktrees)
make setup-env                                                        # Configure public registries where needed; no-op elsewhere
make install                                                          # destroy + clean + setup-env, pin Python 3.12, uv sync --all-extras --dev --group doc, ts-install/build, dev
make dev                                                              # cargo build non-extension crates + maturin develop across _vertebra, _bundler, _storage, _mysql, _bigquery, _mssql, _spanner, _duckdb, _oracle, _postgres, _sqlite
make upgrade                                                          # uv lock --upgrade + uv sync, cargo update, bun update, pre-commit autoupdate
make clean                                                            # Remove Python/Rust/TS build artifacts and caches (keeps .venv)
make release bump=patch                                               # Synchronize versions across manifests via bump-my-version

# 2. Fast Chapter Gate (< 60s — run once per chapter batch) & Focused Suites
make lint-all                                                         # Read-only CI lint, Ruff, Mypy, stubtest, rustfmt --check, Clippy (--features tls,http3,compression,gcp), review-check
make lint                                                             # Auto-fix Biome, Ruff, rustfmt + Clippy (--features tls,http3,compression,gcp), then Mypy + stubtest + review-check
make review-check                                                     # Validate review capability manifests, skills, agents, OKF bundles, and privacy constraints
make review-route                                                     # Route changed files to owning review areas and shards (BASE=origin/main)
uv run --no-sync pytest src/py/tests/unit/server/test_runner.py -q    # Targeted Python unit test file
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core         # Targeted Rust crate test suite
make ts-test                                                          # Vitest suite with isolated --localstorage-file

# 3. Scoped Engine & Documentation Gates
make sqlite-test     # vertebra-sqlite Rust tests, stubtest, and Python unit/conformance suites (make sqlite-wheel-check verifies wheel isolation)
make duckdb-test     # vertebra-duckdb Rust tests, stubtest, and Python unit/conformance suites (make duckdb-wheel-check verifies wheel isolation)
make mysql-test      # vertebra-mysql Rust tests, stubtest, and Python MySQL suites (make mysql-wheel-check verifies wheel isolation)
make mssql-test      # vertebra-mssql Rust tests, stubtest, and Python MSSQL suites (make mssql-wheel-check verifies wheel isolation)
make oracle-test     # vertebra-oracle Rust tests, stubtest, and Python Oracle adapter + native suites (make oracle-wheel-check verifies wheel isolation)
make postgres-test   # vertebra-postgres Rust tests and _postgres stubtest (make postgres-wheel-check runs verify_postgres_wheel.py)
make db-conformance  # Live database conformance suite (ENGINE=<duckdb|mysql|mariadb|mssql|oracle|sqlite> [VERSION=<v>] required; zero-skip ConformanceGate)
make postgres-bench-check # PYTHONPATH="$(CURDIR)/src/py" cargo test -p vertebra-postgres --bench postgres_native --locked + Ruff/Mypy + comparator tests (make postgres-bench runs live + Criterion)
make docs            # scan_public_docstrings + generate_cli_docs --check + zensical build --clean + postbuild + verify_docs_artifacts
make docs-check      # make docs + live zensical serve HTTP 200 check (make docs-serve, make docs-clean, make docs-rust)

# 4. Full Release Gate (run once at /flow:finish or CI, never per-step) & Benchmarks
make rs-test                                                          # Clean /dev/shm/{test-*,*vertebra*}, then cargo test --workspace --features tls,http3,compression,gcp
make py-test                                                          # make dev, mypy.stubtest across every extension, then pytest src/py/tests -v
make test                                                             # rs-test + py-test + ts-test
make coverage                                                         # pytest --cov -> coverage.xml + htmlcov/
make check                                                            # lint + test + coverage
make build                                                            # ts-build, release cargo build, maturin develop -r across extensions, uv build (make build-pgo for two-phase PGO)
make bench                                                            # make build + tools.benchmarks (make bench-all, make bench-ft, make rs-bench, make bench-micro, make bench-baseline, make bench-check, make bench-report)
```

## Polyglot Rebuild & Hygiene Rules

1. **Rust to Python FFI Rebuild (`make dev`)**: Preserve installed extras with `uv run --no-sync` when verifying an existing `.venv` or linked `git worktree`. After any `.rs` edit in `vertebra-core`, `vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-db`, `vertebra-mysql`, `vertebra-bigquery`, `vertebra-mssql`, `vertebra-spanner`, `vertebra-duckdb`, `vertebra-oracle`, `vertebra-postgres`, or `vertebra-sqlite`, rebuild the touched extension (or run `make dev`) before `pytest` or `mypy.stubtest`.
2. **PyPI Distribution vs. Import Namespace**: [`pyproject.toml`](../../../pyproject.toml)[^src-pyproject-toml] publishes `vertebra-core` while exposing the `vertebra` import namespace (`module-name = "vertebra._vertebra"`). Companion extensions build isolated PEP 420 wheels without `vertebra/*.py` files.
3. **Codegen & Wheel-Bundled TypeScript Sync**: `make ts-build` compiles all 17 `@vertebra/client` entrypoints ([`package.json`](../../../src/ts/vertebra-client/package.json)[^src-package-json]) and copies `dist/` into `src/py/vertebra/server/_js/bundle/`.
4. **Shared-Memory Cleanup**: `make rs-test` clears `/dev/shm/test-*` and `/dev/shm/*vertebra*`; parallel review or chapter agents must run targeted `cargo test -p <crate>` commands rather than `make rs-test` so concurrent SHM segments are not unlinked.
5. **Dependency Group Isolation**: Keep third-party `duckdb>=1.1.0; python_version < '3.14'` strictly inside `[dependency-groups].bench` (`make install`, `make upgrade`, `make py-test`, and `make coverage` never sync `--group bench`), HTTP benchmark harnesses in `bench`, `psycopg`/`asyncpg` PostgreSQL benchmark comparators in `postgres-bench` (`python_version < '3.14'`), and Zensical packages in `doc` ([`pyproject.toml`](../../../pyproject.toml)[^src-pyproject-toml]).
6. **Documentation, Secret, & Git Hygiene**: Never enable `navigation.indexes` in `zensical.toml`; use `"mock-oauth-token"` in GCS/IAM tests (`git-secrets` blocks real OAuth2 prefixes); keep `extensions.worktreeConfig` and `core.hooksPath` unset in `.git/config`.

## Direct Markdown Continuity & Chapter-Batch Loop

1. Resolve `.agents/setup-state.json` (`bundle_root: ".agents/bundles"`, `ignore_policy: "shared"`), read [`.agents/bundles/index.md`](../index.md), and load only the active chapter's `spec.md` and `tasks/*.md`.
2. Run preflight (`chapter_dependencies_closed_and_reviewed`, `chapter_planning_status_complete`, `dependencies_closed`, `worksheet_complete`, `verification_strategy_declared`, `plan_identity_matches`, `target_symbols_exist`).
3. Execute the active chapter's `1..4` vertical-slice tasks (`behavior_tdd`, `regression_tdd`, `characterization`, `static_validation`, `documentation_validation`, or `integration_acceptance`) in one cohesive warm batch so shared files are edited once.
4. Run the **Fast Chapter Gate** (`exit 0`), stage exact task-owned code/test paths first to create signed commit `<CODE_SHA>`, and synchronize `tasks/*.md` (`state: completed`, `commit`, `last_verified_commit`), `chapters/<ch_id>/spec.md`, `chapters/index.md`, root `spec.md` (`[x] [<sha>]`), and affected OKF/docs in present tense before Zero-Context Chapter Review (`@code-reviewer` + `@quality-reviewer`, stamping `last_reviewed_commit`).
5. Run `/flow:learn` at any time to reconcile cross-session drift, sharpen domain glossaries and ADRs, and resynthesize `knowledge/**`, `.agents/skills/`, and `.agents/agents/` (plus `.codex/agents/`). Archive completed specs on the working branch via `/flow:archive` (`make review-check` rejects resident `state: completed` root specs).

Provenance sources: [`Makefile`](../../../Makefile)[^src-makefile], [`src/rs/Cargo.toml`](../../../src/rs/Cargo.toml)[^src-cargo-toml].

[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
[^src-tools-scripts-setup-env-sh]: `tools/scripts/setup-env.sh` — Repository tooling module (tools/scripts/setup-env.sh).
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
[^src-cargo-toml]: `src/rs/Cargo.toml` — Rust crate manifest (src/rs/Cargo.toml).
[^src-package-json]: `src/ts/vertebra-client/package.json` — TypeScript package manifest (src/ts/vertebra-client/package.json).
