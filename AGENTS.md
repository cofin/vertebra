# Vertebra — Agent Instructions

Vertebra is a Python web framework and server powered by a Rust runtime (`vertebra-core`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-db`, `vertebra-mysql`, `vertebra-bigquery`, `vertebra-mssql`, `vertebra-spanner`, `vertebra-duckdb`, `vertebra-oracle`, `vertebra-postgres`, `vertebra-sqlite`, `vertebra-redis`, `vertebra-valkey`) and a TypeScript SDK (`@vertebra/client`), supporting Python 3.12–3.15 and free-threaded 3.14t/3.15t.

## Flow Operational Rule

When a repository has `.agents/`, read its configured root (`.agents/setup-state.json`; in linked `git worktree` checkouts, reuse the symlinked `.venv` via `uv run --no-sync` without running `make install`), bundle index ([`.agents/bundles/index.md`](.agents/bundles/index.md)), active specification (`specs/<flow_id>/spec.md`, plus `chapters/index.md` and `chapters/<ch_id>/spec.md` when `has_chapters: true`), authoritative task worksheet (`tasks/<id>-<slug>.md`), and applicable nested knowledge chapters before acting. Operational skills resolve only from `.agents/skills/` and paired subagents from `.agents/agents/`.

Route through the `flow` skill and one matching lifecycle skill. Refine plans until every worksheet is executable, execute one chapter per `/flow:implement` batch validating once with the `< 60s` Fast Chapter Gate (reserving full suites for `/flow:finish`), follow the selected worksheet without improvising, and route contradictions through revise/refine. Rewrite revised specifications, chapter catalogs, and worksheets in place in present tense without historical transcripts, standalone `---` horizontal rules, or frontmatter in subdirectory `index.md` files, and keep Markdown checklist markers (`[x] [<sha>]`, `[~]`, `[ ]`, `[!]`, `[-]`) synchronized with task `state` (`completed`, `in_progress`, `pending`, `blocked`, `deferred`). Apply direct Markdown frontmatter state synchronization and reconcile task-first.

Close and archive completed specs on the working branch without waiting for branch merge; when scope grows beyond the active spec, finish the current spec and open a follow-up spec via `/flow:plan`. Before deleting any spec during archive, curate evidence-backed OKF chapters across nested domain, architecture, pattern, testing, and gotcha folders ([`knowledge/index.md`](.agents/bundles/knowledge/index.md)). Keep `specs/` and `research/` lazy (only materialized when active specs or research briefs exist).

<!-- project-customization: start -->
Write direct, zero-slop prose and code: avoid AI puffery, negative parallelisms, formulaic transitions, and inline `#` comments in Python (use Google-style docstrings and PEP 585/604/695 types). Proactively fix adjacent lint, typing, or dead-code issues in touched in-scope files and suggest the exact canonical `/flow:<action>` command next.

Use `structured-choice-v1` for unresolved decisions (`1` question for dependent forks, `2–3` orthogonal leaf questions once on a settled frontier) through a compatible native tool or sequential-text fallback. Run correctness review and then fresh quality review before finish/archive.

Commits remain local unless the user explicitly asks for delivery. Never create, move, force-update, or delete Git tags. Never commit or push directly to `main`; all changes must be made on a dedicated feature/fix branch with signed conventional commits and submitted via pull request.
<!-- project-customization: end -->

## Canonical Bundle Entry Points

- [Bundle Index](.agents/bundles/index.md) — OKF v0.2 root index for product, nested knowledge, and lazy specs/research
- [Product Definition](.agents/bundles/product/product.md) — Product vision, runtime topologies, and protocol capabilities
- [Product Guidelines](.agents/bundles/product/product-guidelines.md) — Clean-break domain naming rules, hot-path invariants, and module layout
- [Tech Stack](.agents/bundles/product/tech-stack.md) — Python 3.12–3.15 and 3.14t/3.15t, Rust crates, and TypeScript client stack
- [Project Workflow](.agents/bundles/knowledge/workflow.md) — Canonical `Makefile` commands, polyglot rebuild cycle, verification strategies, and Flow execution loop
- [Knowledge Index](.agents/bundles/knowledge/index.md) — Nested architectural (`architecture/{runtime,ipc,http,worker,frontend}/`), domain (`domains/{framework,server,ipc,http,frontend,storage,data,telemetry,extensions,agent}/`), pattern (`patterns/{python,rust,typescript}/`), testing (`testing/`), gotcha (`gotchas/`), and decision (`decisions/`) chapters
- [Skills and Agents Index](.agents/skills/index.md) — Engineering domain skills, review skills, and paired reviewer/specialist subagents

## Quick Verification Commands

```bash
make setup-env   # Configure public uv, bun/npm, and cargo registries on hosts whose mirrors replace them; no-op elsewhere
make install     # Fresh setup: destroy .venv + clean, setup-env, pin Python 3.12, uv sync --all-extras --dev --group doc, bun install/build, make dev
make dev         # Build non-extension Rust crates and install debug PyO3 extensions (_vertebra, _bundler, _storage, _mysql, _bigquery, _mssql, _spanner, _duckdb, _oracle, _postgres, _sqlite, _redis, _valkey)
make lint        # Auto-fix TS (Biome), Python (Ruff), Rust (rustfmt + Clippy --features tls,http3,compression,gcp), and run tsc --noEmit + Mypy + Pyright + check_extras + check_package_metadata + 13-extension fail-loud stubtest
make lint-all    # Read-only CI lint, tsc --noEmit, Mypy, Pyright, check_extras, check_package_metadata, Clippy (--features tls,http3,compression,gcp), and 13-extension fail-loud stubtest gate across TS, Python, and Rust
make review-check # Optional local check for review capability manifests, skills, agents, and privacy constraints
make review-route # Route changed files to owning review areas and shards (BASE=origin/main)
make rs-test     # Remove /dev/shm/test-* and /dev/shm/*vertebra*, then run Rust workspace tests (--features tls,http3,compression,gcp) + non-http3 fail-loud check
make py-test     # Build debug extensions (make dev), verify mypy.stubtest across every installed extension, and run the Python pytest suite
make sqlite-test # Run vertebra-sqlite Rust tests, stubtest, and Python unit/conformance suites (make sqlite-wheel-check verifies wheel payload isolation)
make duckdb-test # Run vertebra-duckdb Rust tests, stubtest, and Python unit/conformance suites (make duckdb-wheel-check verifies wheel payload isolation)
make mysql-test  # Run vertebra-mysql Rust tests, stubtest, and Python MySQL suites (make mysql-wheel-check verifies wheel isolation)
make mssql-test  # Run vertebra-mssql Rust tests, stubtest, and Python MSSQL suites (make mssql-wheel-check verifies wheel isolation)
make oracle-test # Run vertebra-oracle Rust unit tests, _oracle stubtest, and Python Oracle adapter, context/notify/queue, and native suites (make oracle-wheel-check verifies wheel isolation)
make postgres-test # Run vertebra-postgres Rust tests and _postgres stubtest (make postgres-wheel-check verifies wheel and sdist isolation)
make redis-test  # Run vertebra-redis Rust unit tests, stubtest, and Python Redis, RESP3, and queue-backend/channel suites (make redis-wheel-check verifies wheel isolation)
make valkey-test # Run vertebra-valkey Rust unit tests, stubtest, and Python Valkey, RESP3, and queue-backend/channel suites (make valkey-wheel-check verifies wheel isolation)
make db-conformance # Run live database conformance suite (ENGINE=<duckdb|mysql|mariadb|mssql|oracle|redis|sqlite|valkey> [VERSION=<v>] required)
make postgres-bench-check # Run Criterion postgres_native test mode, script lint/mypy, and comparator unit suite
make core-wheel-check # Build relocatable vertebra-server + vertebra-core wheel and smoke test vtb run (plus {storage,bigquery,spanner,bundler}-wheel-check for 13 total wheel checks)
make ts-test     # Run the TypeScript Vitest suite with isolated localstorage
make test        # Run all Rust, Python, and TypeScript test suites
make coverage    # Run pytest with coverage (coverage.xml, htmlcov/)
make check       # Full verification gate: make lint + make test + make coverage
make docs        # Scan docstrings, check CLI docs, build Zensical site, run postbuild, and verify artifacts
make docs-serve  # Build docs and serve Zensical documentation locally at http://127.0.0.1:8000
make docs-check  # Build docs (`make docs`) and verify live `zensical serve` responds with HTTP 200
make docs-clean  # Remove site/ and .cache/ documentation artifacts
make docs-rust   # Build Rust workspace rustdoc and copy into site/rustdoc/
make build       # Release build: TS client, non-extension Rust crates, release maturin develop across extensions, uv build
make build-pgo   # Two-phase Rust PGO instrumentation and profile-use build (or PROFDATA=<path>)
make bench       # make build, then tools.benchmarks (TARGETS=vtb, SCENARIOS=default by default)
make bench-all   # make build, then tools.benchmarks across all targets and full scenarios
make bench-ft    # Build .venv-ft + src/rs/target-ft and benchmark free-threaded Python (FT_PYTHON=3.15t fallback 3.14t)
make bench-report # Show latest benchmark report (make bench-diff compares latest run vs previous)
make rs-bench    # Run Rust Criterion microbenchmarks across core and http
make bench-micro # Run Python microbenchmarks across hot-path primitives and warm import times
make bench-baseline # Record baseline performance metrics (NAME=pre-refactor by default)
make bench-check # Compare current benchmarks against baseline and enforce regression thresholds
make release     # On a non-main branch: check package metadata, bump versions (bump=patch|minor|major, or no bump to finalize a pre-release), relock uv/cargo, build docs, print next steps (make pre-release version=X.Y.Z-alpha.N for pre-releases; release.yaml blocks git pins and tag/version mismatches)
make upgrade     # Upgrade uv, cargo, bun, and pre-commit dependencies
```
