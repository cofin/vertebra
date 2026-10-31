# Vertebra — Agent Instructions

Vertebra is a Python-first, high-performance polyglot application backbone combining Python 3.12–3.15 and free-threaded 3.14t/3.15t developer ergonomics with a Rust 2024 runtime (`vertebra-core`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-db`) and a full-stack TypeScript SDK (`@vertebra/client`).

## Flow Operational Rule

When a repository has `.agents/`, read its configured root, bundle index, active spec, authoritative task worksheet, and applicable recursively nested knowledge before acting. Operational skills resolve only from `.agents/skills/`.

Route through the `flow` skill and exactly one lifecycle skill. Refine plans until every worksheet is executable, follow the selected worksheet without improvising, and route contradictions through revise/refine. Rewrite revised worksheets in place in present tense without historical transcripts, and keep Markdown checklist markers (`[x]`, `[~]`, `[ ]`) synchronized with task `state`. Apply direct Markdown frontmatter state synchronization and reconcile task-first.

Close and archive completed specs on the working branch without waiting for branch merge; when scope grows beyond the active spec, finish the current spec and open a follow-up spec via `/flow:plan`. Before deleting any spec during archive, curate evidence-backed OKF chapters across nested domain, architecture, pattern, testing, and gotcha folders ([`knowledge/index.md`](.agents/bundles/knowledge/index.md)). Keep `specs/` and `research/` lazy (only materialized when active specs or research briefs exist).

Write direct, zero-slop prose and code: avoid AI puffery, negative parallelisms, formulaic transitions, and inline `#` comments in Python (use Google-style docstrings and PEP 585/604/695 types). Proactively fix adjacent lint, typing, or dead-code issues in touched in-scope files and suggest the exact canonical `/flow:<action>` command next.

Use `structured-choice-v1` for unresolved decisions: one decision at a time, only through a currently allowed compatible native tool or the equivalent sequential-text fallback. Run correctness review and then the mandatory fresh quality review before finish/archive.

Commits remain local unless the user explicitly asks for delivery. Never create, move, force-update, or delete Git tags. Never commit or push directly to `main`; all changes must be made on a dedicated feature/fix branch with signed conventional commits and submitted via pull request.

## Canonical Bundle Entry Points

- [Bundle Index](.agents/bundles/index.md) — OKF v0.2 root index for product, nested knowledge, and lazy specs/research
- [Product Definition](.agents/bundles/product/product.md) — Product vision, runtime topologies, and protocol capabilities
- [Product Guidelines](.agents/bundles/product/product-guidelines.md) — Clean-break domain naming rules, hot-path invariants, and module layout
- [Tech Stack](.agents/bundles/product/tech-stack.md) — Python 3.12–3.15 and 3.14t/3.15t, Rust 2024 crates, and TypeScript client stack
- [Project Workflow](.agents/bundles/knowledge/workflow.md) — Canonical `Makefile` commands, polyglot rebuild cycle, verification strategies, and Flow execution loop
- [Knowledge Index](.agents/bundles/knowledge/index.md) — Nested architectural (`architecture/{runtime,ipc,http,worker,frontend}/`), domain (`domains/{framework,server,ipc,http,frontend,storage,data,telemetry,extensions,agent}/`), pattern (`patterns/{python,rust,typescript}/`), testing (`testing/`), and gotcha (`gotchas/`) chapters

## Quick Verification Commands

```bash
make setup-env   # Configure public uv, bun/npm, and cargo registries on hosts whose mirrors replace them; no-op elsewhere
make install     # Fresh setup: destroy .venv + clean, setup-env, pin Python 3.12, uv sync --all-extras --dev, bun install/build, make dev
make dev         # Build non-extension Rust crates and install debug PyO3 extensions (_vertebra, _bundler, _storage)
make lint        # Auto-fix TS (Biome), Python (Ruff), Rust (rustfmt + Clippy), and run Mypy + stubtest + review-check
make lint-all    # Read-only CI lint, type-check, stubtest, and review-check gate across TypeScript, Python, and Rust
make review-check # Validate review capability manifests, skills, agents, and privacy constraints
make review-route # Route changed files to owning review areas and shards (BASE=origin/main)
make rs-test     # Remove /dev/shm/test-* and /dev/shm/*vertebra*, then run Rust workspace tests (--features tls,compression)
make py-test     # Build debug extensions (make dev), verify mypy.stubtest, and run the Python pytest suite
make ts-test     # Run the TypeScript Vitest suite with isolated localstorage
make test        # Run all Rust, Python, and TypeScript test suites
make coverage    # Run pytest with coverage (coverage.xml, htmlcov/)
make check       # Full verification gate: make lint + make test + make coverage
make docs        # Generate CLI docs, build Zensical site, run postbuild, and verify artifacts
make docs-serve  # Serve Zensical documentation locally at http://localhost:8000
make docs-check  # Verify public docstrings, CLI docs sync, and built site artifacts
make docs-clean  # Remove site/ and .cache/ documentation artifacts
make docs-rust   # Build Rust workspace rustdoc into site/rustdoc/
make build       # Release build: TS client, Rust crates, release maturin develop of the extensions, uv build
make build-pgo   # Two-phase Rust PGO instrumentation and profile-use build (or PROFDATA=<path>)
make bench       # make build, then tools.benchmarks (TARGETS=vtb, SCENARIOS=default by default)
make bench-all   # make build, then tools.benchmarks across all targets and full scenarios
make bench-ft    # Build .venv-ft + src/rs/target-ft and benchmark free-threaded Python (FT_PYTHON=3.15t fallback 3.14t)
make bench-report # Show latest benchmark report (make bench-diff compares latest run vs previous)
make rs-bench    # Run Rust Criterion microbenchmarks across core and http
make bench-micro # Run Python microbenchmarks across hot-path primitives and warm import times
make bench-baseline # Record baseline performance metrics (NAME=pre-refactor by default)
make bench-check # Compare current benchmarks against baseline and enforce regression thresholds
make upgrade     # Upgrade uv, cargo, bun, and pre-commit dependencies
```
