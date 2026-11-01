---
name: flow-memory-keeper
description: Use at task, phase, flow, sync, archive, finish, revise, or failure checkpoints to keep Flow specs clean, capture learnings and failures, elevate durable patterns, and refine this skill with project-specific nuances
---

# Flow Memory Keeper

## Purpose

This project-local skill prevents Flow work from drifting into stale specs, missing learnings, or half-archived folders.

Use it whenever work is completed, paused, blocked, revised, synced, or archived.

<workflow>

## Mandatory Outcomes

Before claiming a task, phase, or flow is complete:

1. Ensure `spec.md` is readable and current through the normal Flow sync process.
2. Capture concrete learnings in the flow's `learnings.md`.
3. Capture failures, false starts, blockers, and recovery notes when they would help a future session.
4. If the user had to repeat a correction or showed frustration that something was forgotten, flag that as a workflow gap and capture it explicitly.
5. Elevate durable patterns to evidence-backed `.agents/bundles/knowledge/patterns/<topic>.md` chapters.
6. Update chapters recursively under `.agents/bundles/knowledge/` when the current-state knowledge base changed, preserving project-shaped relative paths, indexes, and links.
7. If the flow is complete, archive it cleanly and leave the active spec area uncluttered.
8. Capture validated repo-native commands and verification workflows in `.agents/bundles/knowledge/workflow.md` when they were discovered or corrected during the work.

## Completion Protocol

### Task or Phase Completion

1. Append a concise entry to `.agents/bundles/specs/<flow_id>/learnings.md`:
   - what changed
   - why it changed
   - files touched
   - commands or checks that mattered
   - canonical repo commands that future agents should reuse
   - gotchas, failures, and recoveries worth remembering
   - any repeated user correction or frustration that revealed a missing default, checklist item, or workflow rule
2. Move reusable guidance into a topic-specific `.agents/bundles/knowledge/patterns/<topic>.md` chapter.
3. If the work changed architecture, conventions, tooling, operational behavior, or canonical project commands, recursively locate and update the relevant chapter under `.agents/bundles/knowledge/` at its existing project-shaped relative path; keep `.agents/bundles/knowledge/workflow.md` as the stable command default.
4. Promote repeated user corrections or frustration into an obvious durable rule instead of leaving it as a one-off note.
5. Run the normal Flow sync step so `spec.md` reflects the latest state.

### Failure Capture

Capture failure notes whenever one of these happened:

- a hypothesis was wrong
- a command or tool failed in a non-obvious way
- a harness integration behaved differently than expected
- a backend migration exposed a hidden assumption
- a repeated reminder from the user revealed a workflow gap
- the user seemed frustrated that something obvious was forgotten again

Failure notes belong in `learnings.md`, but keep them short and reusable. Focus on what future agents should avoid or check earlier.

### Flow Archive

When a flow is finished:

1. Confirm final reconciliation and verification ran through the Flow state contract.
2. Ensure `learnings.md` is not missing critical discoveries.
3. Elevate the stable patterns and current-state knowledge before archiving.
4. Apply the archive state operation: synthesize current-state knowledge, append
   one bundle-log entry, then delete the completed spec directory recorded by
   the approved archive inventory. Do not create a resident archive tree.
5. Confirm the completed spec directory is absent and no stale scratch file
   remains in the active specs area.

</workflow>

<guardrails>

## Skill Refinement Loop

This skill should improve over time.

When you discover a repeated project nuance, add or revise a short rule below instead of keeping the lesson only in session memory.

Only keep:

- durable project-specific rules
- recurring harness quirks
- recurring workflow misses
- validated project-native command wrappers that should become defaults
- user corrections or frustration that clearly indicate a missing default or checklist item
- archive/sync/learnings habits that proved necessary

Do not keep:

- one-off narrative history
- temporary debugging logs
- duplicate rules already captured elsewhere

</guardrails>

<validation>

Before claiming a task, phase, or flow is complete, verify:

- [ ] `spec.md` was synced through the normal Flow process
- [ ] `learnings.md` captures the durable lessons, failures, and recoveries
- [ ] reusable guidance was elevated to an evidence-backed `.agents/bundles/knowledge/patterns/<topic>.md` chapter when appropriate
- [ ] nested chapters, indexes, and links under `.agents/bundles/knowledge/` reflect current-state knowledge without flattening
- [ ] repeated user corrections or frustration were promoted into an explicit rule when applicable
- [ ] canonical repo commands and verification flows were captured when they were learned or corrected
- [ ] completed flows were contracted without a resident archive tree or stale active-spec clutter

</validation>

## Project Nuances

- This consumer skill is installed at `.agents/skills/flow-memory-keeper/SKILL.md`;
  `.agents/skills/` is the only operational project-skill authority.
- **Native Extension & Server Binary Rebuilds**: Whenever Rust code in `src/rs/vertebra-core/`, `src/rs/vertebra-py/`, `src/rs/vertebra-bundler/`, `src/rs/vertebra-storage/`, or `src/rs/vertebra-http/` changes, rebuild via `make dev` (or `make build` for release benchmarks), which passes `PYO3_PYTHON="$(CURDIR)/.venv/bin/python"` to `cargo build` so `vertebra-server` links against the `.venv` Python interpreter. After running `maturin develop --release`, always pass `--no-sync` to subsequent `uv run --no-sync ...` invocations so `uv` does not overwrite the release `.so` wheel in `.venv` with an editable debug rebuild.
- **Strict Python Code Organization**: Enforce zero inline `#` comments (use Google-style docstrings exclusively), top-level imports only, PEP 585/604/695 typing, and strict class member ordering (`class attributes -> __init__ -> @property -> public methods -> private _... methods`).
- **PyO3 GIL & FFI Discipline**: Always declare `#[pymodule(gil_used = false)]` on PyO3 modules and wrap blocking waits, synchronous file reads, and CPU-heavy Rust operations in `py.detach(|| ...)`. Never allocate per-request Tokio runtimes inside PyO3 methods.
- **Pre-Commit Secret Hygiene**: Never use `ya29.*` prefixes in test tokens or fixtures (`git-secrets` blocks Google OAuth2 access token prefixes); use `"mock-oauth-token"` instead.
- **Canonical Test Files Only**: Never create standalone `test_*remediation*.py` or `test_*audit*.py` files; add tests to the canonical domain test file under `src/py/tests/unit/`, `src/py/tests/integration/`, `src/rs/*/tests/`, or `src/ts/vertebra-client/tests/`.
- **Branch-Local Spec Contraction**: Keep `.agents/bundles/specs/` and `.agents/bundles/research/` lazy; before deleting a completed spec during `/flow:finish` / `/flow:archive`, synthesize 5-dimension OKF v0.2 knowledge (`domains/`, `architecture/`, `patterns/`, `testing/`, `gotchas/`), verify all `sources:` and markdown links resolve on the current branch, and append the archive summary to `.agents/bundles/log.md`.
- **Git & Remote Discipline**: Only interact with the `origin` remote for PRs; always sign commits (`commit.gpgsign=true`); never commit directly to `main`.
- **PyPI Distribution vs Import Namespace**: The PyPI distribution package in `pyproject.toml` is `vertebra-core` (`[project] name = "vertebra-core"`), while the Python import package remains `vertebra` (`[tool.maturin] python-packages = ["vertebra"]`, `module-name = "vertebra._vertebra"`). Always pass `"vertebra-core"` to `importlib.metadata.version(...)`, `@click.version_option(package_name=...)`, and scaffolded dependency lists (`vtb init`), and keep `src/rs/vertebra-*/**/*` in `[tool.maturin] include` so `uv build` / `maturin sdist` includes all workspace members.
- **Zensical Documentation & Griffe `msgspec.Struct` Hygiene**: Keep `zensical.toml` pointed at `cofin/vertebra` (`https://cofin.github.io/vertebra/` and `https://github.com/cofin/vertebra`) without `"navigation.indexes"` (so `.md-sidebar--primary` never renders empty on single-page tabs or hides section `Overview` links), keep `Server & Runtime`, `Databases`, and `Object Storage` in separate navigation groups, document every public `msgspec.Struct` and `.pyi` stub field with a PEP 257 docstring, override Zensical's `(0, 2, 1)` `div.doc-contents:not(.first)` selector with `.md-typeset div.doc-contents:not(.first)` (`--vertebra-doc-guide-border`), and verify with `make docs` (`<= 120 KB` reference HTML, `<= 14` secondary TOC items, zero empty `Attributes:` rows).
- **CI Environment Isolation (`build-docs`, `test-python`, `coverage`)**: `build-docs` runs with `--only-group doc` (no compiled `_vertebra` extension or optional web frameworks), so modules imported by `vertebra` or `vertebra.cli` must guard `vertebra._vertebra` and optional framework base classes via `import_optional(...)`. Keep `[tool.coverage.run] source = ["src/py/vertebra"]` and `[tool.coverage.report] ignore_errors = true` in `pyproject.toml` so Litestar's dynamic `dto_transfer_function_*` code objects do not fail `coverage xml`.
