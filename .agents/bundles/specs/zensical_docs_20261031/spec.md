---
type: spec
id: zensical_docs_20261031
title: "Zensical Documentation Pipeline and Polyglot Reference Site"
description: "Adopt Zensical with static Griffe Python and PyO3 stub API docs, first-party CLI/postbuild/verification scripts, Litestar Gold branding, and GitHub Pages CI."
status: stable
state: active
tags:
  - docs
  - zensical
  - mkdocstrings
  - griffe
  - branding
  - ci
created_at: "2026-10-31T00:56:00Z"
updated_at: "2026-10-31T02:15:00Z"
verification:
  strategy: tdd
  commands:
    - "uv run pytest src/py/tests/unit/tools src/py/tests/unit/docs -q"
    - "make docs"
    - "make review-check"
---

# Zensical Documentation Pipeline and Polyglot Reference Site

## Problem Statement

Vertebra currently lacks a documentation build system, API reference site, and GitHub Pages deployment pipeline:
- There is no `zensical.toml`, `docs/stylesheets/extra.css`, `docs/assets/`, `tools/docs/`, or `.github/workflows/docs.yml`.
- `pyproject.toml` has no `doc` dependency group and lacks `exclude = ["*.md"]` under `[tool.ruff.format]` (which is required so Ruff `0.17+` preview formatting does not corrupt `--8<-- "path"` snippet inclusions into `--8 < --"path"`).
- `.pre-commit-config.yaml` still runs legacy `sphinx-lint` (`v1.0.2`) and lacks `types_or: [python, pyi, jupyter]` on `ruff-format`.
- `docs/` contains only `docs/changelog.rst` and two stale pre-refactor Markdown notes (`docs/backpressure.md`, `docs/parallelism.md`), while `README.md` still references pre-rename identifiers (`VertebraApp`, `vertebra-ipc`, `vtb js codegen`).

## Architectural Goals & Simplicity Principles

1. **Minimal Dependency Group (`pyproject.toml`)**:
   - Declare only `zensical>=0.0.71`, `mkdocstrings-python>=2.0.3`, and `pymdown-extensions>=10.16` in `[dependency-groups] doc` (and include `{ include-group = "doc" }` in `dev`).
   - Avoid experimental or unmaintained plugins (`mkdocs-click`, `mkdocstrings-typescript`, `griffe-typedoc`, `griffe-fieldz`).
2. **Zero-Compile Polyglot API Reference**:
   - **Python & PyO3 Native Stubs**: Configure `paths = ["src/py"]` and `find_stubs_package = true` in `zensical.toml` so Griffe statically parses both pure-Python modules and PEP 561 `.pyi` stubs (`_vertebra.pyi`, `_bundler.pyi`, `_storage.pyi`) in milliseconds without compiling Cargo/PyO3 extensions in docs CI.
   - **`msgspec.Struct` & `TypedDict` Tables**: Override `tools/docs/templates/python/material/{attribute.html.jinja,summary/attributes.html.jinja,docstring/attributes.html.jinja}` to render `msgspec.Struct` and `TypedDict` classes as compact 3-column tables (`Name` | `Type` | `Description`) rather than individual `<h4>` attribute cards.
   - **`vtb` Click/`rich-click` CLI**: Generate `docs/reference/cli.md` deterministically via `tools/docs/generate_cli_docs.py` (~200 LOC pure Python) with `--check` validation in `make docs` and CI.
   - **`@vertebra/client` TypeScript SDK**: Author a curated Markdown reference (`docs/reference/typescript-client.md`) covering the root index (`src/ts/vertebra-client/src/index.ts`) and all 14 subpath exports in `src/ts/vertebra-client/package.json`, enforced by an export-sync check in `tools/docs/verify_docs_artifacts.py`.
   - **Rust Workspace Crates**: Document internal architecture in `docs/architecture/` and provide an optional `make docs-rust` target (`cargo doc --manifest-path src/rs/Cargo.toml --workspace --no-deps`) copied to `site/rustdoc/` by `tools/docs/postbuild.py` when present.
3. **First-Party Build & Verification Pipeline (`tools/docs/`)**:
   - `scan_public_docstrings.py` -> `generate_cli_docs.py --check` -> `zensical build --clean` -> `postbuild.py` (`llms.txt`, `llms-full.txt`, legacy `.html` redirects, `<pre>`/`<textarea>`-safe HTML whitespace compaction, root brand asset sync) -> `verify_docs_artifacts.py` (`objects.inv`, borderless `logo.svg`/`favicon.svg`, `BANNED_PHRASES`, `<= 120 KB`/`<= 180 KB` page budgets, `<= 14` TOC items, internal `#anchor` validation, and `@vertebra/client` export sync).
4. **Litestar Ecosystem Branding (`docs/assets/` & `docs/stylesheets/extra.css`)**:
   - Install the Vertebra branding pack (`logo.svg`, `favicon.svg` borderless `#EDB641` monoline mark; single-line `1.28x` `banner-inline-{dark,light}.svg` for `README.md`; `og-social-{dark,light}.png`; multi-size favicons and `site.webmanifest`) and Litestar Gold (`#EDB641`) + Navy (`#202235`) Zensical styling.

## Scope & Boundaries

- **In Scope**:
  - `pyproject.toml`, `.pre-commit-config.yaml`, `Makefile`, `README.md`, `zensical.toml`
  - `tools/docs/**` and unit tests in `src/py/tests/unit/tools/` and `src/py/tests/unit/docs/`
  - `docs/**` (assets, stylesheets, 7-tab Diataxis documentation, runnable `docs/examples/` scripts)
  - Public docstring/stub cleanups in `src/py/vertebra/**` surfaced by `scan_public_docstrings.py`
  - `.github/workflows/ci.yml`, `.github/workflows/docs.yml`, `AGENTS.md`, and `.agents/bundles/knowledge/workflow.md`
- **Out of Scope**:
  - Modifying Rust runtime logic (`src/rs/`) or TypeScript runtime implementation (`src/ts/vertebra-client/src/`).

## Task Breakdown

- [x] [T1: Dependencies, Ruff Markdown Protection, Pre-Commit & Makefile Targets](tasks/T1.md) — Configure `pyproject.toml` `doc` group and Ruff format exclusions, remove `sphinx-lint` in `.pre-commit-config.yaml`, and add `docs*` targets to `Makefile`.
- [x] [T2: First-Party Documentation Tooling, Griffe Templates & Unit Tests](tasks/T2.md) — Implement `tools/docs/{scan_public_docstrings,generate_cli_docs,postbuild,verify_docs_artifacts}.py`, custom Griffe Jinja templates, and unit tests in `src/py/tests/unit/tools/`.
- [x] [T3: Branding Assets, Zensical Config, Stylesheet & Landing Page](tasks/T3.md) — Populate `docs/assets/`, author `zensical.toml`, `docs/stylesheets/extra.css`, `docs/index.md`, and update `README.md`.
- [x] [T4: Getting Started, Usage Guides, Architecture & Changelog](tasks/T4.md) — Author `docs/getting-started/`, `docs/usage/`, `docs/architecture/`, and `docs/changelog/index.md`, replacing stale `docs/backpressure.md`, `docs/parallelism.md`, and `docs/changelog.rst`.
- [x] [T5: Polyglot API Reference (`docs/reference/`)](tasks/T5.md) — Author Python, PyO3 `.pyi` stub, `@vertebra/client` TypeScript, and generated `vtb` CLI reference pages under `docs/reference/`.
- [x] [T6: Runnable Examples (`docs/examples/`) & Example Verification Suite](tasks/T6.md) — Create runnable snippet-sliced examples in `docs/examples/`, `docs/examples/index.md`, and `src/py/tests/unit/docs/test_examples.py`.
- [x] [T7: CI/CD Workflows (`ci.yml`, `docs.yml`) & OKF Knowledge Sync](tasks/T7.md) — Add `build-docs` to `.github/workflows/ci.yml`, create `.github/workflows/docs.yml`, update `AGENTS.md` and `.agents/bundles/knowledge/workflow.md`, and run end-to-end verification.
