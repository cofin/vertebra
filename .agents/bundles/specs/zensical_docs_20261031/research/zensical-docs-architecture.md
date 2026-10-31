---
type: research
id: "research:zensical-docs-architecture"
title: "Zensical Documentation Architecture and Polyglot Gap Analysis"
description: "Research findings from SQLSpec Zensical adoption, polyglot API reference alternatives, branding standardization, and CI pipeline design for Vertebra."
status: stable
tags:
  - docs
  - zensical
  - mkdocstrings
  - griffe
  - branding
sources:
  - resource: "pyproject.toml"
  - resource: "Makefile"
  - resource: ".pre-commit-config.yaml"
  - resource: ".github/workflows/ci.yml"
  - resource: "src/py/vertebra/__init__.py"
  - resource: "src/py/vertebra/cli/__init__.py"
  - resource: "src/ts/vertebra-client/package.json"
  - resource: "src/rs/Cargo.toml"
---

# Zensical Documentation Architecture and Polyglot Gap Analysis

## Research Question

How should Vertebra adopt Zensical (`zensical>=0.0.71`) using the proven conventions from `sqlspec` while covering Vertebra's polyglot surface (Python 3.12–3.15, PyO3 `.pyi` stubs, `msgspec.Struct` models, `vtb` Click/`rich-click` CLI, `@vertebra/client` TypeScript SDK, and Rust 2024 crates) with minimal dependencies and build complexity?

---

## 1. Core Zensical & Griffe Learnings from SQLSpec

### 1.1 Active Package Versions (Verified October 2026)

| Package | Version | Status & Role |
| :--- | :--- | :--- |
| `zensical` | `>=0.0.71` (`0.0.74`) | Rust-powered static site generator (`zensical-ui==0.0.41`) by the creators of Material for MkDocs (`abi3` wheels, Python `>=3.11`). |
| `mkdocstrings-python` | `>=2.0.3` (`2.0.10`) | Python API reference handler backed by `mkdocstrings>=1.0.8` and `griffelib>=2.4.1` static AST analysis. |
| `pymdown-extensions` | `>=10.16` | Provides `pymdownx.snippets` (`--8<--`), `superfences`, `tabbed`, and `highlight` extensions. |

### 1.2 Critical Configuration Invariants

1. **Inventory Placement (`mkdocstrings-python >= 2.0`)**:
   - `enable_inventory = true` must be declared under `[project.plugins.mkdocstrings]`, never under `[project.plugins.mkdocstrings.handlers.python]`. Placing it under `handlers.python` raises `TypeError: PythonConfig.__init__() got an unexpected keyword argument 'enable_inventory'`.
2. **External `objects.inv` URLs**:
   - `msgspec` moved its Sphinx inventory from `https://jcristharif.com/msgspec/objects.inv` (HTTP 404) to `https://msgspec.dev/objects.inv`.
   - Standard inventories for Vertebra: Python 3 (`https://docs.python.org/3/objects.inv`), `msgspec` (`https://msgspec.dev/objects.inv`), `litestar` (`https://docs.litestar.dev/latest/objects.inv`), `sqlspec` (`https://sqlspec.dev/objects.inv`), `anyio` (`https://anyio.readthedocs.io/en/stable/objects.inv`), and `click` (`https://click.palletsprojects.com/en/stable/objects.inv`).
3. **Footer Attribution**:
   - Set `generator = false` under `[project.extra]` in `zensical.toml` so `"Made with Zensical"` is omitted from rendered footers.
4. **Ruff `0.17+` Markdown Snippet Corruption**:
   - Ruff `0.17+` (`preview = true`) and `ruff-pre-commit` (`v0.17.1+`) format Markdown code blocks by default and rewrite PyMdown snippet directives `--8<-- "path"` into `--8 < --"path"` (parsing `--8` as unary minus arithmetic).
   - Fix requires three coordinated safeguards:
     - `exclude = ["*.md"]` under `[tool.ruff.format]` in `pyproject.toml`.
     - `types_or: [python, pyi, jupyter]` on `ruff-format` in `.pre-commit-config.yaml`.
     - `"--8<--"` and `"--8 < --"` in `tools/docs/verify_docs_artifacts.py` `BANNED_PHRASES`.
5. **Page Weight & Right-Sidebar TOC Budgets**:
   - Reference pages must stay `<= 120 KB` of compacted HTML (`<= 180 KB` for narrative guides) and `<= 14` secondary TOC (`On this page`) entries.
   - Setting `show_symbol_type_toc = false`, `inherited_members = false`, `merge_init_into_class = false`, `heading_level = 3`, and overriding `tools/docs/templates/python/material/{attribute.html.jinja,summary/attributes.html.jinja,docstring/attributes.html.jinja}` keeps large classes compact.

---

## 2. Polyglot Gap Analysis & Actively Maintained Alternatives

### 2.1 Gap 1: PyO3 Native Extensions (`_vertebra`, `_bundler`, `_storage`) & Maturin Build Backend
- **Problem**: Compiling the 6-crate Rust workspace via `maturin` (`build-backend = "maturin"` in `pyproject.toml`) inside docs CI adds minutes of Rust build time and toolchain dependencies. Unlike `sqlspec` (which uses `hatchling`), running `uv sync --group doc` in `vertebra` without `--no-install-project` invokes `maturin` to compile `vertebra-py`.
- **Solution**:
  1. In `ci.yml` (`build-docs`) and `docs.yml`, run `uv sync --no-install-project --group doc` and set `PYTHONPATH=src/py:.` in `Makefile` (`docs` target) so `tools.docs.*` and `vertebra.cli:cli` resolve directly from `src/py/vertebra` with zero `maturin` or Cargo compilation.
  2. Griffe is a static AST analyzer. Setting `paths = ["src/py"]` and `find_stubs_package = true` in `zensical.toml` allows Griffe to parse `src/py/vertebra/_vertebra.pyi` (879 lines), `src/py/vertebra/_bundler.pyi` (135 lines), and `src/py/vertebra/_storage.pyi` (157 lines) directly alongside pure-Python modules in milliseconds.

### 2.2 Gap 2: `msgspec.Struct` Field Rendering & Docstring Validation
- **Problem**: Vertebra uses `msgspec.Struct(..., frozen=True, gc=False)` instead of `@dataclass` across all configuration, route, frame, and metadata types.
- **Evaluated Alternative (`griffe-fieldz 0.5.0`)**: Rejected. `griffe-fieldz` is in Beta (`0.5.0`), relies on runtime `fieldz` inspection, and breaks pure-static zero-compile guarantees.
- **Selected Solution**: Griffe's static AST visitor natively extracts annotated class attributes (`ast.AnnAssign`) and PEP 257 inline attribute docstrings on `msgspec.Struct` classes as `Attribute` members. Extending `tools/docs/templates/python/material/attribute.html.jinja` and `tools/docs/scan_public_docstrings.py` to match `Struct` and `msgspec.Struct` bases alongside `TypedDict` renders compact 3-column field tables (`Name` | `Type` | `Description`) with zero extra dependencies.

### 2.3 Gap 3: Click / `rich-click` CLI Documentation (`vtb`)
- **Problem**: `vtb` (`src/py/vertebra/cli/__init__.py`) uses `rich-click` with 4 top-level commands (`run`, `dev`, `init`, `build`) and 3 command groups (`js`, `mcp`, `workers`).
- **Evaluated Alternative (`mkdocs-click 0.9.0`)**: Rejected. `mkdocs-click` has had no release since April 2025, leaks literal ``Default: `Sentinel.UNSET` `` on Click 8.3+ (`mkdocs/mkdocs-click#95`), omits positional `click.Argument` tables, leaks raw Rich markup (`[dim]...[/]`), and emits flat `###` headings that exceed the `<= 14` TOC limit.
- **Selected Solution**: Port `tools/docs/generate_cli_docs.py` (~200 LOC pure Python) targeting `vertebra.cli:cli` (`vtb`), with `--check` enforced in `make docs` and CI.

### 2.4 Gap 4: TypeScript Client SDK (`@vertebra/client`)
- **Problem**: `@vertebra/client` (`src/ts/vertebra-client/package.json`) exports a root entry point (`src/ts/vertebra-client/src/index.ts`, 64 lines) and 14 subpath entry points (`./vite`, `./react`, `./vue`, `./svelte`, `./angular`, `./htmx`, `./store`, `./hmr`, `./live`, `./apex`, `./telemetry`, `./wasm`, `./arrow`, `./agent`).
- **Evaluated Alternatives**:
  - `mkdocstrings-typescript` (`0.1.1`) + `griffe-typedoc` (`0.1.1`): Rejected. Carries an explicit upstream warning (`"WARNING: Still in prototyping phase!"`), pulls in `pydantic`, requires multi-file `typedoc*.json` configs and pre-built `.d.ts` files, and shells out to Node/TypeDoc during `zensical build`.
  - `typedoc` (`0.28.20`) + `typedoc-plugin-markdown` (`4.14.1`): Rejected. Generates multi-file Markdown dumps that require Node build orchestration and violate Zensical's `<= 14` TOC budget.
- **Selected Solution**: Author a curated Markdown reference page (`docs/reference/typescript-client.md`) organized by subpath capability using `##` section headings and `####` symbol headings, backed by a deterministic export-sync check in `tools/docs/verify_docs_artifacts.py` verifying that every root export in `src/ts/vertebra-client/src/index.ts` and every subpath key in `src/ts/vertebra-client/package.json` is documented.

### 2.5 Gap 5: Rust Workspace Crates (`src/rs/Cargo.toml`)
- **Problem**: Vertebra has 6 Rust crates (`vertebra-core`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-db`).
- **Selected Solution**: Keep `make docs` pure-Python and fast by documenting the Python-facing native bridge via `.pyi` stubs (`docs/reference/native-stubs.md`) and deep-dive architecture guides (`docs/architecture/`). Provide an optional `make docs-rust` target (`cargo doc --manifest-path src/rs/Cargo.toml --workspace --no-deps`) whose output (`src/rs/target/doc`) is automatically copied to `site/rustdoc/` by `tools/docs/postbuild.py` when present.

---

## 3. Current Repository Gaps in Vertebra

1. **Missing Infrastructure**:
   - No `zensical.toml`, `docs/stylesheets/extra.css`, `docs/assets/`, `tools/docs/`, or `.github/workflows/docs.yml`.
   - No `doc` dependency group in `pyproject.toml`.
   - No `docs-clean`, `docs`, `docs-serve`, `docs-check`, or `docs-rust` targets in `Makefile`.
   - No `docs` path filter or `build-docs` job in `.github/workflows/ci.yml`.
2. **Legacy & Stale Files**:
   - `.pre-commit-config.yaml` still runs `sphinx-lint` (`v1.0.2`) and lacks `types_or: [python, pyi, jupyter]` on `ruff-format`.
   - `docs/changelog.rst` uses reStructuredText instead of Zensical Markdown (`docs/changelog/index.md`).
   - `docs/backpressure.md` and `docs/parallelism.md` contain pre-refactor notes referencing `_native.py` and library-only primitives.
   - `README.md` contains stale pre-rename references (`VertebraApp`, `vertebra-ipc`, `vtb js codegen`) and lacks the `<picture>` dark/light inline banner header.

---

## 4. Branding Standardization (`litestar-org/branding`)

- The Vertebra branding pack in `litestar-org/branding` (`assets/projects/vertebra/`) has been upgraded to the latest `VectorFont` + `librsvg` pipeline matching `sqlspec`:
  - **Borderless Monoline `logo.svg` & `favicon.svg`**: `256x256` Litestar Gold (`#EDB641`) monoline vertebral column (`stroke-width="14"`, `stroke-linecap="round"`, `stroke-linejoin="round"`) with `18px` inter-vertebral spacing and segmented spinal cord lines for crisp `16x16` / `32x32` rendering, with no outer rounded-rectangle badge box.
  - **Single-Line `1.28x` Inline Banners**: `banner-inline-dark.svg` and `banner-inline-light.svg` (`1600x388`, `viewBox="0 0 1200 290.999983"`) with single-line `VERTEBRA` wordmark (`82.0224px` cap height) and symmetric `52px` divider padding for `README.md`.
  - **Complete Favicon & OpenGraph Set**: `favicon.ico` (`16/32/48`), `favicon-{16x16,32x32,48x48}.png`, `apple-touch-icon.png` (`180x180`), `android-chrome-{192x192,512x512}.png`, `site.webmanifest`, and `og-social-{dark,light}.png` (`1280x640`).
