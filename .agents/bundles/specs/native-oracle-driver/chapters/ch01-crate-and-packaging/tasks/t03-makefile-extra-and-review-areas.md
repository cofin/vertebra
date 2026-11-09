---
type: Task
id: "native-oracle-driver:ch01:t03"
chapter_id: ch01-crate-and-packaging
title: Wire root [oracle] extra, Makefile build/test targets, and review area paths
description: Add [project.optional-dependencies].oracle, uv.sources, and bumpversion entries in root pyproject.toml, wire vertebra-oracle into Makefile (dev, build, py-test, lint, lint-all, oracle-test, oracle-wheel-check), and register src/rs/vertebra-oracle/** and src/py/vertebra/_oracle.pyi under [areas.pyo3-bridge] in .agents/skills/review-core/references/areas.toml.
status: stable
state: completed
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-oracle-driver:ch01:t02"
read_set:
  - pyproject.toml
  - Makefile
  - .agents/skills/review-core/references/areas.toml
  - src/rs/vertebra-oracle/pyproject.toml
  - src/py/vertebra/_oracle.pyi
  - src/py/tests/unit/test_freethreaded_gil.py
files:
  - pyproject.toml
  - Makefile
  - .agents/skills/review-core/references/areas.toml
tests: []
estimated_loc: 95
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T03:12:15Z"
last_verified_commit: ff7c85db
verification_evidence:
  summary: "make oracle-test, make oracle-wheel-check, make review-check, and pyproject.toml/areas.toml assertions passed."
created_at: "2026-11-09T00:55:00Z"
updated_at: "2026-11-09T03:12:15Z"
commit: ff7c85db
sources:
  - id: root-pyproject
    resource: pyproject.toml
    title: Root vertebra-core optional dependency extras, uv.sources, and bumpversion config
  - id: root-makefile
    resource: Makefile
    title: Workspace dev, build, lint, lint-all, py-test, and wheel verification targets
  - id: review-areas
    resource: .agents/skills/review-core/references/areas.toml
    title: Review area path routing manifest for pyo3-bridge
  - id: gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false
    title: Free-threaded GIL declaration verification across native extension modules
tags:
  - oracle
  - packaging
  - makefile
  - review
---

# Task t03: Wire Root `[oracle]` Extra, `Makefile` Build/Test Targets, and Review Area Paths

## Objective & Scope Boundary

Register the `oracle = ["vertebra-oracle==0.3.0"]` optional dependency extra, `[tool.uv.sources]` entry, and `[[tool.bumpversion.files]]` blocks in root `pyproject.toml`, integrate `vertebra-oracle` into `Makefile` (`dev`, `build`, `py-test`, `lint`, `lint-all`, `oracle-test`, `oracle-wheel-check`), and register `src/rs/vertebra-oracle/**` alongside `src/py/vertebra/_oracle.pyi` under `[areas.pyo3-bridge]` in `.agents/skills/review-core/references/areas.toml` [^root-pyproject] [^root-makefile] [^review-areas] [^gil-test].

## Bounded Read Manifest

- `pyproject.toml` — `[project.optional-dependencies]`, `[tool.uv.sources]`, and `[[tool.bumpversion.files]]` sections [^root-pyproject].
- `Makefile` — `dev`, `build`, `py-test`, `lint`, and `lint-all` targets [^root-makefile].
- `.agents/skills/review-core/references/areas.toml` — `[areas.pyo3-bridge].paths` list [^review-areas].
- `src/rs/vertebra-oracle/pyproject.toml` — Optional wheel manifest from `ch01:t02`.
- `src/py/vertebra/_oracle.pyi` — Stub file from `ch01:t02`.
- `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded `GIL_USED = False` verification pattern [^gil-test].

## Symbol & Signature Contracts

- `pyproject.toml`:
  - `[project.optional-dependencies]`: add `oracle = ["vertebra-oracle==0.3.0"]` and append `"vertebra-oracle==0.3.0"` to `all`.
  - `[tool.uv.sources]`: add `vertebra-oracle = { path = "src/rs/vertebra-oracle", editable = true }` (and sync `uv.lock` if needed via `uv lock`).
  - `[[tool.bumpversion.files]]`: add entries for `pyproject.toml` (`search = 'vertebra-oracle=={current_version}'`, `replace = 'vertebra-oracle=={new_version}'`) and `src/rs/vertebra-oracle/pyproject.toml` (`search = 'version = "{current_version}"'`, `replace = 'version = "{new_version}"'`).
- `Makefile`:
  - `dev`: append `--exclude vertebra-oracle` to `cargo build --workspace` and add `@uv run maturin develop --uv -m src/rs/vertebra-oracle/Cargo.toml`.
  - `build`: append `--exclude vertebra-oracle` to `cargo build --workspace ... --release` and add `@uv run maturin develop --uv -r -m src/rs/vertebra-oracle/Cargo.toml`.
  - `py-test`: append `vertebra._oracle` to `@uv run python -m mypy.stubtest ...`.
  - `lint` and `lint-all`: append `"vertebra._oracle"` to both dynamic `stub_mods` tuples.
  - `.PHONY: oracle-test`: run `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked`, `uv run --no-sync python -m mypy.stubtest vertebra._oracle`, and `uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -v` (when present, or `src/py/tests/unit/test_freethreaded_gil.py -v` before `ch02:t02` creates `test_oracle_adapter.py`).
  - `.PHONY: oracle-wheel-check`: build `uv build src/rs/vertebra-oracle --wheel --sdist --out-dir /tmp/vertebra-oracle-wheel-check` and verify via `zipfile`/`tarfile` that `.whl` contains only `vertebra/_oracle.<ext>` and `vertebra/_oracle.pyi` (`len == 2`, zero `.py` files) while `.tar.gz` sdist contains `Cargo.toml`, `build.rs`, and `src/lib.rs`.
- `.agents/skills/review-core/references/areas.toml`:
  - Under `[areas.pyo3-bridge].paths`: add `"src/rs/vertebra-oracle/**"` alongside `"src/py/vertebra/_oracle.pyi"` so all Rust sources and PyO3 stubs for `vertebra-oracle` route to `pyo3-bridge`.

## Edge Cases & Error Semantics

- Excluding `vertebra-oracle` from `cargo build --workspace` in `Makefile` `dev` and `build` prevents non-Maturin `cdylib` `libpython` linker errors on Linux/macOS while `maturin develop` builds and installs the extension into `.venv`.
- `.whl` archive under `vertebra/` must contain strictly `vertebra/_oracle.<abi>.*` and `vertebra/_oracle.pyi` (2 entries total; zero `.py`, `py.typed`, or sibling `.pyi` files).
- Verify `make review-check` and `uv run python -m tools.scripts.review_setup route` route `src/rs/vertebra-oracle/src/lib.rs` and `src/py/vertebra/_oracle.pyi` to `[areas.pyo3-bridge]`.

## Public-Seam Test Blueprint

- **Canonical gate**: `make dev`, `make oracle-test`, `make oracle-wheel-check`, and `make review-check` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| :--- | :--- | :--- |
| Root `pyproject.toml` `[oracle]` extra & sources | Parse `pyproject.toml` with `tomllib` | `optional-dependencies["oracle"] == ["vertebra-oracle==0.3.0"]`, `"vertebra-oracle==0.3.0" in optional-dependencies["all"]`, `tool["uv"]["sources"]["vertebra-oracle"] == {"path": "src/rs/vertebra-oracle", "editable": True}`. |
| `make dev` & `make oracle-test` | Run `make dev && make oracle-test` | Exits `0`; `mypy.stubtest vertebra._oracle` passes and `vertebra._oracle.GIL_USED is False`. |
| `make oracle-wheel-check` (`zipfile` + `tarfile`) | Run `make oracle-wheel-check` | Builds `.whl` and `.tar.gz` in `/tmp/vertebra-oracle-wheel-check`; `.whl` has exactly `2` `vertebra/` entries (`_oracle.<ext>` and `_oracle.pyi`); `.tar.gz` includes `Cargo.toml`, `build.rs`, and `src/lib.rs`. |
| Review area routing (`[areas.pyo3-bridge]`) | `make review-check` + `ReviewRouter.route(["src/rs/vertebra-oracle/src/lib.rs", "src/py/vertebra/_oracle.pyi"])` | Exits `0` and routes both `src/rs/vertebra-oracle/**` and `src/py/vertebra/_oracle.pyi` to `pyo3-bridge`. |

## Verification

Run the manifest checks, `make dev`, `make oracle-test`, `make oracle-wheel-check`, and `make review-check`:

```bash
uv run --no-sync python -c '
import pathlib, tomllib
pyp = tomllib.loads(pathlib.Path("pyproject.toml").read_text())
opt = pyp["project"]["optional-dependencies"]
assert opt["oracle"] == ["vertebra-oracle==0.3.0"]
assert "vertebra-oracle==0.3.0" in opt["all"]
assert pyp["tool"]["uv"]["sources"]["vertebra-oracle"] == {"path": "src/rs/vertebra-oracle", "editable": True}
areas = tomllib.loads(pathlib.Path(".agents/skills/review-core/references/areas.toml").read_text())
p = areas["areas"]["pyo3-bridge"]["paths"]
assert "src/rs/vertebra-oracle/**" in p and "src/py/vertebra/_oracle.pyi" in p
'
make dev
make oracle-test
make oracle-wheel-check
make review-check
```

Commit as `build(oracle): wire root [oracle] extra, Makefile targets, and review areas`.

## Acceptance Criteria

- [x] `pyproject.toml` declares `[project.optional-dependencies].oracle = ["vertebra-oracle==0.3.0"]`, includes `"vertebra-oracle==0.3.0"` in `all`, configures `[tool.uv.sources].vertebra-oracle`, and registers `[[tool.bumpversion.files]]` entries (R1).
- [x] `Makefile` wires `vertebra-oracle` into `dev`, `build`, `py-test`, `lint`, `lint-all`, `oracle-test`, and `oracle-wheel-check`, and `make oracle-wheel-check` exits `0` (R1).
- [x] `.agents/skills/review-core/references/areas.toml` includes `"src/rs/vertebra-oracle/**"` and `"src/py/vertebra/_oracle.pyi"` under `[areas.pyo3-bridge].paths`, and `make review-check` exits `0` (R1).

## Notes & Discoveries

- [2026-11-09 01:15] Keeping `oracle-wheel-check` self-contained in `Makefile` during `ch01:t03` lets every chapter verify `.whl` and `.tar.gz` isolation immediately, before `ch07:t02` adds the full outside-checkout venv + free-threaded `3.14t`/`3.15t` `tools/scripts/verify_oracle_wheel.py` script and `.github/workflows/release.yaml` matrix lanes.

[^root-pyproject]: `pyproject.toml` — Root `vertebra-core` optional dependency extras, `uv.sources`, and `bumpversion` config.
[^root-makefile]: `Makefile` — Workspace `dev`, `build`, `lint`, `lint-all`, `py-test`, and wheel verification targets.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review area path routing manifest for `pyo3-bridge`.
[^gil-test]: `src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false` — Free-threaded GIL declaration verification across native extension modules.
