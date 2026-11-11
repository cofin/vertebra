---
type: Task
id: "native-postgres-driver:ch01:t03"
chapter_id: ch01-wheel-scaffold-and-config
title: Wire vertebra-postgres wheel, _postgres.pyi stub, and Makefile build/test targets
description: Author src/rs/vertebra-postgres/pyproject.toml, src/py/vertebra/_postgres.pyi, and Makefile dev/build/stubtest/postgres-test/postgres-wheel-check targets with wheel zipfile and sdist tarfile verification.
status: done
state: completed
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch01:t02"
read_set:
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-spanner/pyproject.toml
  - src/py/vertebra/_spanner.pyi
  - Makefile
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
files:
  - src/rs/vertebra-postgres/pyproject.toml
  - src/py/vertebra/_postgres.pyi
  - Makefile
tests: []
estimated_loc: 110
plan_revision: 5
plan_commit: 79eea133
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: 62bf91f1
sources:
  - id: spanner-pyproject
    resource: src/rs/vertebra-spanner/pyproject.toml
    title: Reference Maturin namespace wheel manifest and file exclusions
  - id: spanner-pyi
    resource: src/py/vertebra/_spanner.pyi::SyncSpannerClient
    title: Reference native extension .pyi stub header
  - id: root-makefile
    resource: Makefile
    title: Workspace dev, build, py-test, lint, and lint-all extension targets
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Truthful optional wheel export and namespace isolation contract
tags:
  - postgres
  - wheel
  - stubs
  - makefile
---

# Task t03: Wire `vertebra-postgres` Wheel, `_postgres.pyi` Stub, and `Makefile` Build/Test Targets

## Objective & Scope Boundary

Create `src/rs/vertebra-postgres/pyproject.toml` and `src/py/vertebra/_postgres.pyi` and wire `Makefile` (`dev`, `build`, `py-test`, `lint`, `lint-all`, `postgres-test`, and `postgres-wheel-check`) so `vertebra._postgres` builds cleanly in development and release modes, passes `mypy.stubtest`, and produces verified `.whl` and `.tar.gz` sdist archives [^spanner-pyproject] [^spanner-pyi] [^root-makefile] [^pg-contracts]. Root `pyproject.toml`/`uv.lock`/`areas.toml` registration, sibling `pyproject.toml` `_postgres.pyi` exclusions, and `.github/workflows/release.yaml` land in `ch08:t02`–`t04` (and `ch29:t01`, [../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-postgres/Cargo.toml` — Crate target `_postgres` and `extension-module` feature from `ch01:t01`.
- `src/rs/vertebra-postgres/src/lib.rs` — Exported `__version__`, `FREE_THREADED`, and `GIL_USED` constants from `ch01:t01`.
- `src/rs/vertebra-spanner/pyproject.toml` — Reference `maturin` `python-source`, `module-name`, and `exclude` globs [^spanner-pyproject].
- `src/py/vertebra/_spanner.pyi` — Stub docstring, metadata constants, and `__all__` conventions [^spanner-pyi].
- `Makefile` — `dev`, `build`, `py-test`, `lint`, and `lint-all` targets [^root-makefile].
- `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — Optional namespace wheel isolation invariants.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/pyproject.toml`:
  - `[build-system]`: `requires = ["maturin>=1.5,<2"]`, `build-backend = "maturin"`.
  - `[project]`: `name = "vertebra-postgres"`, `version = "0.3.0"`, `description = "Native Rust PostgreSQL Protocol 3.0 client and pool extension for Vertebra."`, `requires-python = ">=3.12"`, `license = { text = "MIT" }`.
  - `[tool.maturin]`:
    - `python-source = "../../py"`
    - `module-name = "vertebra._postgres"`
    - `features = ["extension-module"]`
    - `exclude` containing `"**/py/vertebra/**/*.py"`, `"**/py/vertebra/**/*.so"`, `"**/py/vertebra/**/*.pyd"`, `"**/py/vertebra/py.typed"`, `"**/py/vertebra/_bigquery.pyi"`, `"**/py/vertebra/_bundler.pyi"`, `"**/py/vertebra/_spanner.pyi"`, `"**/py/vertebra/_storage.pyi"`, `"**/py/vertebra/_vertebra.pyi"`.
- `src/py/vertebra/_postgres.pyi`:
  - Module docstring `"""Type stubs for the vertebra._postgres native extension module."""`
  - `__version__: str`
  - `FREE_THREADED: bool`
  - `GIL_USED: bool`
  - `__all__ = ["FREE_THREADED", "GIL_USED", "__version__"]`
- `Makefile`:
  - `dev`: append `--exclude vertebra-postgres` to `cargo build --workspace` and add `@uv run maturin develop --uv -m src/rs/vertebra-postgres/Cargo.toml`.
  - `build`: append `--exclude vertebra-postgres` to `cargo build --workspace ... --release` and add `@uv run maturin develop --uv -r -m src/rs/vertebra-postgres/Cargo.toml`.
  - `py-test`: append `vertebra._postgres` to `@uv run python -m mypy.stubtest ...`.
  - `lint` and `lint-all`: append `"vertebra._postgres"` to both dynamic `stub_mods` tuples.
  - Add `.PHONY: postgres-test` running `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked` and `uv run --no-sync python -m mypy.stubtest vertebra._postgres`.
  - Add `.PHONY: postgres-wheel-check` building `uv build src/rs/vertebra-postgres --wheel --sdist --out-dir /tmp/vertebra-postgres-wheel-check` and validating both `.whl` (`zipfile`) and `.tar.gz` (`tarfile`) contents.

## Edge Cases & Error Semantics

- `.whl` archive under `vertebra/` must contain strictly `vertebra/_postgres.<abi>.*` and `vertebra/_postgres.pyi` (2 entries total; zero `.py`, `py.typed`, or sibling `.pyi` files).
- `.tar.gz` sdist archive inspected via `tarfile` must include `Cargo.toml`, `build.rs`, and `src/lib.rs` (plus workspace Cargo sources packaged by Maturin) so source builds succeed outside a git checkout.
- Excluding `vertebra-postgres` from `cargo build --workspace` in `Makefile` avoids non-Maturin `cdylib` `libpython` link errors while `maturin develop` installs the editable extension into `.venv`.

## Public-Seam Test Blueprint

- **Canonical gate**: `make dev`, `mypy.stubtest vertebra._postgres`, `make postgres-test`, and `make postgres-wheel-check` (`zipfile` + `tarfile` archive verification, `tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `make dev` & runtime import | `make dev` then `import vertebra._postgres as m` | `m.__version__ == "0.3.0"`, `isinstance(m.FREE_THREADED, bool)`, `m.GIL_USED is False`. |
| Stub parity | `uv run --no-sync python -m mypy.stubtest vertebra._postgres` | Exits `0` with zero missing or extra symbols. |
| Wheel isolation (`zipfile`) | Inspect built `vertebra_postgres-*.whl` via `zipfile.ZipFile` | `vertebra/` entries are exclusively `vertebra/_postgres.<ext>` and `vertebra/_postgres.pyi` (`len == 2`, zero `.py` or sibling `.pyi` files). |
| Sdist Cargo sources (`tarfile`) | Inspect built `vertebra_postgres-*.tar.gz` via `tarfile.open` | Archive member names include `Cargo.toml`, `vertebra-postgres/Cargo.toml` (or root `Cargo.toml`), `build.rs`, and `src/lib.rs`. |

## Verification

Run the development install, stubtest, test target, and wheel/sdist archive inspections and verify exit `0`:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._bigquery vertebra._spanner vertebra._postgres
make postgres-test
make postgres-wheel-check
uv run --no-sync python -c '
import glob, tarfile, zipfile
whls = glob.glob("/tmp/vertebra-postgres-wheel-check/vertebra_postgres-*.whl")
sdists = glob.glob("/tmp/vertebra-postgres-wheel-check/vertebra_postgres-*.tar.gz")
assert len(whls) == 1 and len(sdists) == 1, (whls, sdists)
with zipfile.ZipFile(whls[0]) as zf:
    vtb = [n for n in zf.namelist() if n.startswith("vertebra/")]
    assert "vertebra/_postgres.pyi" in vtb and len(vtb) == 2, vtb
    assert all(not n.endswith(".py") for n in vtb), vtb
with tarfile.open(sdists[0], "r:gz") as tf:
    names = tf.getnames()
    assert any(n.endswith("Cargo.toml") for n in names), names
    assert any(n.endswith("build.rs") for n in names), names
    assert any(n.endswith("src/lib.rs") for n in names), names
'
```

Commit as `build(postgres): wire vertebra-postgres wheel, stub, and Makefile targets`.

## Acceptance Criteria

- [x] `src/rs/vertebra-postgres/pyproject.toml` and `src/py/vertebra/_postgres.pyi` package only `vertebra/_postgres.<ext>` and `vertebra/_postgres.pyi` in `.whl` while retaining Cargo sources (`Cargo.toml`, `build.rs`, `src/lib.rs`) in `.tar.gz` sdist (R1).
- [x] `Makefile` integrates `vertebra-postgres` across `dev`, `build`, `py-test`, `lint`, `lint-all`, `postgres-test`, and `postgres-wheel-check`, and `mypy.stubtest vertebra._postgres` exits `0` (R1).

## Notes & Discoveries

- [2026-11-07 16:40] Root `pyproject.toml` `[postgres]` extra, `uv.lock`, `.agents/skills/review-core/references/areas.toml`, sibling `pyproject.toml` `_postgres.pyi` exclusions, and `release.yaml` are staged in `ch08:t02`–`t04` (and `ch29:t01`) once the Python client classes are registered.

[^spanner-pyproject]: `src/rs/vertebra-spanner/pyproject.toml` — Maturin namespace wheel manifest and exclusion globs.
[^spanner-pyi]: `src/py/vertebra/_spanner.pyi::SyncSpannerClient` — Native extension type stub layout.
[^root-makefile]: `Makefile` — Polyglot build, test, and `mypy.stubtest` targets.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Optional wheel export and ownership rules.
