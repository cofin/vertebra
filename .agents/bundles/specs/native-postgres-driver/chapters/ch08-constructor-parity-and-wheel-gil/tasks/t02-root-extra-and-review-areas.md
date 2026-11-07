---
type: Task
id: "native-postgres-driver:ch08:t02"
chapter_id: ch08-constructor-parity-and-wheel-gil
title: Register root postgres extra, uv.lock, and review area paths
description: Add the vertebra-core[postgres] optional extra, all-extra entry, editable uv source, and bumpversion rules to root pyproject.toml, synchronize uv.lock, and register vertebra-postgres and _postgres.pyi in areas.toml.
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch08:t01"
read_set:
  - pyproject.toml
  - uv.lock
  - .agents/skills/review-core/references/areas.toml
  - tools/scripts/review_setup.py
  - src/rs/vertebra-postgres/pyproject.toml
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
files:
  - pyproject.toml
  - uv.lock
  - .agents/skills/review-core/references/areas.toml
tests: []
estimated_loc: 85
plan_revision: 3
plan_commit: dd432f18
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
updated_at: "2026-11-07T16:55:00Z"
commit: null
sources:
  - id: root-pyproject
    resource: pyproject.toml
    title: Root vertebra-core optional dependencies, uv.sources, and bumpversion configuration
  - id: review-areas
    resource: .agents/skills/review-core/references/areas.toml
    title: Review capability area manifest for runtime-ipc and pyo3-bridge
  - id: adr-004
    resource: .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
    title: Optional driver wheel distribution and coordinated version invariants
tags:
  - postgres
  - packaging
  - lockfile
  - review
---

# Task t02: Register Root `postgres` Extra, `uv.lock`, and Review Area Paths

## Objective & Scope Boundary

Wire `vertebra-postgres` into root `pyproject.toml` (`[project.optional-dependencies]`, `all`, `[tool.uv.sources]`, and `[[tool.bumpversion.files]]`), synchronize `uv.lock` atomically in the same commit so `uv lock --check` stays clean, and register `src/rs/vertebra-postgres/**` and `src/py/vertebra/_postgres.pyi` in `.agents/skills/review-core/references/areas.toml` [^root-pyproject] [^review-areas] [^adr-004]. Companion wheel `[tool.maturin].exclude` updates and `.github/workflows/release.yaml` belong to `ch08:t03` and `ch08:t04` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `pyproject.toml` — Sibling `bigquery`, `bundler`, `spanner`, and `storage` optional extras, `all` list, `[tool.uv.sources]`, and `[[tool.bumpversion.files]]` entries [^root-pyproject].
- `uv.lock` — Workspace Python dependency lockfile paired with `pyproject.toml`.
- `.agents/skills/review-core/references/areas.toml` — `[areas.runtime-ipc].paths` and `[areas.pyo3-bridge].paths` routing lists [^review-areas].
- `tools/scripts/review_setup.py` — `ReviewManifest`, `ReviewSetupValidator`, and `ReviewRouter` validation rules.
- `src/rs/vertebra-postgres/pyproject.toml` — `name = "vertebra-postgres"` and `version = "0.3.0"` from `ch01:t03`.
- `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — Coordinated optional wheel versioning and isolation rules [^adr-004].

## Symbol & Signature Contracts

- `pyproject.toml`:
  - Under `[project.optional-dependencies]`:
    - Add `postgres = ["vertebra-postgres>=0.3.0"]` (or lockstep `"vertebra-postgres==0.3.0"` matching sibling `bigquery`/`bundler`/`spanner`/`storage` pins and bumpversion search strings; keep `postgres` and `all` identical so `bump-my-version` updates both cleanly). Specifically, match the sibling optional-wheel convention in `pyproject.toml` (`vertebra-postgres==0.3.0` or `vertebra-postgres>=0.3.0` paired with matching `[[tool.bumpversion.files]]` `search`/`replace`) and include the exact same requirement entry inside `all = [...]`.
  - Under `[tool.uv.sources]`:
    - Add `vertebra-postgres = { path = "src/rs/vertebra-postgres", editable = true }` so `uv lock` and editable workspace sync resolve `vertebra-postgres` locally without querying PyPI.
  - Under `[[tool.bumpversion.files]]`:
    - Add root `pyproject.toml` dependency version replacement (`vertebra-postgres` `{current_version}` -> `{new_version}` matching the specifier in `[project.optional-dependencies]`) and `filename = "src/rs/vertebra-postgres/pyproject.toml"` (`search = 'version = "{current_version}"'`, `replace = 'version = "{new_version}"'`).
- `uv.lock`:
  - Regenerate without upgrading unrelated dependencies (`uv lock --offline`, falling back to `uv lock` only if cache metadata requires it) so `vertebra-postgres` is recorded in `uv.lock` and `uv lock --check` exits `0`.
- `.agents/skills/review-core/references/areas.toml`:
  - Append `"src/rs/vertebra-postgres/**"` to `[areas.runtime-ipc].paths`.
  - Append `"src/rs/vertebra-postgres/**"` and `"src/py/vertebra/_postgres.pyi"` to `[areas.pyo3-bridge].paths` (using explicit crate/stub paths, never blanket `src/rs/**` or `src/py/**` globs).

## Edge Cases & Error Semantics

- Editing `pyproject.toml` without updating `uv.lock` leaves `uv lock --check` failing in CI and subsequent offline `--no-sync` commands out of date; both files must be committed together.
- Omitting `[[tool.bumpversion.files]]` for either `pyproject.toml` or `src/rs/vertebra-postgres/pyproject.toml` breaks version parity (`importlib.metadata.version("vertebra-postgres") == vertebra.__version__`) on the next release bump.
- Using a blanket glob in `areas.toml` would misroute unrelated crates; `ReviewRouter.route` must route `src/rs/vertebra-postgres/src/lib.rs` to both `runtime-ipc` and `pyo3-bridge`, and `src/py/vertebra/_postgres.pyi` to `pyo3-bridge` (plus `python-framework`).

## Public-Seam Test Blueprint

- **Canonical gate**: `tomllib` manifest inspection, `uv lock --check`, `ReviewRouter.route` assertion, and `make review-check` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Root `[postgres]` & `all` extra + `uv.sources` + `bumpversion` | Parse `pyproject.toml` via `tomllib.loads` | `"postgres"` in `project["optional-dependencies"]` referencing `vertebra-postgres` (`0.3.0`); `all` includes `vertebra-postgres`; `tool.uv.sources["vertebra-postgres"] == {"path": "src/rs/vertebra-postgres", "editable": True}`; `tool.bumpversion.files` covers both `pyproject.toml` and `src/rs/vertebra-postgres/pyproject.toml`. |
| Lockfile synchronization | `uv lock --check` | Exits `0` with zero dirty diff after regeneration. |
| Review area routing & policy gate | `ReviewRouter.route(["src/rs/vertebra-postgres/src/lib.rs", "src/py/vertebra/_postgres.pyi"])` and `make review-check` | `src/rs/vertebra-postgres/src/lib.rs` routes to `runtime-ipc` and `pyo3-bridge`; `src/py/vertebra/_postgres.pyi` routes to `pyo3-bridge`; `make review-check` exits `0`. |

## Verification

Update `pyproject.toml`, `uv.lock`, and `.agents/skills/review-core/references/areas.toml`, then run and verify exit `0`:

```bash
uv lock --check
uv run --no-sync python -c '
import pathlib, tomllib
from tools.scripts.review_setup import ReviewManifest, ReviewRouter

data = tomllib.loads(pathlib.Path("pyproject.toml").read_text())
opt = data["project"]["optional-dependencies"]
assert "postgres" in opt and any(r.startswith("vertebra-postgres") for r in opt["postgres"]), opt.get("postgres")
assert any(r.startswith("vertebra-postgres") for r in opt["all"]), opt["all"]
assert data["tool"]["uv"]["sources"]["vertebra-postgres"] == {"path": "src/rs/vertebra-postgres", "editable": True}
bv_files = [(f["filename"], f.get("search", "")) for f in data["tool"]["bumpversion"]["files"]]
assert any(fn == "pyproject.toml" and "vertebra-postgres" in s for fn, s in bv_files), bv_files
assert ("src/rs/vertebra-postgres/pyproject.toml", "version = \"{current_version}\"") in bv_files, bv_files

repo = pathlib.Path(".")
router = ReviewRouter(ReviewManifest.from_toml(repo / ".agents/skills/review-core/references/areas.toml"), repo)
routed = router.route(["src/rs/vertebra-postgres/src/lib.rs", "src/py/vertebra/_postgres.pyi"])
assert "src/rs/vertebra-postgres/src/lib.rs" in routed["pyo3-bridge"], routed
assert "src/py/vertebra/_postgres.pyi" in routed["pyo3-bridge"], routed
assert any("src/rs/vertebra-postgres/src/lib.rs" in v for k, v in routed.items() if k.startswith("runtime-ipc")), routed
'
make review-check
```

Commit as `build(postgres): register root postgres extra, uv.lock, and review areas`.

## Acceptance Criteria

- [ ] Root `pyproject.toml` defines `[project.optional-dependencies].postgres`, includes `vertebra-postgres` in `all`, configures `[tool.uv.sources].vertebra-postgres`, and registers bumpversion rules for both `pyproject.toml` and `src/rs/vertebra-postgres/pyproject.toml` (R1).
- [ ] `uv.lock` is updated cleanly without unrelated upgrades and `uv lock --check` exits `0` (R1).
- [ ] `.agents/skills/review-core/references/areas.toml` routes `src/rs/vertebra-postgres/**` and `src/py/vertebra/_postgres.pyi` explicitly and `make review-check` exits `0` (R1).

## Notes & Discoveries

- None.

[^root-pyproject]: `pyproject.toml` — Root optional extras, `uv.sources`, and `bumpversion` configuration.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review area routing manifest.
[^adr-004]: `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — Optional driver wheel packaging and version coordination rules.
