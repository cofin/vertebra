---
type: Task
id: "native-postgres-driver:ch08:t03"
chapter_id: ch08-constructor-parity-and-wheel-gil
title: Exclude _postgres.pyi from bigquery, bundler, and spanner wheels
description: Add "**/py/vertebra/_postgres.pyi" to [tool.maturin].exclude in vertebra-bigquery, vertebra-bundler, and vertebra-spanner pyproject.toml manifests and verify zero stub leakage across built wheel archives.
status: stable
state: completed
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch08:t02"
read_set:
  - src/rs/vertebra-bigquery/pyproject.toml
  - src/rs/vertebra-bundler/pyproject.toml
  - src/rs/vertebra-spanner/pyproject.toml
  - src/rs/vertebra-postgres/pyproject.toml
files:
  - src/rs/vertebra-bigquery/pyproject.toml
  - src/rs/vertebra-bundler/pyproject.toml
  - src/rs/vertebra-spanner/pyproject.toml
tests: []
estimated_loc: 45
plan_revision: 5
plan_commit: c8722c76
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
commit: adf34226
sources:
  - id: bq-pyproject
    resource: src/rs/vertebra-bigquery/pyproject.toml
    title: BigQuery optional Maturin wheel manifest and sibling stub exclusions
  - id: bundler-pyproject
    resource: src/rs/vertebra-bundler/pyproject.toml
    title: Bundler optional Maturin wheel manifest and sibling stub exclusions
  - id: spanner-pyproject
    resource: src/rs/vertebra-spanner/pyproject.toml
    title: Spanner optional Maturin wheel manifest and sibling stub exclusions
tags:
  - postgres
  - wheel
  - packaging
  - stubs
---

# Task t03: Exclude `_postgres.pyi` from `bigquery`, `bundler`, and `spanner` Wheels

## Objective & Scope Boundary

Add `"**/py/vertebra/_postgres.pyi"` to `[tool.maturin].exclude` across `src/rs/vertebra-bigquery/pyproject.toml`, `src/rs/vertebra-bundler/pyproject.toml`, and `src/rs/vertebra-spanner/pyproject.toml` so building those companion wheels with `python-source = "../../py"` never bundles `vertebra/_postgres.pyi` [^bq-pyproject] [^bundler-pyproject] [^spanner-pyproject]. The fourth companion wheel (`src/rs/vertebra-storage/pyproject.toml`) is updated in `ch08:t04` to keep every worksheet strictly within `<= 3` production files ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-bigquery/pyproject.toml` — `[tool.maturin].exclude` list for `vertebra._bigquery` [^bq-pyproject].
- `src/rs/vertebra-bundler/pyproject.toml` — `[tool.maturin].exclude` list for `vertebra._bundler` [^bundler-pyproject].
- `src/rs/vertebra-spanner/pyproject.toml` — `[tool.maturin].exclude` list for `vertebra._spanner` [^spanner-pyproject].
- `src/rs/vertebra-postgres/pyproject.toml` — Reciprocal sibling `.pyi` exclusion list created in `ch01:t03`.

## Symbol & Signature Contracts

- `src/rs/vertebra-bigquery/pyproject.toml`:
  - Append `"**/py/vertebra/_postgres.pyi"` to `[tool.maturin].exclude` alongside `_bundler.pyi`, `_spanner.pyi`, `_storage.pyi`, and `_vertebra.pyi`.
- `src/rs/vertebra-bundler/pyproject.toml`:
  - Append `"**/py/vertebra/_postgres.pyi"` to `[tool.maturin].exclude` alongside `_bigquery.pyi`, `_spanner.pyi`, `_storage.pyi`, and `_vertebra.pyi`.
- `src/rs/vertebra-spanner/pyproject.toml`:
  - Append `"**/py/vertebra/_postgres.pyi"` to `[tool.maturin].exclude` alongside `_bigquery.pyi`, `_bundler.pyi`, `_storage.pyi`, and `_vertebra.pyi`.

## Edge Cases & Error Semantics

- Because every optional wheel sets `python-source = "../../py"` so Maturin places its native `.so`/`.pyd` and `.pyi` inside the shared `vertebra/` namespace directory, any newly added `src/py/vertebra/_*.pyi` file is automatically included in **every** sibling wheel unless explicitly listed in `[tool.maturin].exclude`.
- Duplicate `vertebra/_postgres.pyi` entries inside sibling wheels cause `pip` / `uv` record ownership conflicts on uninstall (`pip uninstall vertebra-postgres` in `ch29:t01` would either leave stale stubs behind or corrupt sibling wheel RECORD manifests).

## Public-Seam Test Blueprint

- **Canonical gate**: `tomllib` manifest check and `uv build --wheel` + `zipfile.ZipFile` archive member inspection across `vertebra-bigquery`, `vertebra-bundler`, and `vertebra-spanner` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Manifest `exclude` inventory | Parse `src/rs/vertebra-{bigquery,bundler,spanner}/pyproject.toml` via `tomllib` | Each `[tool.maturin].exclude` list contains exact `"**/py/vertebra/_postgres.pyi"`. |
| Built wheel `zipfile` isolation (`bigquery`, `bundler`, `spanner`) | Build wheels for `src/rs/vertebra-bigquery`, `src/rs/vertebra-bundler`, and `src/rs/vertebra-spanner` into a temporary `/tmp` dir and inspect `zf.namelist()` | Each wheel's `vertebra/` entries are strictly `vertebra/_<pkg>.<ext>` and `vertebra/_<pkg>.pyi` (`"vertebra/_postgres.pyi"` is absent from all three archives). |

## Verification

Update the three `pyproject.toml` manifests, build all three wheels into an isolated `/tmp` directory, and verify zero `_postgres.pyi` leakage:

```bash
SIBLING_DIST="$(mktemp -d /tmp/vertebra-sibling-wheels-XXXXXX)"
for pkg in bigquery bundler spanner; do
  uv build "src/rs/vertebra-$pkg" --wheel --out-dir "$SIBLING_DIST"
done
uv run --no-sync python -c '
import glob, pathlib, sys, tomllib, zipfile

dist = pathlib.Path(sys.argv[1])
for pkg in ("bigquery", "bundler", "spanner"):
    cfg = tomllib.loads(pathlib.Path(f"src/rs/vertebra-{pkg}/pyproject.toml").read_text())
    excludes = cfg["tool"]["maturin"]["exclude"]
    assert "**/py/vertebra/_postgres.pyi" in excludes, (pkg, excludes)
    whls = glob.glob(str(dist / f"vertebra_{pkg}-*.whl"))
    assert len(whls) == 1, (pkg, whls)
    with zipfile.ZipFile(whls[0]) as zf:
        vtb = [n for n in zf.namelist() if n.startswith("vertebra/")]
        assert "vertebra/_postgres.pyi" not in vtb, (pkg, vtb)
        assert f"vertebra/_{pkg}.pyi" in vtb and len(vtb) == 2, (pkg, vtb)
' "$SIBLING_DIST"
rm -rf "$SIBLING_DIST"
```

Commit as `build(postgres): exclude _postgres.pyi from bigquery, bundler, and spanner wheels`.

## Acceptance Criteria

- [x] `src/rs/vertebra-bigquery/pyproject.toml`, `src/rs/vertebra-bundler/pyproject.toml`, and `src/rs/vertebra-spanner/pyproject.toml` each include `"**/py/vertebra/_postgres.pyi"` in `[tool.maturin].exclude` (R1).
- [x] Built wheel archives for `vertebra-bigquery`, `vertebra-bundler`, and `vertebra-spanner` contain zero `vertebra/_postgres.pyi` entries and strictly two `vertebra/` files each (R1).

## Notes & Discoveries

- None.

[^bq-pyproject]: `src/rs/vertebra-bigquery/pyproject.toml` — Maturin wheel manifest and `.pyi` exclusions for `vertebra-bigquery`.
[^bundler-pyproject]: `src/rs/vertebra-bundler/pyproject.toml` — Maturin wheel manifest and `.pyi` exclusions for `vertebra-bundler`.
[^spanner-pyproject]: `src/rs/vertebra-spanner/pyproject.toml` — Maturin wheel manifest and `.pyi` exclusions for `vertebra-spanner`.
