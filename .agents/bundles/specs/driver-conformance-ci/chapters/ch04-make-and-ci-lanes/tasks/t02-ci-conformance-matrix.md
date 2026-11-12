---
type: Task
id: "driver-conformance-ci:ch04:t02"
chapter_id: ch04-make-and-ci-lanes
title: Add the test-db-conformance CI matrix job
description: Add a GitHub Actions job running make db-conformance for every engine and version row against pytest-databases containers after lint, gated by the same path filter as test-python.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "driver-conformance-ci:ch04:t01"
read_set:
  - .github/workflows/ci.yml
  - Makefile
  - .agents/bundles/specs/driver-conformance-ci/chapters/ch04-make-and-ci-lanes/spec.md
files:
  - .github/workflows/ci.yml
tests: []
estimated_loc: 50
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
sources:
  - id: ci
    resource: .github/workflows/ci.yml
    title: test-python job layout (checkout, rust toolchain, rust-cache, setup-uv)
  - id: make
    resource: Makefile
    title: db-conformance target (ch04 t01)
tags:
  - ci
  - github-actions
  - conformance
---

# Task t02: Add the `test-db-conformance` CI Matrix Job

## Objective & Scope Boundary

Add a `test-db-conformance` job to `.github/workflows/ci.yml` after `test-python`, mirroring its step layout [^ci] [^make]:

```yaml
  test-db-conformance:
    name: db-conformance (${{ matrix.engine }} ${{ matrix.version }} py${{ matrix.python-version }})
    runs-on: ubuntu-latest
    timeout-minutes: 45
    needs: [changes, lint]
    if: needs.changes.outputs.python == 'true' || needs.changes.outputs.rust == 'true' || needs.changes.outputs.ci == 'true'
    strategy:
      fail-fast: false
      matrix:
        include:
          - { engine: duckdb, version: "", python-version: "3.12" }
          - { engine: duckdb, version: "", python-version: "3.14t" }
          - { engine: mysql, version: "8.0", python-version: "3.12" }
          - { engine: mysql, version: "8.4", python-version: "3.12" }
          - { engine: mysql, version: "8.4", python-version: "3.14t" }
          - { engine: mysql, version: "9.7", python-version: "3.12" }
          - { engine: mariadb, version: "11.4", python-version: "3.12" }
          - { engine: mariadb, version: "11.8", python-version: "3.12" }
          - { engine: mssql, version: "2022", python-version: "3.12" }
          - { engine: mssql, version: "2025", python-version: "3.12" }
          - { engine: oracle, version: "23ai", python-version: "3.12" }
    steps:
      - uses: actions/checkout@v7
        with:
          persist-credentials: false
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: "src/rs"
          key: "db-${{ matrix.python-version }}"
          shared-key: "vertebra"
          cache-on-failure: true
      - uses: astral-sh/setup-uv@v7
        with:
          enable-cache: true
          python-version: ${{ matrix.python-version }}
      - name: Run live conformance lane
        env:
          CARGO_PROFILE_DEV_DEBUG: "0"
          CARGO_INCREMENTAL: "0"
          ENGINE: ${{ matrix.engine }}
          VERSION: ${{ matrix.version }}
        run: make db-conformance ENGINE="$ENGINE" VERSION="$VERSION"
```

Out of scope: other jobs, release workflows, PostgreSQL rows.

## Bounded Read Manifest

- `.github/workflows/ci.yml` — `changes` outputs, `lint`, `test-python` steps [^ci].
- `Makefile` — `db-conformance` target [^make].
- Chapter spec alternatives (failure policy, version coverage).

## Symbol & Signature Contracts

- Job id `test-db-conformance`; matrix keys `engine`, `version`, `python-version`; eleven `include` rows exactly as listed.
- Matrix values reach the shell through `env:` (never interpolated directly into `run:`), matching the workflow's injection-safe pattern.

## Edge Cases & Error Semantics

- Empty `version` for DuckDB rows passes `VERSION=""`, which the Make target treats as unset.
- Docker is preinstalled on `ubuntu-latest`; `pytest-databases` resolves it without extra setup.
- Image pulls for SQL Server and Oracle fit within `timeout-minutes: 45`.

## Public-Seam Test Blueprint

- **Canonical gate**: YAML structural assertions (static validation).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Job exists and gates | `yaml.safe_load(open(".github/workflows/ci.yml"))["jobs"]["test-db-conformance"]` | `needs == ["changes", "lint"]`; `strategy["fail-fast"] is False`; `timeout-minutes == 45` |
| Matrix rows | `[(r["engine"], r["version"], r["python-version"]) for r in job["strategy"]["matrix"]["include"]]` | Equals the eleven listed tuples in order |
| Injection-safe run | `job["steps"][-1]["run"]` | `== 'make db-conformance ENGINE="$ENGINE" VERSION="$VERSION"'` and `"${{" not in` that string |

## Verification

- `uv run --no-sync python -c "import yaml; job = yaml.safe_load(open('.github/workflows/ci.yml'))['jobs']['test-db-conformance']; rows = [(r['engine'], r['version'], r['python-version']) for r in job['strategy']['matrix']['include']]; assert rows == [('duckdb', '', '3.12'), ('duckdb', '', '3.14t'), ('mysql', '8.0', '3.12'), ('mysql', '8.4', '3.12'), ('mysql', '8.4', '3.14t'), ('mysql', '9.7', '3.12'), ('mariadb', '11.4', '3.12'), ('mariadb', '11.8', '3.12'), ('mssql', '2022', '3.12'), ('mssql', '2025', '3.12'), ('oracle', '23ai', '3.12')], rows; assert job['strategy']['fail-fast'] is False; assert job['needs'] == ['changes', 'lint']; assert '\${{' not in job['steps'][-1]['run']"`
- `make lint-all` exits `0` (pre-commit YAML checks).
- Acceptance on the pull request: every `test-db-conformance` row is green.

## Acceptance Criteria

- [ ] `test-db-conformance` runs all eleven rows after `lint` with `fail-fast: false` (R10).
- [ ] Rows invoke `make db-conformance` with env-passed matrix values (R10).

## Notes & Discoveries

- [2026-11-12 00:30] The `rust-cache` key `db-<python>` shares compiled crates across engines for the same interpreter.

[^ci]: `.github/workflows/ci.yml` — `test-python` job layout.
[^make]: `Makefile` — `db-conformance` target.
