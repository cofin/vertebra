---
type: Task
id: "driver-conformance-ci:ch04:t01"
chapter_id: ch04-make-and-ci-lanes
title: Add make db-conformance and remove the oracle-test fallback branch
description: Add the parameterized db-conformance Make target driving one live lane and make oracle-test run the Oracle unit, conformance, and native suites without a fallback test file.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "driver-conformance-ci:ch03:t03"
read_set:
  - Makefile
  - src/py/tests/integration/conformance_gate.py
  - .agents/bundles/specs/driver-conformance-ci/chapters/ch04-make-and-ci-lanes/spec.md
files:
  - Makefile
tests: []
estimated_loc: 40
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
  - id: oracle-test
    resource: Makefile
    title: oracle-test fallback branch and py-test/PY_TEST_GROUPS conventions
  - id: gate
    resource: src/py/tests/integration/conformance_gate.py::ConformanceGate
    title: Lane environment variables consumed by the gate
tags:
  - makefile
  - conformance
---

# Task t01: Add `make db-conformance` and Remove the `oracle-test` Fallback Branch

## Objective & Scope Boundary

Edit `Makefile` in the Test section [^oracle-test]:

1. Add `db-conformance` after `postgres-wheel-check`:

```make
.PHONY: db-conformance
db-conformance: dev                                 ## Run a live pytest-databases conformance lane: make db-conformance ENGINE=<duckdb|mysql|mariadb|mssql|oracle> [VERSION=<v>]
	@if [ -z "$(ENGINE)" ]; then echo "ENGINE is required: duckdb, mysql, mariadb, mssql, oracle"; exit 2; fi
	@VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_ENGINES="$(ENGINE)" \
		$(if $(VERSION),VERTEBRA_CONFORMANCE_$(shell printf '%s' '$(ENGINE)' | tr '[:lower:]' '[:upper:]')_VERSION="$(VERSION)") \
		$(if $(filter duckdb,$(ENGINE)),VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1) \
		uv run $(PY_TEST_GROUPS) pytest src/py/tests/integration/db -v -rs
	@echo "${OK} $(ENGINE) live conformance passed"
```

2. Replace the `oracle-test` `if [ -f ... ] else ... fi` block with one unconditional line: `@PYTHONPATH="src/py:." uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_oracle_native.py -k "oracle" -v -rs`, and update its help text to `## Run Oracle Rust unit tests, stubtest, and Python unit, conformance, and native suites`.

Out of scope: other targets, CI (`t02`).

## Bounded Read Manifest

- `Makefile` — Test section, `PY_TEST_GROUPS`, `OK` variable, `oracle-test`, `postgres-wheel-check` [^oracle-test].
- `src/py/tests/integration/conformance_gate.py` — env variable names [^gate].
- Chapter spec edge-case matrix.

## Symbol & Signature Contracts

- Target name `db-conformance`; variables `ENGINE` (required), `VERSION` (optional).
- Exported env: `VERTEBRA_CONFORMANCE_DOCKER=1`, `VERTEBRA_CONFORMANCE_ENGINES=$(ENGINE)`, optional `VERTEBRA_CONFORMANCE_<ENGINE>_VERSION=$(VERSION)`, `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` for `ENGINE=duckdb`.
- Comma lists (`ENGINE=mysql,mariadb`) pass through to the gate; `VERSION` is only meaningful for a single engine and the target documents that in its help text.

## Edge Cases & Error Semantics

- `ENGINE` empty: exit `2` with the exact message.
- `ENGINE=mysql,mariadb VERSION=8.4`: the uppercase transform yields `VERTEBRA_CONFORMANCE_MYSQL,MARIADB_VERSION`, an invalid name the shell rejects; the help text states `VERSION` requires a single `ENGINE`, and the recipe guards with `@if [ -n "$(VERSION)" ] && printf '%s' "$(ENGINE)" | grep -q ','; then echo "VERSION requires a single ENGINE"; exit 2; fi`.
- `test_oracle_adapter.py` missing: pytest reports `file or directory not found` and exits `4`.

## Public-Seam Test Blueprint

- **Canonical gate**: `make -n` dry runs (static validation; no Python test file changes).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| MySQL lane dry run | `make -n db-conformance ENGINE=mysql VERSION=8.0` | Output contains `VERTEBRA_CONFORMANCE_ENGINES="mysql"` and `VERTEBRA_CONFORMANCE_MYSQL_VERSION="8.0"` and `pytest src/py/tests/integration/db` |
| DuckDB lane dry run | `make -n db-conformance ENGINE=duckdb` | Output contains `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` and no `_VERSION=` |
| Missing engine | `make db-conformance` with `dev` satisfied (`make -o dev db-conformance`) | Exit `2`; prints `ENGINE is required: duckdb, mysql, mariadb, mssql, oracle` |
| Version with list | `make -o dev db-conformance ENGINE=mysql,mariadb VERSION=8.4` | Exit `2`; prints `VERSION requires a single ENGINE` |
| Oracle target | `make -n oracle-test` | Contains `test_oracle_adapter.py`, `test_driver_conformance.py`, `test_oracle_native.py`; no `test_freethreaded_gil.py` |

## Verification

- `make -n db-conformance ENGINE=mysql VERSION=8.0 | grep -q 'VERTEBRA_CONFORMANCE_MYSQL_VERSION="8.0"'`
- `make -n db-conformance ENGINE=duckdb | grep -q 'VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1'`
- `make -o dev db-conformance; test $? -eq 2`
- `! make -n oracle-test | grep -q test_freethreaded_gil.py`
- Live smoke (Docker): `make db-conformance ENGINE=mariadb VERSION=11.4` exits `0`.

## Acceptance Criteria

- [ ] `make db-conformance` drives one lane with the gate env and fails loud on missing or ambiguous inputs (R9).
- [ ] `make oracle-test` runs the Oracle unit, conformance, and native suites with no fallback (R9).

## Notes & Discoveries

- [2026-11-12 00:30] `make help` parses the `##` comment, so the target appears automatically.

[^oracle-test]: `Makefile` — `oracle-test` fallback branch and test conventions.
[^gate]: `src/py/tests/integration/conformance_gate.py::ConformanceGate` — Lane environment variables.
