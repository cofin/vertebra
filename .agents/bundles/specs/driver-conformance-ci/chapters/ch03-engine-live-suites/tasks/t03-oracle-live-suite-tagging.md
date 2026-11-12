---
type: Task
id: "driver-conformance-ci:ch03:t03"
chapter_id: ch03-engine-live-suites
title: Tag the Oracle live suite for lane selection
description: Add the driver_engine("oracle") module marker to the Oracle live suite and add a regression test proving the conformance gate selects it in the Oracle lane and deselects it elsewhere.
status: draft
state: pending
priority: P2
verification_strategy: regression_tdd
depends_on:
  - "driver-conformance-ci:ch01:t01"
read_set:
  - src/py/tests/integration/db/test_oracle_native.py
  - src/py/tests/integration/conformance_gate.py
  - src/py/tests/integration/test_conformance_gate.py
files: []
tests:
  - src/py/tests/integration/db/test_oracle_native.py
  - src/py/tests/integration/test_conformance_gate.py
estimated_loc: 60
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
  - id: oracle-native
    resource: src/py/tests/integration/db/test_oracle_native.py::oracle_config
    title: Live Oracle 26ai suite fixture
  - id: gate
    resource: src/py/tests/integration/conformance_gate.py::ConformanceGate.item_engines
    title: Engine tagging resolution (ch01 t01)
tags:
  - oracle
  - integration
  - conformance
---

# Task t03: Tag the Oracle Live Suite for Lane Selection

## Objective & Scope Boundary

1. Add `pytestmark = [pytest.mark.driver_engine("oracle")]` to `src/py/tests/integration/db/test_oracle_native.py` directly after the imports [^oracle-native].
2. Add a regression row in `src/py/tests/integration/test_conformance_gate.py` that collects `test_oracle_native.py` with `pytest.main(["--collect-only", "-q", <path>], plugins=[collector])` (an in-process collector plugin capturing `session.items`) and asserts every collected item resolves `ConformanceGate.item_engines(item) == frozenset({"oracle"})` [^gate].

Out of scope: Oracle test bodies and `_resolve_oracle_config` behavior (unchanged), Oracle 23ai version matrix (ch01 `t01`).

## Bounded Read Manifest

- `src/py/tests/integration/db/test_oracle_native.py` — imports, `_resolve_oracle_config`, `oracle_config` [^oracle-native].
- `src/py/tests/integration/conformance_gate.py` — `ConformanceGate.item_engines` [^gate].
- `src/py/tests/integration/test_conformance_gate.py` — existing gate tests.

## Symbol & Signature Contracts

```python
pytestmark = [pytest.mark.driver_engine("oracle")]

class _ItemCollector:
    """pytest plugin capturing collected items for engine-tag assertions."""
    items: list[pytest.Item]
    def pytest_collection_finish(self, session: pytest.Session) -> None: ...
```

## Edge Cases & Error Semantics

- Collection imports `test_oracle_native.py` but never resolves `oracle_config`, so no `MissingDependencyError` or container start occurs during collect-only.
- The nested `pytest.main` runs with `-p no:cacheprovider` to avoid writing `.pytest_cache`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/integration/test_conformance_gate.py` (regression) and `src/py/tests/integration/db/test_oracle_native.py` (tag). Dependency category: in-process collection.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Oracle items tagged | collect-only `src/py/tests/integration/db/test_oracle_native.py` | `len(items) >= 8`; every `ConformanceGate.item_engines(item) == frozenset({"oracle"})` |
| Oracle lane selects | `ConformanceGate(frozenset({"oracle"})).selects(item, <integration root>)` for each item | `True` |
| Other lane deselects | `ConformanceGate(frozenset({"mysql"})).selects(item, <integration root>)` | `False` |

## Verification

- Red: `uv run --no-sync pytest src/py/tests/integration/test_conformance_gate.py -k oracle -q` fails before the marker exists.
- Green: `uv run --no-sync pytest src/py/tests/integration/test_conformance_gate.py src/py/tests/integration/db/test_oracle_native.py -q -rs` exits `0` offline.
- Live: `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_ENGINES=oracle uv run --no-sync pytest src/py/tests/integration/db -q -rs` exits `0`.
- Lint: `uv run --no-sync ruff check src/py/tests/integration`.

## Acceptance Criteria

- [ ] `test_oracle_native.py` is tagged and selected only in the Oracle lane (R8).
- [ ] The Oracle lane passes with zero skips under Docker (R8).

## Notes & Discoveries

- [2026-11-12 00:30] The Oracle suite already resolves through `vtb_oracle_config`, which now routes through `ServiceVersionMatrix` (`23ai`).

[^oracle-native]: `src/py/tests/integration/db/test_oracle_native.py::oracle_config` — Live Oracle suite fixture.
[^gate]: `src/py/tests/integration/conformance_gate.py::ConformanceGate.item_engines` — Engine tagging resolution.
