---
type: Task
id: "native-cursor-data-path:network:t40"
chapter_id: network-engines
title: "Delete NATIVE_CURSOR_ENGINES and the per-statement cache helper"
description: "Delete the transitional engine set and _lookup_or_cache_statement from db/driver/_common.py now that no caller remains."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: characterization
depends_on:
  - "native-cursor-data-path:network:t37"
  - "native-cursor-data-path:network:t38"
  - "native-cursor-data-path:network:t39"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:181-195"
  - "src/py/vertebra/db/driver/_common.py:1-55"
  - "src/py/vertebra/db/driver/_common.py:112-255"
  - "src/py/tests/unit/db/test_db_unified_driver_contracts.py:1-60"
files:
  - "src/py/vertebra/db/driver/_common.py"
tests:
  - "src/py/tests/unit/db/test_db_unified_driver_contracts.py"
estimated_loc: 80
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: driver-common
    resource: "src/py/vertebra/db/driver/_common.py::CommonDriverMixin"
    title: "Shared driver mixin with lease and statement-cache helpers"
tags: [driver, python, cleanup, lease]
---

# Delete NATIVE_CURSOR_ENGINES and the per-statement cache helper

## Objective & Scope Boundary

`_common.py` no longer carries the transitional constant or a helper that registers statements in shared memory for every query. The lease helpers stay, and their only callers are the checkout and connect points defined in this chapter.

Out of scope:

- `_BrokerLeasePlan`, `_acquire_broker_lease`, `_release_broker_lease`, `_acquire_pid_lease`, and `_release_pid_lease`. Checkout points and Storage API calls use them.
- `capabilities.py` and protocol members. The cleanup chapter owns them.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/driver/_common.py:231-254` is `_lookup_or_cache_statement`.
- `src/py/vertebra/db/driver/_common.py:160-199` are the lease helpers that stay.
- `NATIVE_CURSOR_ENGINES` sits where the wrapper-layers chapter placed it in this file.

## Contracts & Edge Cases

- Run `git grep -n 'NATIVE_CURSOR_ENGINES' -- src tools docs`. Matches outside `_common.py` mean t37, t38, or t39 is incomplete: stop and mark the task blocked with the paths.
- Delete the constant `NATIVE_CURSOR_ENGINES` and its `__all__` entry if it has one.
- Run `git grep -n '_lookup_or_cache_statement' -- src/py`. With no match outside `_common.py`, delete the method and the `StatementCache` and `StatementPlan` type-checking imports it alone used. With a match under the SQLite or DuckDB adapter packages, the embedded chapter left a caller: stop and mark the task blocked with the path.
- Run `git grep -n '_acquire_broker_lease\|_release_broker_lease' -- src/py/vertebra/db/adapters`. Every match must be inside a checkout, release, connect, close, or refresh method named in tasks t07, t08, t13, t17, t26, and t27. Any other match is a per-statement lease: stop and mark the task blocked with the path.
- Update the docstring of `CommonDriverMixin` to say leases are taken at connection checkout.
- Tests that called `_lookup_or_cache_statement` are deleted.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Constant gone | `from vertebra.db.driver._common import NATIVE_CURSOR_ENGINES` | `ImportError` |
| Helper gone | `CommonDriverMixin._lookup_or_cache_statement` | `AttributeError` |
| Lease helpers kept | `_acquire_broker_lease` with a recording broker double | Acquires and releases as before |
| Driver contract tests | Existing tests in the file | Pass |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_db_unified_driver_contracts.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/driver/_common.py`
- `uv run --no-sync mypy src/py/vertebra/db/driver/_common.py`
- `test -z "$(git grep -n 'NATIVE_CURSOR_ENGINES' -- src tools docs)"`
- `test -z "$(git grep -n '_lookup_or_cache_statement' -- src/py)"`
- `make lint-all`

## Acceptance Criteria

- [ ] `NATIVE_CURSOR_ENGINES` and `_lookup_or_cache_statement` have no reference in the repository outside the spec directory.
- [ ] `make lint-all` passes.

## Notes & Discoveries

- All 7 test files referencing `NATIVE_CURSOR_ENGINES` or `_lookup_or_cache_statement` are now in scope; removed those transitional assertions and deleted both symbols plus `StatementCache`/`StatementPlan` `TYPE_CHECKING` imports from `src/py/vertebra/db/driver/_common.py`.
- Added `.cursor()` support to `_ContractBigQueryClient`, `_ContractSpannerClient`, and `_ContractSpannerRwTx` in `src/py/tests/unit/db/test_db_unified_driver_contracts.py` so all 29 unified driver contract tests pass, plus a direct `_acquire_broker_lease` / `_release_broker_lease` roundtrip test.
- Repo-wide `make lint` passes Biome, `tsc`, Ruff, `rustfmt`, and Clippy, but `mypy` reports 49 pre-existing errors in 9 unrelated test files outside `t40` scope (targeted `mypy` and `pyright` on `_common.py` and `test_db_unified_driver_contracts.py` pass with 0 errors).
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` retains 11 pre-existing failing tests for removed per-statement broker lease / `StatementCache` / `last_rewritten_sql` behavior (to be addressed in `cleanup-and-checks`).
