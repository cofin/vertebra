---
type: Task
id: server-runtime-correctness:ch09:t04
title: Remove the Inert max_commit_delay_ms Parameter from Spanner batch_write
description: Delete max_commit_delay_ms from SyncSpannerDriver.batch_write, AsyncSpannerDriver.batch_write, and the SQLSpec Spanner sync and async batch_write wrappers, since BatchWriteRequest has no commit-delay field.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch09-runtime-native-cleanup/spec.md
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/vertebra/extensions/sqlspec/spanner.py
  - src/py/vertebra/_spanner.pyi
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
files:
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/vertebra/extensions/sqlspec/spanner.py
tests:
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
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
created_at: "2026-11-12T01:35:00Z"
updated_at: "2026-11-12T01:35:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T01:35:00Z"
sources:
  - id: spanner-driver-py
    resource: src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver.batch_write
    title: batch_write discarding max_commit_delay_ms
  - id: sqlspec-spanner-py
    resource: src/py/vertebra/extensions/sqlspec/spanner.py
    title: SQLSpec Spanner drivers forwarding max_commit_delay_ms into batch_write
  - id: spanner-pyi
    resource: src/py/vertebra/_spanner.pyi
    title: Native batch_write signature without a commit-delay argument
tags:
  - spanner
  - cleanup
---

# Task t04: Remove the Inert max_commit_delay_ms Parameter from Spanner batch_write

## Objective & Scope Boundary

`SyncSpannerDriver.batch_write` and `AsyncSpannerDriver.batch_write` accept `max_commit_delay_ms` and discard it with `_ = max_commit_delay_ms` [^spanner-driver-py]; the native `batch_write(mutation_groups_json, exclude_txn_from_change_streams=False)` has no such argument [^spanner-pyi]. The SQLSpec sync and async `batch_write` wrappers forward it [^sqlspec-spanner-py]. Remove the parameter from all four. Out of scope: `max_commit_delay_ms` on `apply_mutations`, `commit_mutations`, `run_in_transaction`, and `SpannerConfig`.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver.batch_write`, `AsyncSpannerDriver.batch_write`.
- `src/py/vertebra/extensions/sqlspec/spanner.py` — sync and async `batch_write` calling `self._core.batch_write(mutation_groups, max_commit_delay_ms=max_commit_delay_ms)`.
- `src/py/vertebra/_spanner.pyi` — native `batch_write` signatures.
- `src/py/tests/unit/db/test_spanner.py` — fake clients and existing `driver.batch_write` coverage.
- `src/py/tests/unit/extensions/test_sqlspec_spanner.py` — SQLSpec driver coverage.

## Symbol & Signature Contracts

```python
class SyncSpannerDriver:
    def batch_write(self, mutation_groups: Sequence[MutationGroup | Sequence[Mutation | Mapping[str, Any]]], *, exclude_txn_from_change_streams: bool = False) -> list[dict[str, Any]]: ...

class AsyncSpannerDriver:
    async def batch_write(self, mutation_groups: Sequence[MutationGroup | Sequence[Mutation | Mapping[str, Any]]], *, exclude_txn_from_change_streams: bool = False) -> list[dict[str, Any]]: ...
```

- SQLSpec sync: `def batch_write(self, mutation_groups: Sequence[MutationGroup | Sequence[Mutation]]) -> list[dict[str, Any]]` returning `self._core.batch_write(mutation_groups)`; the async wrapper mirrors it with `await`.
- The `_ = max_commit_delay_ms` statements are deleted.

## Edge Cases & Error Semantics

- Passing `max_commit_delay_ms=` to any of the four raises `TypeError`.
- Empty `mutation_groups` still returns `[]` without calling the client.
- `exclude_txn_from_change_streams` still reaches the native call.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_spanner.py`, `src/py/tests/unit/extensions/test_sqlspec_spanner.py`; dependency category `unit`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Rejects delay, parametrized over sync driver, async driver, SQLSpec sync, SQLSpec async | `batch_write([[mutation]], max_commit_delay_ms=5)` | `TypeError` |
| Still writes | `driver.batch_write([[mutation]])` on the fake-backed `SyncSpannerDriver` | Existing per-group status list |
| Change-stream flag | `driver.batch_write([[mutation]], exclude_txn_from_change_streams=True)` | Fake client receives `True` |
| Empty input | `driver.batch_write([])` | `[]`, fake client not called |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py src/py/tests/unit/extensions/test_sqlspec_spanner.py -q -k batch_write`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py src/py/tests/unit/extensions/test_sqlspec_spanner.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/spanner src/py/vertebra/extensions/sqlspec/spanner.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/spanner src/py/vertebra/extensions/sqlspec/spanner.py
  uv run --no-sync python -m mypy.stubtest vertebra._spanner
  ```
- Planned local commit: `fix(spanner): remove inert max_commit_delay_ms from batch_write`

## Acceptance Criteria

- [ ] No Spanner `batch_write` accepts `max_commit_delay_ms` (R22).

## Notes & Discoveries

- `BatchWriteRequest` has no commit-delay field, which is why the parameter is removed rather than forwarded.

[^spanner-driver-py]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver.batch_write` — `batch_write` discarding `max_commit_delay_ms`.
[^sqlspec-spanner-py]: `src/py/vertebra/extensions/sqlspec/spanner.py` — SQLSpec Spanner drivers forwarding `max_commit_delay_ms` into `batch_write`.
[^spanner-pyi]: `src/py/vertebra/_spanner.pyi` — Native `batch_write` signature without a commit-delay argument.
