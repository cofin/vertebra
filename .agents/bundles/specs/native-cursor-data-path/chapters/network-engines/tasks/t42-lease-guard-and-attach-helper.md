---
type: Task
id: "native-cursor-data-path:network:t42"
chapter_id: network-engines
title: "Lease guard in vertebra-db and the driver helper that attaches it"
description: "Add a Rust guard that owns one broker lease and returns it on drop, and the Python helper that acquires a lease and hands its release to a native client."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - "src/py/vertebra/db/driver/_common.py:148-200"
  - "src/py/vertebra/db/driver/_common.py::_BrokerLeasePlan"
  - "src/rs/vertebra-db/src/lib.rs"
  - "src/rs/vertebra-db/Cargo.toml"
files:
  - "src/rs/vertebra-db/src/lease_guard.rs"
  - "src/rs/vertebra-db/src/lib.rs"
  - "src/py/vertebra/db/driver/_common.py"
tests:
  - "src/rs/vertebra-db/src/lease_guard.rs"
  - "src/py/tests/unit/db/test_db_unified_driver_contracts.py"
estimated_loc: 180
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-20T16:37:35Z
commit: null
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-cursor-data-path/spec.md
    title: "Root spec with the lease contract"
  - id: driver-common
    resource: "src/py/vertebra/db/driver/_common.py::CommonDriverMixin"
    title: "Lease acquire and release helpers"
tags: [lease, rust, pyo3, python]
---

# Lease guard in vertebra-db and the driver helper that attaches it

## Objective & Scope Boundary

A broker lease has one owner in Rust. When that owner goes away for any reason, the lease goes back to the broker. The four network crates with broker leases reuse one guard type, and their drivers reuse one Python helper.

Out of scope:

- The client classes. The cursor pair tasks add `attach_lease` and `release_lease` to them.
- The drivers. The driver tasks call the helper.
- Changing how a lease is acquired. Acquire stays best effort.

## Bounded Read Manifest

- Root spec section 2, lease contract.
- `src/py/vertebra/db/driver/_common.py:148-200` holds `_lease_plan`, `_acquire_broker_lease`, and `_release_broker_lease`. Acquire waits up to `timeout_ms`, default `5000`. It returns `None` when the driver has no broker and when the broker raises `RuntimeError`.
- The class `_BrokerLeasePlan` in the same file holds the broker facts the release needs.
- `src/rs/vertebra-db/src/lib.rs` and `src/rs/vertebra-db/Cargo.toml` for the module list and the `pyo3` feature.

## Contracts & Edge Cases

`src/rs/vertebra-db/src/lease_guard.rs`, compiled under the crate's `pyo3` feature and declared in `lib.rs` as `pub mod lease_guard;` behind the same feature:

```rust
pub struct LeaseGuard { release: Option<Py<PyAny>> }

impl LeaseGuard {
    pub fn new(release: Py<PyAny>) -> Self;
    pub fn release(&mut self, py: Python<'_>);
    pub fn is_held(&self) -> bool;
}

impl Drop for LeaseGuard { fn drop(&mut self); }
```

- `release` takes the stored callable, calls it with no arguments, and reports a raised error as unraisable through PyO3's `write_unraisable`. A second call does nothing.
- `drop` does nothing when the callable was already taken. Otherwise it attaches to the interpreter with PyO3's `attach` and calls `release`.
- The guard is `Send`. It may be dropped on a worker thread.

`src/py/vertebra/db/driver/_common.py`:

```python
def _release_lease(broker: Any, plan: _BrokerLeasePlan, lease: Any) -> None:
    """Return a lease to its broker using the release shape the plan recorded."""

def _lease_releaser(broker: Any, plan: _BrokerLeasePlan, lease: Any) -> Callable[[], None]:
    """Return a zero-argument callable that releases the lease and holds no driver reference."""
```

- `_release_lease` holds the body `_release_broker_lease` has today, with `broker` and `plan` as arguments. `_release_broker_lease` keeps its signature and calls it.
- `_lease_releaser` returns `functools.partial(_release_lease, broker, plan, lease)`. The callable references the broker, the plan, and the lease only. It holds no reference to a driver or a client, so a driver dropped without `close()` is collected and its guard drops.
- New method on `CommonDriverMixin`: `def _attach_checkout_lease(self, client: Any, timeout_ms: int = 5000) -> bool`. It calls `self._acquire_broker_lease(timeout_ms)`. With `None` it returns `False` and attaches nothing. Otherwise it calls `client.attach_lease(_lease_releaser(self._broker, self._lease_plan(), lease))` and returns `True`.
- An async driver awaits its existing acquire helper and then makes the same `client.attach_lease` call.
- Neither new function is exported from `__all__`.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Release once | A new guard over a counting callable; `release` twice | One call |
| Drop releases | Guard dropped without `release` | One call |
| Drop after release | `release`, then drop | One call |
| Error in callable | Callable that raises | No panic; `is_held` is false |
| Releaser shape | `_lease_releaser(broker, plan, lease)()` with a recording broker double | One release recorded for that lease |
| No driver reference | `gc.get_referents` of the callable from `_lease_releaser` | Contains the broker, plan, and lease; no driver object |
| Refused acquire | `_attach_checkout_lease` with a broker double whose `acquire` raises `RuntimeError` | Returns `False`; the client double records no `attach_lease` call |
| Granted acquire | Same with a granting broker double | Returns `True`; one `attach_lease` call with a callable |
| Existing release path | `_release_broker_lease(lease)` on a driver | Same broker calls as before |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 lease_guard`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --all-targets -- -D warnings`
- `uv run --no-sync pytest src/py/tests/unit/db/test_db_unified_driver_contracts.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/driver/_common.py`
- `uv run --no-sync mypy src/py/vertebra/db/driver/_common.py`

## Acceptance Criteria

- [ ] Every matrix case has a test.
- [ ] `_release_broker_lease` behaves as before for every broker shape the plan covers.
- [ ] No driver file is edited.
