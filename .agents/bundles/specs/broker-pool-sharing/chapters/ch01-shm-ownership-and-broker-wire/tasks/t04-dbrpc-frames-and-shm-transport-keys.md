---
type: Task
id: broker-pool-sharing:ch01-shm-ownership-and-broker-wire:t04
flow_id: broker-pool-sharing
chapter_id: ch01-shm-ownership-and-broker-wire
title: Implement Binary DBRPC Wire Frames, SHM Transport Keys, and In-Memory Fallback Removal
description: Define binary DBRPC request/response frames, propagate deterministic SHM transport keys, and remove silent in-memory broker fallbacks.
status: stable
state: completed
commit: 936110fc
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 1b191388
depends_on:
- broker-pool-sharing:ch01-shm-ownership-and-broker-wire:t03
files:
- src/rs/vertebra-core/src/dbrpc/mod.rs
- src/rs/vertebra-core/src/lib.rs
- src/py/vertebra/extensions/broker/dbrpc.py
- src/py/vertebra/extensions/broker/transport.py
- src/py/vertebra/extensions/broker/plugin.py
- src/py/vertebra/extensions/alchemy/broker.py
tests:
- src/rs/vertebra-core/tests/it/broker.rs
- src/py/tests/unit/extensions/test_broker_transport.py
- src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
- src/py/tests/unit/extensions/test_shm_broker.py
read_set:
- src/rs/vertebra-core/src/transport/ring_pair.rs
- src/rs/vertebra-core/src/lib.rs
- src/py/vertebra/utils/serializers.py
- src/py/vertebra/extensions/broker/transport.py
- src/py/vertebra/extensions/broker/plugin.py
- src/py/vertebra/extensions/alchemy/broker.py
estimated_loc: 220
tags:
- dbrpc
- ipc
- msgspec
- broker
- python
- debloat
sources:
- id: ring-pair
  resource: src/rs/vertebra-core/src/transport/ring_pair.rs
  title: Bidirectional SHM ring pair transport
- id: core-broker
  resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
  title: Shared-memory lease table, statement cache, dialect tags, and worker reclamation
- id: broker-transport
  resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
  title: Python SHM broker transports and lease objects
- id: broker-plugin
  resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
  title: BrokerPlugin lifecycle and metric snapshot integration
- id: alchemy-cache
  resource: src/py/vertebra/extensions/alchemy/broker.py::SyncSHMSessionBroker
  title: Alchemy session broker after StatementCache removal
---

# Task `t04`: Implement Binary DBRPC Wire Frames, SHM Transport Keys, and In-Memory Fallback Removal

## Objective & Scope Boundary

Implement `vertebra_core::dbrpc` (`DBRPC_VERSION = 1`, `DbRpcOp`, `DbRpcHeader`) and `vertebra.extensions.broker.dbrpc` (`DbRpcRequest`, `DbRpcResponse`) per [`../spec.md` §2](../spec.md) [^ring-pair] [^core-broker]. Out of scope: engine pool execution (`ch03`–`ch04`).

Replace salted `hash()` in `get_or_compile_statement`, `_acquire_sync`, and `AsyncSHMBrokerTransport.acquire` with `stable_hash64`, and delete `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `use_shm=False`, and `alchemy/broker.py::StatementCache` per [`../spec.md` §2](../spec.md) [^broker-transport] [^broker-plugin] [^alchemy-cache].

## Bounded Read Manifest

- `src/rs/vertebra-core/src/transport/ring_pair.rs` — SHM ring frame payload conventions.
- `src/rs/vertebra-core/src/lib.rs` — Crate module exports.
- `src/py/vertebra/utils/serializers.py` — Centralized msgspec serialization helpers.

- `src/py/vertebra/extensions/broker/transport.py` — `get_or_compile_statement`, `_acquire_sync`, `AsyncSHMBrokerTransport.acquire`, and `_BaseInMemoryBrokerTransport`.
- `src/py/vertebra/extensions/broker/plugin.py` — `BrokerPlugin` `use_shm` branch and partition registration.
- `src/py/vertebra/extensions/alchemy/broker.py` — Dead `StatementCache` (`self._cache`) class.
- `src/py/tests/unit/extensions/test_broker_transport.py` — Existing transport unit tests.

## Contracts & Edge Cases

- Encode/decode fixed little-endian `DbRpcHeader` (`version`, `op`, `request_id`, `lease_slot`, `lease_generation`) with checked bounds.
- Declare `DbRpcRequest` and `DbRpcResponse` as `msgspec.Struct(..., frozen=True, gc=False, array_like=True)` using immutable tuple fields so `gc=False` cannot leak cycles.
- **Edge Case**: Truncated buffers or unknown `DbRpcOp` discriminants return `DbRpcError::{Truncated, InvalidOp}`.
- **Edge Case**: `version != DBRPC_VERSION` returns `DbRpcError::UnsupportedVersion(version)`.

- Route all statement, `pool_key`, and `bind_key` hashes in `transport.py` and `plugin.py` through `vertebra._vertebra.stable_hash64`.
- Delete `_BaseInMemoryBrokerTransport`, `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `use_shm=False`, and `alchemy/broker.py::StatementCache` with zero aliases.
- **Edge Case**: Passing `bind_key=None` uses `0` without hashing `"None"`.
- **Edge Case**: Attempting `SyncInMemoryBrokerTransport` import raises `ImportError` (`PY-07` clean break).

## Test Matrix

- Round-trip every `DbRpcOp` (`Execute`..`Close`) across Rust header codec and Python `DbRpcRequest`/`DbRpcResponse`.
- Parameterized malformed/truncated/version-mismatch byte slices fail cleanly without panicking.

- `@pytest.mark.parametrize` verifies deterministic keys across `get_or_compile_statement`, `_acquire_sync`, and `AsyncSHMBrokerTransport.acquire`.
- Confirm zero occurrences of `hash(` in `src/py/vertebra/extensions/broker/transport.py`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_broker_transport.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py src/py/tests/unit/extensions/test_shm_broker.py -q && uv run --no-sync ruff check src/py/vertebra/extensions && uv run --no-sync mypy src/py/vertebra/extensions
```

## Acceptance Criteria

- [x] Rust `dbrpc` and Python `dbrpc.py` agree byte-for-byte on header layout and op codes.
- [x] `DbRpcRequest` and `DbRpcResponse` obey `PY-04` acyclic `gc=False, array_like=True` rules.
- [x] Zero salted `hash()` calls remain in `extensions/broker/transport.py`.
- [x] In-memory broker transports and `alchemy/broker.py::StatementCache` are deleted cleanly.
- [x] Zero regressions in the fast verification gate.

[^ring-pair]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Bidirectional SHM ring pair transport.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/broker.py::StatementCache` — Unused Python StatementCache in alchemy broker module.
