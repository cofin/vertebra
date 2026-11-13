---
type: Spec
flow_id: server-runtime-correctness
chapter_id: ch06-runtime-native-cleanup
title: "Chapter 06: Runtime Entropy, Native Binding, and Spanner Signature Cleanup"
description: "Enforce fail-loud getrandom UUID entropy with post-fork reseeding, delete _module_cache, bind _zero_copy helpers directly from _vertebra, and drop inert max_commit_delay_ms on Spanner batch_write."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters: []
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: null
updated_at: "2026-11-13T06:01:30Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - entropy
  - ffi
  - typing
  - spanner
  - cleanup
sources:
  - id: id-rs
    resource: "src/rs/vertebra-core/src/id.rs::fill_os_random"
    title: UUID entropy generator with splitmix fallback and no fork reseed
  - id: module-loader-py
    resource: "src/py/vertebra/utils/module_loader.py::import_optional"
    title: Optional module loader duplicating sys.modules in _module_cache
  - id: zero-copy-py
    resource: src/py/vertebra/utils/serializers/_zero_copy.py
    title: "Zero-copy codec helpers carrying dead _py_* fallbacks"
  - id: spanner-driver-py
    resource: "src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver.batch_write"
    title: Spanner batch_write discarding max_commit_delay_ms
---

# Chapter 06: Runtime Entropy, Native Binding, and Spanner Signature Cleanup

<!-- truth: start -->
- `vertebra-core/src/id.rs` fills entropy exclusively through `getrandom::fill` (panicking on OS entropy failure) and increments an atomic `FORK_GENERATION` in a `pthread_atfork` child handler so forked workers never emit duplicate UUIDs [^id-rs].
- `import_optional` reads `sys.modules` directly and `_module_cache` is deleted from `module_loader.py`, `_typing.py`, and `typing.py` [^module-loader-py].
- `_zero_copy.py` binds its five helpers directly from mandatory `vertebra._vertebra` (deleting `_py_*` fallbacks), and sync/async Spanner `batch_write` in `driver.py` and `extensions/sqlspec/spanner.py` drop the ignored `max_commit_delay_ms` parameter [^zero-copy-py] [^spanner-driver-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`fill_os_random` in `vertebra-core` silently degrades to a `SystemTime` splitmix PRNG on OS failure and reuses thread-local 256-byte pools across `fork()` [^id-rs]. Python `import_optional` duplicates `sys.modules` in `_module_cache` [^module-loader-py], `_zero_copy.py` keeps `_py_*` fallbacks despite mandatory `_vertebra` [^zero-copy-py], and Spanner `batch_write` accepts `max_commit_delay_ms` even though `BatchWriteRequest` has no commit-delay field [^spanner-driver-py].

## 2. Requirements, Concrete Contracts & Architecture

- **`R9a` (`t01`)**: Add `getrandom` workspace dependency to `src/rs/Cargo.toml` and `src/rs/vertebra-core/Cargo.toml`; replace `fill_os_random` in `id.rs` with `getrandom::fill(dest).unwrap_or_else(|e| panic!("vertebra-core: OS entropy source unavailable: {e}"))`; register a Unix `libc::pthread_atfork` child handler incrementing `FORK_GENERATION: AtomicU64` so `EntropyPool` reseeds immediately after `fork()`.
- **`R9b` (`t02`)**: Delete `_module_cache` from `src/py/vertebra/_typing.py`, `src/py/vertebra/typing.py` (`__all__` and re-exports), and `src/py/vertebra/utils/module_loader.py`, resolving `import_optional` through `sys.modules` (`None` sentinel honored) and `_dependency_cache`.
- **`R9c` (`t03`)**: Import `hydrate_one_vectorcall`, `hydrate_rows_vectorcall`, `instantiate_struct_vectorcall`, `orm_to_struct_vectorcall`, and `pack_grpc_lpm_frame` directly from `vertebra._vertebra` in `_zero_copy.py` (deleting `_py_*` fallbacks), and remove `max_commit_delay_ms` from `SyncSpannerDriver.batch_write`, `AsyncSpannerDriver.batch_write`, and SQLSpec sync/async Spanner `batch_write`.

## 3. Documentation & OKF Knowledge Plan

The fail-loud OS entropy panic contract is recorded in `server-runtime-limitations.md` and direct `_zero_copy` FFI bindings are synced in `pyo3-ffi-and-batch-dispatch.md` (`ch07`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R9a` | `t01` | `src/rs/Cargo.toml`, `src/rs/vertebra-core/{Cargo.toml,src/id.rs}` | `cargo test -p vertebra-core --lib id::` |
| `R9b` | `t02` | `src/py/vertebra/{_typing,typing}.py`, `utils/module_loader.py` | `pytest src/py/tests/unit/framework/test_typing.py` |
| `R9c` | `t03` | `utils/serializers/_zero_copy.py`, `db/adapters/spanner/driver.py`, `extensions/sqlspec/spanner.py` | `pytest test_vector_wire_codecs.py test_spanner.py test_sqlspec_spanner.py` |

## 5. Implementation Plan

- [x] [db02a0b6] [t01-uuid-entropy-fail-loud-and-fork-reseed.md](tasks/t01-uuid-entropy-fail-loud-and-fork-reseed.md) — Enforce `getrandom::fill` panic-on-failure and `pthread_atfork` pool reseeding in `vertebra-core/src/id.rs`
- [x] [db02a0b6] [t02-module-cache-removal.md](tasks/t02-module-cache-removal.md) — Remove `_module_cache` from `_typing.py`, `typing.py`, and `module_loader.py`
- [x] [db02a0b6] [t03-direct-native-bindings-and-spanner-batch-write.md](tasks/t03-direct-native-bindings-and-spanner-batch-write.md) — Bind `_zero_copy.py` helpers directly from `vertebra._vertebra` and remove `max_commit_delay_ms` from Spanner `batch_write`

## 6. Current State & Remaining Plan Snapshot

Completed (`3/3` tasks at `db02a0b6`).

- **Fast Chapter Gate (`< 25s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --lib id:: && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core -- -D warnings && uv run --no-sync ruff check src/py/vertebra/_typing.py src/py/vertebra/typing.py src/py/vertebra/utils src/py/vertebra/db/adapters/spanner/driver.py src/py/vertebra/extensions/sqlspec/spanner.py && uv run --no-sync mypy src/py/vertebra/typing.py src/py/vertebra/utils src/py/vertebra/db/adapters/spanner/driver.py src/py/vertebra/extensions/sqlspec/spanner.py && uv run --no-sync pytest src/py/tests/unit/framework/test_typing.py src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/extensions/test_sqlspec_spanner.py -q`.

[^id-rs]: `src/rs/vertebra-core/src/id.rs::fill_os_random` — UUID entropy generator with splitmix fallback and no fork reseed.
[^module-loader-py]: `src/py/vertebra/utils/module_loader.py::import_optional` — Optional module loader duplicating sys.modules in _module_cache.
[^zero-copy-py]: `src/py/vertebra/utils/serializers/_zero_copy.py` — Zero-copy codec helpers carrying dead _py_* fallbacks.
[^spanner-driver-py]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver.batch_write` — Spanner batch_write discarding max_commit_delay_ms.
