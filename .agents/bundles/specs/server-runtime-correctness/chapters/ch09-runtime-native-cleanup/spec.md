---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch09-runtime-native-cleanup
title: "Chapter 09: Fail-Loud UUID Entropy, Direct Native Bindings, Spanner batch_write Signature, and Broker Latency"
description: Make UUID entropy fail loud with fork-safe reseeding, bind native zero-copy helpers directly, drop the module cache that duplicates sys.modules, remove the inert Spanner batch_write commit-delay parameter, and remove the fixed per-chunk sleep in EmbeddingBroker.submit_batch.
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
created_at: "2026-11-12T01:35:00Z"
updated_at: "2026-11-12T01:35:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T01:35:00Z"
sources:
  - id: id-rs
    resource: src/rs/vertebra-core/src/id.rs::fill_os_random
    title: UUID entropy source with time-seeded fallback and no fork detection
  - id: zero-copy-py
    resource: src/py/vertebra/utils/serializers/_zero_copy.py
    title: Zero-copy codec module with pure-Python fallbacks for native helpers
  - id: module-loader-py
    resource: src/py/vertebra/utils/module_loader.py::import_optional
    title: Optional import helper with a cache duplicating sys.modules
  - id: spanner-driver-py
    resource: src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver.batch_write
    title: Spanner batch_write that discards max_commit_delay_ms
  - id: broker-py
    resource: src/py/vertebra/embeddings/_broker.py::EmbeddingBroker.submit_batch
    title: Bulk embedding submission with a fixed per-chunk sleep
tags:
  - entropy
  - native
  - spanner
  - embeddings
  - cleanup
---

# Chapter 09: Fail-Loud UUID Entropy, Direct Native Bindings, Spanner batch_write Signature, and Broker Latency

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`fill_os_random` in `vertebra-core` falls back to a `SystemTime`-seeded splitmix stream when `getrandom(2)` or `/dev/urandom` fails, and `THREAD_ENTROPY` keeps its 256-byte pool across `fork()`, so a forked child that inherits a partly consumed pool emits the same UUIDs as its parent [^id-rs].

`_zero_copy.py` binds `pack_grpc_lpm_frame`, `hydrate_one_vectorcall`, `hydrate_rows_vectorcall`, `instantiate_struct_vectorcall`, and `orm_to_struct_vectorcall` through `getattr(_native_vtb, ..., None) or _py_*`, although `vertebra._vertebra` is a mandatory extension [^zero-copy-py]. `import_optional` stores every module in `vertebra._typing._module_cache`, which duplicates `sys.modules` [^module-loader-py].

`SyncSpannerDriver.batch_write` and `AsyncSpannerDriver.batch_write` accept `max_commit_delay_ms` and discard it (`_ = max_commit_delay_ms`); Spanner's `BatchWriteRequest` has no commit-delay field, and the SQLSpec Spanner drivers forward the argument into this no-op [^spanner-driver-py]. `EmbeddingBroker.submit_batch` awaits `asyncio.sleep(0.002)` after every chunk, adding a fixed 2 ms per chunk on top of the `asyncio.sleep(0)` yield already taken before each chunk [^broker-py].

### 1.2 Non-Goals

- UUID formatting, `UUID7_STATE` ordering, and the nanoid alphabet.
- Other `import_optional` callers and `module_available` caching (`_dependency_cache` stays).
- `max_commit_delay_ms` on `apply_mutations`, `commit_mutations`, `run_in_transaction`, or `SpannerConfig`; those paths forward it to `CommitRequest`.
- The interactive-query preemption order in `EmbeddingBroker.submit_batch`.

### 1.3 Upstream Baseline & Code Analysis

- The Cargo workspace lockfile already resolves `getrandom 0.3.4`; `vertebra-core` has no direct dependency on it.
- `vertebra/_vertebra.pyi` declares the five zero-copy helpers, so stubtest covers direct imports.
- `vertebra._typing.reset_dependency_cache` clears `_module_cache`; `vertebra.typing` re-exports `_module_cache`; `src/py/tests/unit/framework/test_typing.py` asserts on it.
- `vertebra.extensions.sqlspec.spanner` sync and async drivers call `self._core.batch_write(mutation_groups, max_commit_delay_ms=max_commit_delay_ms)`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R20** (`t01`): `fill_os_random` uses `getrandom::fill` and panics on failure; every thread entropy pool refills after `fork()`.
- **R21** (`t02`, `t03`): `_zero_copy.py` binds the five native helpers directly from `vertebra._vertebra`; `import_optional` relies on `sys.modules` with `_module_cache` removed.
- **R22** (`t04`): No Spanner `batch_write` (driver or SQLSpec, sync or async) accepts `max_commit_delay_ms`.
- **R23** (`t02`): `EmbeddingBroker.submit_batch` applies no fixed per-chunk sleep.

### 2.2 Concrete Type & Signature Contracts

```rust
// src/rs/vertebra-core/src/id.rs
static FORK_GENERATION: AtomicU64;
struct EntropyPool { buf: [u8; ENTROPY_POOL_BYTES], pos: usize, generation: u64 }
fn fill_os_random(dest: &mut [u8]);
```

```python
# src/py/vertebra/utils/serializers/_zero_copy.py
from vertebra._vertebra import (
    hydrate_one_vectorcall,
    hydrate_rows_vectorcall,
    instantiate_struct_vectorcall,
    orm_to_struct_vectorcall,
    pack_grpc_lpm_frame,
)

# src/py/vertebra/utils/module_loader.py
def import_optional(module_name: str) -> Any | None: ...

# src/py/vertebra/db/adapters/spanner/driver.py
class SyncSpannerDriver:
    def batch_write(self, mutation_groups: Sequence[MutationGroup | Sequence[Mutation | Mapping[str, Any]]], *, exclude_txn_from_change_streams: bool = False) -> list[dict[str, Any]]: ...
class AsyncSpannerDriver:
    async def batch_write(self, mutation_groups: Sequence[MutationGroup | Sequence[Mutation | Mapping[str, Any]]], *, exclude_txn_from_change_streams: bool = False) -> list[dict[str, Any]]: ...
```

### 2.3 Caller → Seam → Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| `uuid4_u128`, `uuid7_u128`, `nanoid` | `EntropyPool::fill_bytes` | `getrandom::fill`, `FORK_GENERATION` |
| gRPC framing and struct hydration callers | `_zero_copy` module attributes | `vertebra._vertebra` |
| Optional-dependency callers | `import_optional` | `sys.modules`, `module_available` |
| SQLSpec Spanner drivers | `SyncSpannerDriver.batch_write` / `AsyncSpannerDriver.batch_write` | `_spanner` `batch_write` |
| Embedding callers | `EmbeddingBroker.submit_batch` | `asyncio.to_thread`, `_drain_pending_once` |

### 2.4 Edge-Case & Failure-Mode Matrix

| Case | Behaviour |
| --- | --- |
| OS entropy unavailable | `fill_os_random` panics with `vertebra-core: OS entropy source unavailable: <error>` |
| Fork in a thread that already drew UUIDs | Child refills its pool before the next draw; parent and child UUIDs differ |
| `vertebra._vertebra` missing | `ImportError` at `_zero_copy` import |
| `sys.modules[name] is None` | `import_optional` returns `None` |
| `batch_write(..., max_commit_delay_ms=5)` | `TypeError` (unexpected keyword argument) |
| `submit_batch` with pending interactive requests | Pending requests drain before and after each chunk, as today |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Inert `max_commit_delay_ms` on `batch_write` | Remove the parameter | Raise `ValueError` when set (keeps a parameter that can never work, since `BatchWriteRequest` has no commit-delay field). |
| Fork detection | `pthread_atfork` child handler bumps `FORK_GENERATION`; pools compare a cached generation | `std::process::id()` on every draw (a syscall per UUID on glibc ≥ 2.25). |
| Fixed 2 ms per-chunk sleep | Remove it; keep the existing `asyncio.sleep(0)` yield before each chunk | A tuned delay (no measurement justifies a fixed latency tax; `asyncio.to_thread` already releases the loop during inference). |

## 3. Documentation & OKF Knowledge Plan

The UUID entropy panic is recorded in the limitations chapter by `ch10:t04`. No public `docs/**` page documents the `_py_*` fallbacks, `_module_cache`, the `batch_write` commit-delay argument, or the broker sleep; `docs/reference/native-stubs.md` lists `batch_write` by name only.

## 4. Task Plan

- [ ] [t01](tasks/t01-uuid-entropy-fail-loud-and-fork-reseed.md) — `getrandom` entropy with panic on failure and fork-generation reseed.
- [ ] [t02](tasks/t02-zero-copy-native-bindings-and-broker-sleep.md) — Direct native zero-copy bindings and removal of the broker per-chunk sleep.
- [ ] [t03](tasks/t03-module-cache-removal.md) — `import_optional` over `sys.modules` with `_module_cache` deleted.
- [ ] [t04](tasks/t04-spanner-batch-write-signature.md) — Remove `max_commit_delay_ms` from the Spanner driver and SQLSpec `batch_write`.

**Chapter Checkpoint:** `make lint-all && make rs-test && make py-test` exits `0`.

[^id-rs]: `src/rs/vertebra-core/src/id.rs::fill_os_random` — UUID entropy source with time-seeded fallback and no fork detection.
[^zero-copy-py]: `src/py/vertebra/utils/serializers/_zero_copy.py` — Zero-copy codec module with pure-Python fallbacks for native helpers.
[^module-loader-py]: `src/py/vertebra/utils/module_loader.py::import_optional` — Optional import helper with a cache duplicating `sys.modules`.
[^spanner-driver-py]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver.batch_write` — Spanner `batch_write` that discards `max_commit_delay_ms`.
[^broker-py]: `src/py/vertebra/embeddings/_broker.py::EmbeddingBroker.submit_batch` — Bulk embedding submission with a fixed per-chunk sleep.
