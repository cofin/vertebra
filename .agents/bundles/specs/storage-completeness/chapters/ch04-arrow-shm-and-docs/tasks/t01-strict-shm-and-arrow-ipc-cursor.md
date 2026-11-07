---
type: Task
id: "storage-completeness:ch04:t01"
chapter_id: ch04-arrow-shm-and-docs
title: Enforce fail-loud allocate_shm_region and valid Arrow IPC in ArrowCursor.from_rows
description: Remove the anonymous ShmRegion::create fallback in vertebra_db::arrow_shm::allocate_shm_region and the JSON pseudo-Arrow fallback in vertebra.db.arrow.ArrowCursor.from_rows.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/rs/vertebra-db/src/arrow_shm.rs
  - src/py/vertebra/db/arrow.py
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
  - .agents/skills/runtime-ipc/SKILL.md
files:
  - src/rs/vertebra-db/src/arrow_shm.rs
  - src/py/vertebra/db/arrow.py
tests:
  - src/rs/vertebra-db/src/arrow_shm.rs
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
estimated_loc: 160
plan_revision: 2
plan_commit: cdb978d3
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-07T04:15:00Z"
commit: null
sources:
  - id: arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region
    title: Named shared-memory allocator in vertebra-db
  - id: db-arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Python ArrowCursor IPC stream wrapper
tags:
  - storage
  - arrow
  - shm
  - rust
  - python
---

# Task t01: Enforce Fail-Loud `allocate_shm_region` and Valid Arrow IPC in `ArrowCursor.from_rows`

## Objective & Scope Boundary

Eliminate two silent data-plane fallbacks: (1) in `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region`, stop falling back to anonymous `ShmRegion::create(alloc_size)` when `ShmRegion::create_named` fails on Unix and instead return `Err(ArrowShmError)` immediately [^arrow-shm-rs]; and (2) in `src/py/vertebra/db/arrow.py::ArrowCursor.from_rows`, delete the `encode_json_bytes(dict_list)` pseudo-Arrow fallback so `from_rows` always writes valid Arrow IPC stream bytes (or raises `MissingDependencyError` when `pyarrow` is uninstalled) [^db-arrow-py].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/arrow_shm.rs` — `allocate_shm_region`, `write_arrow_to_shm`, and inline Rust unit tests [^arrow-shm-rs].
- `src/py/vertebra/db/arrow.py` — `ArrowCursor`, `from_rows`, `to_arrow`, and `to_pylist` [^db-arrow-py].
- `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — Arrow data-plane and `ArrowCursor` unit tests.
- `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — `VTBARROW` SHM ownership invariants.
- `.agents/skills/runtime-ipc/SKILL.md` — POSIX SHM naming and lifecycle rules.

## Symbol & Signature Contracts

- `src/rs/vertebra-db/src/arrow_shm.rs`:
  - Define `pub type ArrowShmError = String;` (or `#[derive(Debug, Clone, PartialEq, Eq)] pub struct ArrowShmError(pub String)` implementing `Display`, `Error`, and `From<ArrowShmError> for String` so existing callers expecting `Result<ShmRegion, ArrowShmError>` compile cleanly).
  - On `#[cfg(unix)]`, `pub fn allocate_shm_region(shm_name: &str, payload_len: usize) -> Result<ShmRegion, ArrowShmError>`:
    - Calls `ShmRegion::create_named(norm_name, alloc_size)` and maps `Err(e)` directly to `Err(...)` containing `norm_name` and `e` — zero `ShmRegion::create(alloc_size)` fallback.
- `src/py/vertebra/db/arrow.py`:
  - `ArrowCursor.from_rows(cls, rows: Sequence[Mapping[str, Any]], *, schema: Any = None) -> ArrowCursor`:
    - Requires `pyarrow` via `import_optional("pyarrow")`; if `None`, raises `MissingDependencyError("pyarrow", install_package="pyarrow", extra="arrow")`.
    - Serializes `pa.Table.from_pylist(list(rows), schema=schema)` into a `pa.BufferOutputStream` via `pa.ipc.new_stream(...)` so `_ipc_bytes` always starts with valid Arrow IPC stream continuation framing (`b"\xff\xff\xff\xff"`), including when `rows == []`.
  - Remove any JSON-decoding fallback in `ArrowCursor` methods that existed solely to read `encode_json_bytes` payloads from `from_rows`.

## Edge Cases & Error Semantics

- Passing an invalid POSIX SHM name with embedded interior slashes after leading `/` (e.g., `"/bad/nested/seg"` on Linux `shm_open` returning `EINVAL`/`ENOENT`) or empty name `""` causes `allocate_shm_region` to return `Err(...)` deterministically.
- `ArrowCursor.from_rows([])` with `pyarrow` installed produces valid empty Arrow IPC stream bytes readable by `pyarrow.ipc.open_stream(cursor.as_bytes()).read_all()` (`num_rows == 0`).
- Masking `pyarrow` unavailable causes `ArrowCursor.from_rows([{"id": 1}])` to raise `MissingDependencyError`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-db/src/arrow_shm.rs` (`#[cfg(test)]`) and `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Named SHM failure fails loud on Unix | `allocate_shm_region("/invalid/nested/shm/path", 4096)` | Returns `Err(err)` containing `"/invalid/nested/shm/path"`; zero anonymous SHM segment allocated. |
| Valid named SHM round-trip | `write_arrow_to_shm("/vtb-arrow-test-ok", b"ARROW_IPC")` + `ShmRegion::open_named` | Succeeds, reads exact payload, and unlinks cleanly. |
| `ArrowCursor.from_rows` non-empty & empty | `ArrowCursor.from_rows([{"id": 1, "name": "a"}])` and `ArrowCursor.from_rows([])` | `cursor.as_bytes()[:4] == b"\xff\xff\xff\xff"`; `pa.ipc.open_stream(cursor.as_bytes()).read_all().to_pylist()` equals input rows. |
| `ArrowCursor.from_rows` without `pyarrow` | Mask `pyarrow` unavailable and call `ArrowCursor.from_rows([{"id": 1}])` | Raises `MissingDependencyError` mentioning `pyarrow`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db arrow_shm && uv run --no-sync pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -q`
- Final Green + Clippy + lint gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-db --all-targets -- -D warnings && uv run --no-sync ruff check src/py/vertebra/db/arrow.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py && uv run --no-sync mypy src/py/vertebra/db/arrow.py && uv run --no-sync pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -q`

## Acceptance Criteria

- [ ] `allocate_shm_region` returns `Err` when `ShmRegion::create_named` fails without falling back to `ShmRegion::create` (R8).
- [ ] `ArrowCursor.from_rows` always emits valid Arrow IPC stream bytes and raises `MissingDependencyError` when `pyarrow` is absent (R8).

## Notes & Discoveries

- [2026-11-07 04:15] Clean up `/dev/shm/*vtb-arrow-test*` in Rust unit tests via `Drop` / explicit unlink so test runs leave `/dev/shm` clean.

[^arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` — Named SHM allocator for Arrow IPC frames.
[^db-arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Python Arrow IPC result cursor.
