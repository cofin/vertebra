---
type: Task
id: storage-completeness:ch04:t01
chapter_id: ch04-arrow-shm-and-docs
title: Preserve ArrowCursor APIs while enforcing named SHM and real IPC
description: Remove anonymous allocation after named SHM failure and JSON pseudo-Arrow
  from ArrowCursor.from_rows. Preserve public cursor construction, tuple/mapping rows,
  columns, threshold, and live-view ownership. Unrelated write_arrow_shm input policy
  remains outside this task.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
- src/rs/vertebra-db/src/arrow_shm.rs
- src/py/vertebra/db/arrow.py
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
- src/rs/vertebra-py/src/db.rs
- src/py/tests/unit/db/test_enterprise_olap_drivers.py
- .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
files:
- src/rs/vertebra-db/src/arrow_shm.rs
- src/py/vertebra/db/arrow.py
tests:
- src/rs/vertebra-db/src/arrow_shm.rs
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
estimated_loc: 180
plan_revision: 9
plan_commit: 222cbf99
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-08T00:00:00Z'
updated_at: '2026-11-08T00:00:00Z'
commit: null
sources:
- id: shm
  resource: src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region
  title: Named frame allocation
- id: cursor
  resource: src/py/vertebra/db/arrow.py::ArrowCursor
  title: Public cursor
tags:
- storage
- arrow
---
# Task t01: Preserve ArrowCursor APIs while enforcing named SHM and real IPC

## Objective & Scope Boundary

Remove anonymous allocation after named SHM failure and JSON pseudo-Arrow from ArrowCursor.from_rows. Preserve public cursor construction, tuple/mapping rows, columns, threshold, and live-view ownership. Unrelated write_arrow_shm input policy remains outside this task.

The implementation uses src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region [^shm].

The implementation uses src/py/vertebra/db/arrow.py::ArrowCursor [^cursor].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/arrow_shm.rs` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/vertebra/db/arrow.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — owning implementation, collaborator contract, or canonical verification surface.
- `src/rs/vertebra-py/src/db.rs` — owning implementation, collaborator contract, or canonical verification surface.
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — owning implementation, collaborator contract, or canonical verification surface.
- `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — owning implementation, collaborator contract, or canonical verification surface.

## Symbol & Signature Contracts

- Keep existing `ArrowShmError = String` and private `fn allocate_shm_region(norm_name: &str, alloc_size: usize) -> Result<ShmRegion, String>`; caller owns normalization, header sizing, and page alignment. Unix `create_named` errors retain normalized name and OS error. Non-Unix existing allocation stays supported.
- Keep `ArrowCursor.from_rows(cls, rows: Sequence[Any], columns: Sequence[str] | None = None, *, threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS) -> ArrowCursor`. Require `pyarrow` before serialization; produce `pa.ipc.new_stream` bytes for tuple, mapping, and empty rows. Preserve column inference and row-count behavior.
- Caller -> `from_rows` -> native `ArrowCursor.write_ipc_to_shm(name, payload, row_count)` for threshold-selected SHM. Propagate native allocation RuntimeError; remove suppression followed by NativeShmCursor/anonymous mmap fallback. Missing native module/class on a required SHM path raises the appropriate MissingDependencyError, rather than claiming SHM ownership.
- Preserve `_buffer`, `buffer_view()`, `to_arrow_table()`, `to_pylist()`, release semantics, and bytes underlying live native views. Remove JSON decoding used solely for from_rows payloads. Public Rust frame writer is `write_shm_arrow_frame`, not write_arrow_to_shm.

## Edge Cases & Error Semantics

Missing pyarrow raises `MissingDependencyError("pyarrow", install_package="pyarrow", extra="arrow")`. Empty input emits a valid empty schema IPC stream. Invalid `/bad/nested/seg` and empty normalized POSIX names fail without registering an anonymous region. Use unique short names; clean only allocations owned by the test. SHM threshold changes storage backing, never row/column semantics.

## Public-Seam Test Blueprint

Canonical tests: `src/rs/vertebra-db/src/arrow_shm.rs`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`. Real installed PyArrow and explicit memory/local native stores supply codec and I/O behavior; narrow spies observe requests and cleanup.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Invalid named allocation | `write_shm_arrow_frame("/bad/nested/seg", b"payload")` on Unix | Err contains normalized name; registry has no entry. |
| Tuple/mapping/empty IPC | `[(1,"a")]` with `["id","name"]`, `[{"id":1}]`, `[]` | `pa.ipc.open_stream(cursor.buffer_view()).read_all()` yields exact values or zero rows. |
| Native allocation rejection | Patch native writer to raise RuntimeError, `threshold_rows=1` | Exact error propagates; anonymous mmap is never invoked. |
| Missing optional codec | Mask import_optional for pyarrow | MissingDependencyError mentions pyarrow. |
| Retained view | Existing native cursor release/unlink test | View decodes after release; release is idempotent. |

## Verification

Red: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db arrow_shm` and `uv run --no-sync pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -q`.

Green: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-db --all-targets -- -D warnings`. Rebuild with `make dev` before Python verification. Run `uv run --no-sync pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_enterprise_olap_drivers.py -q`, `uv run --no-sync ruff check src/py/vertebra/db/arrow.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, and `uv run --no-sync mypy src/py/vertebra/db/arrow.py`. Run the five-extension stubtest gate from the pyo3-bridge skill.

## Acceptance Criteria

- [ ] R8 named allocation failure propagates through native and Python threshold-selected paths without anonymous fallback.
- [ ] R8 from_rows produces only real IPC and preserves public API and retained-view lifetime.

## Notes & Discoveries

- [2026-11-08 00:00] PyArrow 26.0.1 exposes synchronous Python file callbacks. Async native I/O stays on the caller loop; existing `SyncPool` executes codecs. Documentation targets belong to ch05:t02.

[^shm]: `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` — Named frame allocation.
[^cursor]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public cursor.
