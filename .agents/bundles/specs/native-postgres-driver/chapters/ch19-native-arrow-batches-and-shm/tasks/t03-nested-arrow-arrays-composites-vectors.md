---
type: Task
id: "native-postgres-driver:ch19:t03"
chapter_id: ch19-native-arrow-batches-and-shm
title: Build recursive Arrow builders for arrays, composites, pgvector, and bitstrings
description: Extend PostgresArrowBatchBuilder in vertebra-postgres with recursive depth-32 builders for multidimensional PostgreSQL arrays, catalog composites, domains, enums, vector/halfvec, and bit/varbit columns.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch19:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch19-native-arrow-batches-and-shm/spec.md
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/src/types.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-db/src/postgres/mod.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/arrow.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs
estimated_loc: 235
plan_revision: 5
plan_commit: 0147d80c
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-10T03:44:09Z"
last_verified_commit: 4f9242ab
verification_evidence: "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests (13 passed); cargo fmt --check; cargo clippy -p vertebra-postgres --all-targets -- -D warnings; make review-check"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-10T03:44:09Z"
commit: 7d18bfda
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Exact PgArray, PgComposite, PgVector, halfvec, and PgBitString contracts
  - id: pg-vec-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::encode_pgvector_f16
    title: Existing IEEE 754 binary16 halfvec and pgvector dimension conventions
tags:
  - postgres
  - rust
  - arrow
  - nested-types
  - pgvector
---

# Task t03: Build Recursive Arrow Builders for Arrays, Composites, pgvector, and Bitstrings

## Objective & Scope Boundary

Extend `PostgresArrowBatchBuilder` in `src/rs/vertebra-postgres/src/arrow.rs` to recursively populate Arrow columns for PostgreSQL multidimensional arrays, ordered non-dropped catalog composites, domains, enums, `vector`/`halfvec` embeddings, and `bit`/`varbit` bitstrings under the `MAX_ARROW_NESTING_DEPTH = 32` and `max_bytes` (`8 MiB`) caps [^pg-contracts] [^pg-vec-rs]. `VTBARROW` shared-memory IPC framing follows in `ch19:t04`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 19 recursive nested Arrow column contracts (`arrays`, `composites`, `vector`/`halfvec`, `bit`/`varbit`, depth `32`).
- `src/rs/vertebra-postgres/src/arrow.rs` — `PostgresArrowSchema` (`ch19:t01`) and scalar `PostgresArrowBatchBuilder` (`ch19:t02`).
- `src/rs/vertebra-postgres/src/types.rs` — `TypeCatalog`, `TypeInfo`, `TypeKind`, and `CompositeField` composite attribute order, domain base types, enum OIDs, and `vector`/`halfvec` dynamic OID resolution (`ch11:t01`).
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `codecs::decode_value_with_catalog` dispatcher over `array.rs` (`PgArray` rank `0..=6`, lower bounds, flat row-major values, `ch11:t02`–`t03`), `composite.rs` (`PgComposite` non-dropped attributes, `ch11:t04`), and `PgVector` / `PgBitString` (`ch12:t01`).
- `src/rs/vertebra-db/src/postgres/mod.rs` — Shared `PgVector` (`vector` / `halfvec` `1..=16_000`, `half = "=2.7.1"`) and `PgBitString` (`bit` / `varbit` MSB-first zero-padded bytes) codecs (`ch12:t01`) [^pg-vec-rs].
- [contracts.md](../../../contracts.md) — Nested NULL vs empty-container validity rules [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/arrow.rs` (extending internal `ArrowColumnBuilder` tree inside `PostgresArrowBatchBuilder`):
  - **PostgreSQL Arrays (`Struct(dimensions: List<Int32>, lower_bounds: List<Int32>, values: List<T>)`)**:
    - SQL `NULL` array appends null parent `Struct` and null slots to `dimensions`, `lower_bounds`, and `values` list builders so child array lengths match parent length.
    - Rank-`0` empty array (`PgArray { values: [], dimensions: [], lower_bounds: [], .. }`) appends non-null parent `Struct` with empty non-null `dimensions = []`, `lower_bounds = []`, and `values = []` lists.
    - Non-empty `1..=6`-D arrays validate `dimensions.len() == lower_bounds.len()`, positive dimensions, and checked product `== values.len()`, preserving negative or non-`1` lower bounds verbatim without normalizing to `1`.
  - **Catalog Composites (`Struct(attr_0, attr_1, ...)`)**:
    - Follows ordered non-dropped catalog attributes; SQL `NULL` composite appends null parent `Struct` and recursive null slots to every child builder; non-`NULL` composite with all-`NULL` attributes appends non-null parent `Struct` with null child slots.
    - Supports nested arrays of composites, composites of arrays, domains over nested types, and enum elements without opaque binary fallback.
  - **`vector` / `halfvec`**:
    - Fixed typmod dimension `D` (`1..=16_000`): populates `FixedSizeListBuilder<Float32Builder>` of width `D`, rejecting non-`NULL` values whose `values.len() != D` with `PostgresError::Data`.
    - Dynamic/unknown typmod (`-1`): populates `ListBuilder<Float32Builder>` accepting variable `1..=16_000` lengths across rows.
    - `halfvec` decoded `f16` values widen losslessly to exact IEEE 754 `f32` (preserving `-0.0` and subnormals; rejecting `NaN`/`±Inf`).
  - **`bit` / `varbit` (`Struct(bytes: Binary, bit_length: Int32)`)**:
    - Validates `bit_length >= 0`, `bytes.len() == (bit_length as usize).div_ceil(8)`, and zeroed unused low bits in the final byte (`0`, `1`, and `9`-bit boundaries).

## Edge Cases & Error Semantics

- Recursive nesting depth exceeding `32` or aggregate nested leaf bytes exceeding `max_bytes` fails with `PostgresError::Data` before buffer reallocation.
- Multi-batch flushes over nested columns reset child list/struct builders cleanly while retaining identical `Arc<Schema>`, and `arrow_ipc::writer::StreamWriter` -> `StreamReader` roundtrips preserve exact parent/child validity bitmaps (critical for distinguishing `NULL` composite vs `(NULL, NULL)` composite under Arrow IPC).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` (`nested_*` and `vector_*` tests)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Array `NULL` vs `0`-D empty vs `[NULL]` vs negative lower bounds | Rows: `NULL`, `'{}'::int4[]`, `'{NULL, 7}'::int4[]`, and `[-2:-1][3:4]={{10,20},{30,40}}` | Row 0 parent `is_null`; Row 1 valid with `dimensions=[]`, `lower_bounds=[]`, `values=[]`; Row 2 `values=[null, 7]`; Row 3 `dimensions=[2,2]`, `lower_bounds=[-2,3]`, `values=[10,20,30,40]`. |
| Composite `NULL` vs `(NULL, NULL)`, nested array-of-composites, domain, enum | `NULL::pair_t` vs `ROW(NULL, NULL)::pair_t`, `pair_t[]`, composite with `numeric[]` + `mood` enum + `uuid` domain across 2 flushed batches + `arrow-ipc` roundtrip | IPC roundtrip preserves `is_null(0) == true` vs `is_valid(1) == true` with null children, ordered non-dropped attributes, and identical schema across batches. |
| `vector(3)` fixed vs dynamic `vector` & `halfvec` exact `f16` values | `vector(3)` rows `[1.0, -0.0, 0.5]`, `NULL`, and mismatched `[1.0, 2.0]`; dynamic `halfvec` rows `[65504.0, 0.00006103515625]` and `[1.5, -2.5, 3.25]` | Fixed `[1.0, -0.0, 0.5]` + `NULL` succeed in `FixedSizeList(3)`, length `2` returns `Err(PostgresError::Data)`; `halfvec` produces `List<Float32>` with exact widened `f16` floats and `"pghalfvec"` metadata. |
| `bit` / `varbit` lengths `0`, `1`, `9` & nonzero padding rejection | `(b"", 0)`, `(b"\x80", 1)`, `(b"\xff\x80", 9)`, and invalid `(b"\x81", 1)` | Valid `0`, `1`, and `9`-bit values roundtrip in `Struct(bytes, bit_length)`; nonzero trailing pad bit `(b"\x81", 1)` returns `Err(PostgresError::Data)`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests nested_`
- Final Green + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests nested_
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests vector_
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```
- Planned local commit: `feat(postgres): build typed nested Arrow arrays`

## Acceptance Criteria

- [x] Multidimensional PostgreSQL arrays (including `0`-D empty, `NULL` elements, and negative lower bounds) and ordered catalog composites preserve exact child values and parent/child validity bitmaps across multi-batch flushes and `arrow-ipc` roundtrips (R20).
- [x] `vector` and `halfvec` use `FixedSizeList<Float32>` only when typmod specifies dimension `1..=16_000` (otherwise `List<Float32>`), reject dimension mismatches, and preserve exact `f16`-widened `f32` values and metadata (R20).
- [x] `bit`/`varbit` bitstrings validate byte length and zeroed padding bits, and nested builders enforce depth `<= 32` and aggregate `max_bytes` caps without opaque binary fallbacks (R20).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Exact PgArray, PgComposite, PgVector, halfvec, and PgBitString contracts.
[^pg-vec-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::encode_pgvector_f16` — Existing IEEE 754 binary16 halfvec and pgvector dimension conventions.
