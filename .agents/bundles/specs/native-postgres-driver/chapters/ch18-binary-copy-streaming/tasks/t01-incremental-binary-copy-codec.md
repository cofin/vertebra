---
type: Task
id: "native-postgres-driver:ch18:t01"
chapter_id: ch18-binary-copy-streaming
title: Frame binary COPY rows incrementally with bounded decoder and encoder state machines
description: Implement incremental BinaryCopyDecoder and BinaryCopyEncoder in vertebra-postgres with 16 MiB row/field caps, 64 KiB extension caps, critical flag rejection, and shared binary value encoding.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch17:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch18-binary-copy-streaming/spec.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-db/src/postgres/mod.rs
  - src/py/vertebra/db/adapters/postgres/core.py
  - .agents/skills/runtime-ipc/SKILL.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/copy.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_copy_codec_tests.rs
estimated_loc: 210
plan_revision: 5
plan_commit: 0147d80c
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: 5bfc7feb
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Lossless PostgreSQL value and binary COPY refusal contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL binary COPY header, flags, and incremental framing constraints
  - id: pg-core-copy
    resource: src/rs/vertebra-db/src/postgres/mod.rs::decode_pgcopy_binary_rows
    title: Existing whole-buffer PGCOPY decoder in vertebra-db
tags:
  - postgres
  - rust
  - copy
  - codec
---

# Task t01: Frame Binary COPY Rows Incrementally with Bounded Decoder and Encoder State Machines

## Objective & Scope Boundary

Implement `BinaryCopyDecoder` and `BinaryCopyEncoder` in `src/rs/vertebra-postgres/src/copy.rs` (wired via `pub mod copy;` in `lib.rs`) to frame `PGCOPY\n\xff\r\n\0` binary streams incrementally across arbitrary `CopyData` split points while retaining at most one incomplete row in memory [^pg-contracts] [^pg-protocol-research] [^pg-core-copy]. Live socket `CopyOut` and `CopyFromStream` session streams belong to `ch18:t02` and `ch18:t03`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 18 incremental binary `COPY` codec and failure-mode contracts.
- [contracts.md](../../../contracts.md) — Exact `PgValue` invariants, `TemporalText` (`wire_text`) binary refusal rule, and numeric/array/composite/vector widths [^pg-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — PostgreSQL `COPY` binary header flags, OID bit rejection, and trailer rules [^pg-protocol-research].
- `src/rs/vertebra-db/src/postgres/mod.rs::decode_pgcopy_binary_rows` — Legacy whole-buffer `PGCOPY` framing reference (must not be called on streaming paths) [^pg-core-copy].
- `src/py/vertebra/db/adapters/postgres/core.py` — Canonical `PGCOPY\n\xff\r\n\x00` signature bytes.
- [SKILL.md](../../../../../../skills/runtime-ipc/SKILL.md) — Checked arithmetic and pre-allocation length validation rules.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Export `pub mod copy;` (re-exporting `BinaryCopyDecoder` and `BinaryCopyEncoder` for crate/integration tests).
- `src/rs/vertebra-postgres/src/copy.rs`:
  - Constants:
    - `pub const PGCOPY_SIGNATURE: [u8; 11] = *b"PGCOPY\n\xff\r\n\0";`
    - `pub const DEFAULT_MAX_COPY_ROW_BYTES: usize = 16 * 1024 * 1024;`
    - `pub const DEFAULT_MAX_COPY_FIELD_BYTES: usize = 16 * 1024 * 1024;`
    - `pub const DEFAULT_MAX_COPY_EXTENSION_BYTES: usize = 64 * 1024;`
  - `pub struct BinaryCopyDecoder` driving explicit `Header` -> `Extension` -> `FieldCount` -> `FieldLength` -> `FieldBytes` -> `Done` states, buffering unparsed bytes in `BytesMut` and retaining only the single in-progress `Vec<Option<Bytes>>` row:
    - `pub fn new(expected_columns: usize) -> Self`
    - `pub fn with_limits(expected_columns: usize, max_row_bytes: usize, max_field_bytes: usize, max_ext_bytes: usize) -> Self`
    - `pub fn push(&mut self, chunk: &[u8]) -> Result<(), PostgresError>`
    - `pub fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, PostgresError>`
    - `pub fn finish(self) -> Result<u64, PostgresError>`
  - `pub struct BinaryCopyEncoder`:
    - `pub fn new(expected_columns: usize) -> Result<Self, PostgresError>` (validates `expected_columns <= i16::MAX as usize`)
    - `pub fn write_header(&mut self, out: &mut BytesMut)` (idempotently writes `PGCOPY_SIGNATURE` + `0u32` flags + `0u32` extension length once)
    - `pub fn encode_row_values(&mut self, column_oids: &[u32], row: &[PgValue], ctx: &CodecContext, catalog: &TypeCatalog, out: &mut BytesMut) -> Result<(), PostgresError>` (delegates per-cell binary encoding to shared `codecs::encode_value_with_catalog(value, target_oid, Format::Binary, out, ctx, catalog)`, enforcing exact `int2`/`int4`/`int8`/`text`/`bytea`/`numeric`/`array`/`composite`/`vector`/`bit` widths without `DataRow` framing)
    - `pub fn encode_raw_row(&mut self, fields: &[Option<&[u8]>], out: &mut BytesMut) -> Result<(), PostgresError>`
    - `pub fn finish_trailer(&mut self, out: &mut BytesMut) -> Result<(), PostgresError>` (appends exactly one `-1i16` `0xFFFF` trailer and marks encoder finished)

## Edge Cases & Error Semantics

- Critical flags in bits `16..=31` (`flags & 0xFFFF_0000 != 0`, including OID flag bit `16` `0x0001_0000`) fail immediately with `PostgresError::Data` before extension allocation; reserved low bits `0..=15` (`flags & 0x0000_FFFF`) are ignored per PostgreSQL binary `COPY` specification.
- Header extension length `> max_ext_bytes` (`64 KiB` default), field byte length `> max_field_bytes` (`16 MiB` default), or cumulative row payload `> max_row_bytes` (`16 MiB` default) fails via checked arithmetic before `reserve` or `Vec` capacity growth.
- Negative field count other than `-1i16` (`-2i16`, `i16::MIN`) or non-trailer field count `!= expected_columns` fails with `PostgresError::Data`.
- Negative field length other than `-1i32` (`-2i32`, `i32::MIN`) fails with `PostgresError::Data`; `-1i32` decodes to `None` (`SQL NULL`) while `0i32` decodes to `Some(Bytes::new())` (empty bytes/string).
- `finish()` fails if the `19`-byte header + extension was incomplete, if a partial row is buffered, if the `-1i16` trailer was not consumed (`Done` not reached), or if any trailing byte follows the `-1i16` trailer.
- `BinaryCopyEncoder::encode_row_values` rejects unresolved `PgValue::TemporalText` (`special="wire_text"`) with `PostgresError::Data` before mutating the output frame buffer.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_copy_codec_tests.rs`
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Zero rows & zero columns | `PGCOPY\n\xff\r\n\0` + `flags=0` + `ext_len=0` + `trailer=-1i16`, plus `0`-column `2`-row stream (`0i16`, `0i16`, `-1i16`) | `0` rows (`finish() == Ok(0)`) for empty stream; `2` empty `vec![]` rows (`finish() == Ok(2)`) for `0`-column rows. |
| NULL vs empty bytes & exact scalar/container widths | Encode 1 row with `(None, Some(b""), -7i16, 42i32, -9_000_000_000i64, Decimal("1.2300"), PgArray([1, None], dims=[2], lower=[0]))`, feed every 2-chunk split index and 1-byte-at-a-time into `BinaryCopyDecoder` | Decoded field bytes have exact widths (`None`, `Some(b"")`, `2`B `0xFFF9`, `4`B `0x0000002A`, `8`B `i64`, `12`B binary `numeric`, `28`B binary `int4[]`); `finish() == Ok(1)` at every split boundary. |
| Reserved low flags vs critical/OID flags | `flags = 0x0000_0005` vs `flags = 0x0001_0000` (OID bit 16) and `flags = 0x8000_0000` (critical bit 31) | `0x0000_0005` succeeds; `0x0001_0000` and `0x8000_0000` return `Err(PostgresError::Data(_))` on header parse. |
| Extension & row/field cap enforcement | `ext_len = 65_537` (`> 64 KiB`), field `len = 16 * 1024 * 1024 + 1`, and two `9 MiB` fields in one row (`> 16 MiB` row cap) | All three fail before buffer allocation with `Err(PostgresError::Data(_))`. |
| Mismatched arity, truncated trailer, & trailing bytes | `expected_columns = 3` receiving `2i16` row; stream truncated mid-field or before `0xFFFF`; stream with `0x00` appended after `0xFFFF` | Mismatched arity and trailing byte fail during `push`/`next_row`; truncated streams fail `finish()`. |
| Unresolved `TemporalText` refusal | `BinaryCopyEncoder::encode_row_values` given `PgValue::TemporalText { wire_text: "2024-11-03 01:30:00 CST", ... }` | Returns `Err(PostgresError::Data(_))` before writing row bytes. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_codec_tests`
- Final Green + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_codec_tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```
- Planned local commit: `feat(postgres): frame incremental binary COPY`

## Acceptance Criteria

- [x] `BinaryCopyDecoder` decodes binary `PGCOPY` frames incrementally across arbitrary split and 1-byte boundaries while retaining at most one incomplete row (R19).
- [x] Critical header flags (`16..=31`, including OID bit `16`), oversize extensions (`> 64 KiB`), oversize fields/rows (`> 16 MiB`), mismatched column counts, truncated trailers, and post-trailer bytes fail closed before allocation (R19).
- [x] `BinaryCopyEncoder` writes `PGCOPY` headers, exact `Format::Binary` PostgreSQL send payloads, and one `-1i16` trailer without `DataRow` framing or whole-buffer CPU helper delegation (R19).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Lossless PostgreSQL value and binary COPY refusal contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL binary COPY header, flags, and incremental framing constraints.
[^pg-core-copy]: `src/rs/vertebra-db/src/postgres/mod.rs::decode_pgcopy_binary_rows` — Existing whole-buffer PGCOPY decoder in `vertebra-db`.
