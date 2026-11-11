---
type: Task
id: "native-postgres-driver:ch22:t02"
chapter_id: ch22-copyboth-and-pgoutput-decoding
title: Decode pgoutput relation metadata and lossless tuples
description: Implement incremental PgOutputDecoder with bounded RelationCache, Type invalidation, shared lossless tuple decoding, and pure-Python PgUnchangedToast and replication record types.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch22:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/types.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/py/vertebra/db/adapters/postgres/types.py
files:
  - src/rs/vertebra-postgres/src/replication/pgoutput.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/py/vertebra/db/adapters/postgres/types.py
tests:
  - src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs
estimated_loc: 240
plan_revision: 6
plan_commit: 8e9b324e
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-11T01:52:41Z"
last_verified_commit: 64288c63
verification_evidence: "cargo test -p vertebra-postgres --test postgres_pgoutput_tests --test postgres_replication_wire_tests (7 + 9 passed), cargo clippy -p vertebra-postgres --all-targets -- -D warnings, ruff check, mypy, pytest test_postgres_types.py (26 passed), and scan_public_docstrings exited 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-11T01:53:00Z"
commit: 64288c63
generated:
  by: flow-revise
  at: "2026-11-10T14:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 8e9b324e
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Exact PostgreSQL value records, replication identity, and TOAST rules
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: pgoutput v1 Relation, Type, and four-state TupleData message layouts
  - id: cdc-domain
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: Adapter-owned CDC value fidelity and unchanged TOAST separation
tags:
  - postgres
  - replication
  - pgoutput
  - codecs
---

# Task t02: Decode `pgoutput` Relation Metadata and Lossless Tuples

## Objective & Scope Boundary

Implement `PgOutputDecoder` in `src/rs/vertebra-postgres/src/replication/pgoutput.rs` to incrementally parse `pgoutput` v1 `Relation` (`'R'`), `Type` (`'Y'`), and `TupleData` payloads across arbitrary chunk splits, maintain an atomic `4096`-entry `RelationCache`, preload publication custom types on the ordinary metadata connection before `START_REPLICATION`, and define the pure-Python `PgUnchangedToast`, `PostgresReplicationIdentity`, and `PostgresReplicationRecord` structs in `src/py/vertebra/db/adapters/postgres/types.py` [^npd-contracts] [^npd-proto-research] [^cdc-domain]. Full `Begin`/`Commit`/DML transaction ordering belongs to `ch22:t03`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Frozen `msgspec.Struct` contracts, `PostgresReplicationIdentity.identity_key`, and lossless value invariants [^npd-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `pgoutput` v1 `'R'`, `'Y'`, and four-state `TupleData` (`'n'`, `'u'`, `'t'`, `'b'`) wire layouts [^npd-proto-research].
- `src/rs/vertebra-postgres/src/replication/mod.rs` — `PostgresReplicationConnection` preflight and metadata session hooks from `ch22:t01`.
- `src/rs/vertebra-postgres/src/types.rs` — Session `TypeCatalog` for array, composite, domain, and extension OID discovery.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — Shared PostgreSQL text and binary value decoders (`decode_value_with_catalog`, `PgValue`, `CodecContext`).
- `src/py/vertebra/db/adapters/postgres/types.py` — Pure-Python frozen `msgspec.Struct` value records.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/replication/pgoutput.rs`:
  - `pub const DEFAULT_MAX_RELATIONS: usize = 4096;` and `pub const DEFAULT_MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;`
  - `pub struct PgOutputColumn { pub is_key: bool, pub name: String, pub type_oid: u32, pub type_modifier: i32 }`
  - `pub struct PgOutputRelationSchema { pub relation_oid: u32, pub namespace: String, pub name: String, pub replica_identity: u8, pub columns: Arc<[PgOutputColumn]> }`
  - `pub enum PgOutputTupleValue { Null, UnchangedToast, Value(PgValue) }`
  - `pub struct PgOutputDecoder`:
    - `pub fn new(max_relations: usize, max_record_bytes: usize) -> Self`
    - `pub fn push(&mut self, payload: &[u8]) -> Result<(), crate::errors::PostgresError>` — appends incoming `XLogData` payload bytes into a single bounded `BytesMut` accumulator, failing immediately with resource-limit `OperationalError` if buffered incomplete bytes exceed `max_record_bytes`.
    - `pub fn decode_next_frame(&mut self, catalog: &mut crate::types::TypeCatalog, codec_ctx: &CodecContext) -> Result<Option<PgOutputFrame>, crate::errors::PostgresError>` — parses network-endian fields via checked cursor helpers (`u8`, `u16`, `i32`, `u32`, `i64`, NUL-terminated UTF-8 `cstring`) and decodes cell values through `crate::codecs::decode_value_with_catalog`, returning `Ok(None)` when an incomplete record awaits more bytes while consuming exact prefixes so coalesced messages inside one `XLogData` frame decode sequentially.
    - `pub fn reset_on_disconnect(&mut self)` — clears partial buffered bytes and in-flight state on transport EOF without raising corruption errors so `ch23:t02` can reconnect and replay cleanly.
    - `pub fn finish_clean_eof(&self) -> Result<(), crate::errors::PostgresError>` — returns `ProtocolError` if unread partial record bytes remain when a clean `CopyDone` arrives.
- `src/rs/vertebra-postgres/src/replication/mod.rs`:
  - Preload `(namespace, name)` alongside `TypeInfo` for every custom `vector`, `halfvec`, `enum`, `domain`, `range`, `array`, and `composite` OID referenced by published relations (recursively resolving element, domain-base, range-subtype, and composite-field OIDs via bound `pg_catalog.pg_type` + `pg_catalog.pg_namespace` joins, zero correlated scalar subqueries) into `crate::types::TypeCatalog` plus a `HashMap<u32, (String, String)>` type-identity map on the ordinary preflight connection before `START_REPLICATION` (never issuing SQL on the `CopyBoth` socket); expose replication-only Python conversion mapping `PgOutputTupleValue::UnchangedToast` -> `vertebra.db.adapters.postgres.types.PG_UNCHANGED_TOAST` without adding `UnchangedToast` to SQL `PgValue`.
- `src/py/vertebra/db/adapters/postgres/types.py` (module top-level imports only, strict class member ordering, Google-style docstrings, zero inline `#` comments):
  - `class PgUnchangedToast(msgspec.Struct, frozen=True, gc=False):` (zero fields; singleton `PG_UNCHANGED_TOAST: Final[PgUnchangedToast] = PgUnchangedToast()`, explicitly excluded from the `PostgresValue` bind union).
  - `class PostgresReplicationIdentity(msgspec.Struct, frozen=True, gc=False): system_identifier: str; database: str; slot: str; publication_fingerprint: str` (validates non-empty scalar strings in `__post_init__`; `@property def identity_key(self) -> tuple[str, str, str, str]`).
  - `class PostgresReplicationRecord(msgspec.Struct, frozen=True): kind: Literal["control", "begin", "insert", "update", "delete", "truncate", "message", "commit"]; payload: dict[str, Any]; commit_end_lsn: int | None = None; generation: int = 0` (`gc=False` intentionally omitted because `payload` stores mutable `dict`/`list`/`PgJson` containers that must stay tracked by Python's cyclic garbage collector; validates non-bool `int` bounds `0 <= generation` and `0 <= commit_end_lsn < 2**64` when set).

## Edge Cases & Error Semantics

- `Relation` (`'R'`) update for an existing `relation_oid` replaces the map entry with a fresh `Arc<PgOutputRelationSchema>` so already decoded events keep their captured schema snapshot immutable; inserting a `4097`th distinct relation into a `4096`-cap cache fails explicitly with `OperationalError`.
- `Type` (`'Y'`: `type_oid: u32`, `namespace: cstring`, `name: cstring`) compares incoming `(namespace, name)` against the preloaded `(type_oid, namespace, name)` metadata: when `(namespace, name)` matches, the preloaded `TypeCatalog` entry (`vector`, `halfvec`, `enum`, `domain`, `range`, `array`, or `composite`) stays intact so standard `pgoutput` `'Y'` messages preceding the first custom tuple preserve full rich decoding; `type_oid` is removed from `TypeCatalog` ONLY when `(namespace, name)` mismatches the preloaded identity (for example after DDL rename/drop-recreate), causing subsequent `'t'` cells of that OID to fall back to lossless tagged text (`PgValue` text + column `type_oid`) without failing or issuing mid-stream SQL.
- `TupleData` strictly distinguishes missing/dropped columns (absent from `Relation` column list), `'n'` (`PgOutputTupleValue::Null`), `'u'` (`PgOutputTupleValue::UnchangedToast`), `'t'` (UTF-8 text decoded via shared codec for `type_oid`/`type_modifier`), and `'b'` (binary payload decoded via shared binary codec even though `binary='false'` was requested).
- Unknown `TupleData` column tag, column count mismatch against `PgOutputRelationSchema.columns.len()`, unknown `relation_oid`, invalid UTF-8 in `cstring` / `'t'`, invalid `replica_identity` (`not in b"dfni"`), or truncated trailing bytes on clean `CopyDone` returns `ProtocolError`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Four-state `TupleData` fidelity | Golden `'R'` (`public.accounts`, `replident = b'd'`, cols `id int8 key`, `bio text`, `meta jsonb`, `amount numeric`) + tuple with `'t'("42")`, `'u'` (TOAST), `'n'` (`NULL`), `'b'` (binary numeric `1.2300`) | Decodes `id = 42`, `bio = UnchangedToast`, `meta = Null`, `amount = Decimal("1.2300")` with exact scale preserved; missing dropped attribute omitted. |
| `Type` (`'Y'`) matching preservation vs mismatch invalidation | Preload `(namespace, name)` + `TypeInfo` for `vector`, `enum`, `domain`, `range`, `array`, and `composite` OIDs; feed matching `'Y'` frames before tuples, then feed mismatching `'Y'` (`"public"`, `"renamed_composite"`) + tuple; also verify `gc.is_tracked(PostgresReplicationRecord(...))` | Matching `'Y'` retains preloaded `TypeCatalog` entries and decodes rich values (`PgVector`, composite dict, typed array, enum/range); mismatching `'Y'` invalidates only that `type_oid` to lossless tagged text; `PostgresReplicationRecord` stays GC-tracked while `PgUnchangedToast` and `PostgresReplicationIdentity` use `gc=False`. |
| Arbitrary byte-split & coalesced frames | Concatenate `'Y'` + `'R'` + tuple frame (`N` bytes) and feed `push()` at every `1`-byte split `0..N` as well as in one coalesced slice | Identical decoded frames in both single-slice and `1`-byte incremental feeds; buffer empty afterward (`finish_clean_eof().is_ok()`). |
| Atomic `Relation` schema evolution | Emit `'R'` (`rel_oid = 16384`, 2 cols), capture `Arc<PgOutputRelationSchema>`, then emit updated `'R'` (`rel_oid = 16384`, 3 cols) | Earlier captured `Arc` still has `2` columns; subsequent tuples require `3` columns (`2`-col tuple after update fails `ProtocolError`). |
| `RelationCache` cap & unknown relation | Insert `4096` relations then `4097`th `'R'`; separately feed tuple referencing unregistered `rel_oid = 99999` | `4097`th relation returns resource-limit `OperationalError`; unregistered `rel_oid` returns `ProtocolError`. |
| Clean `CopyDone` truncation vs transport drop | Push partial `'R'` prefix (`5` bytes), call `finish_clean_eof()` vs `reset_on_disconnect()` | `finish_clean_eof()` returns `ProtocolError`; `reset_on_disconnect()` clears partial buffer cleanly so next full `'R'` succeeds. |

## Verification

- Initial Red baseline: `PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_pgoutput_tests`
- Final Green + Clippy + Lint + Mypy gate:
  ```bash
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_pgoutput_tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/types.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/types.py
  ```

## Acceptance Criteria

- [x] `PgOutputDecoder` parses `'R'`, mismatch-aware `'Y'` (retaining matching preloaded `vector`/`halfvec`/`enum`/`domain`/`range`/`array`/`composite` entries), and four-state `TupleData` (`'n'`, `'u'`, `'t'`, `'b'`) incrementally across arbitrary splits and coalesced frames using shared `vertebra-postgres` value codecs (R22).
- [x] `RelationCache` enforces the `4096`-relation cap and immutable `Arc` schema snapshots, while `types.py` defines `PgUnchangedToast` (`PG_UNCHANGED_TOAST`), `PostgresReplicationIdentity`, and GC-tracked `PostgresReplicationRecord` (`frozen=True`, without `gc=False`) without optional native wheel imports (R22).
- [x] Commit message follows `feat(postgres): decode pgoutput relations and tuples` once verified.

## Notes & Discoveries

- `PgUnchangedToast` is intentionally excluded from `PostgresValue` so accidentally passing an unchanged-TOAST marker into a SQL `INSERT`/`UPDATE` parameter list fails preflight type checking immediately [^npd-contracts].
- Recursive `preload_publication_types` resolves published attribute OIDs plus transitive `array`, `domain`, `range`, PG14+ `multirange`, `composite`, and `pgvector` `vector`/`halfvec` metadata on the standalone ordinary connection before closing `meta_conn` and opening `replication=database`, keeping zero SQL on `CopyBoth`.

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Exact PostgreSQL value records, replication identity, and TOAST rules.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — pgoutput v1 Relation, Type, and four-state TupleData message layouts.
[^cdc-domain]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Adapter-owned CDC value fidelity and unchanged TOAST separation.
