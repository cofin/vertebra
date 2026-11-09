---
type: Task
id: t03
flow_id: native-mssql-driver
title: "Implement MSSQL codecs, VTBARROW ShmCapsule export, BulkLoad load_from_arrow, and BrokerEngineBackend registration"
description: "Implement rich MSSQL column/parameter codecs (numeric/decimal/money, datetime/datetime2/datetimeoffset/date/time, uniqueidentifier, PLP varchar(max)/nvarchar(max)/varbinary(max), xml, json), VTBARROW ShmCapsule (select_to_arrow, stream_arrow_batches), BulkLoadRequest load_from_arrow, and BrokerEngineBackend."
status: stable
state: completed
verification_strategy: tdd
depends_on:
  - t02
plan_revision: 1
state_revision: 2
commit_sha: null
claimed_at: "2026-11-09T15:10:00Z"
completed_at: "2026-11-09T15:15:00Z"
created_at: "2026-11-09T03:20:00Z"
updated_at: "2026-11-09T15:15:00Z"
reads:
  - src/rs/vertebra-db/src/arrow_shm.rs
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-mssql/src/client.rs
  - src/rs/vertebra-mssql/src/params.rs
targets:
  - src/rs/vertebra-mssql/src/codecs.rs
  - src/rs/vertebra-mssql/src/arrow_export.rs
  - src/rs/vertebra-mssql/src/bulk_load.rs
  - src/rs/vertebra-mssql/src/broker.rs
  - src/rs/vertebra-mssql/src/client.rs
  - src/rs/vertebra-mssql/src/lib.rs
  - src/rs/vertebra-mssql/tests/tds_wire_loopback.rs
  - src/py/vertebra/_mssql.pyi
sources:
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: VTBARROW ShmCapsule and Arrow IPC decoder/encoder in vertebra-db
  - id: broker-rs
    resource: src/rs/vertebra-core/src/broker.rs
    title: BrokerEngineBackend trait for SHM DB broker registration
---

# Task t03: Implement MSSQL codecs, `VTBARROW` `ShmCapsule` export, `BulkLoad` `load_from_arrow`, and `BrokerEngineBackend` registration

<!-- truth: start -->
- **Rich TDS Column & Parameter Codecs**: `vertebra-mssql` decodes and encodes `numeric`/`decimal`/`money`/`smallmoney` (`Decimal`), `date`/`time`/`datetime`/`smalldatetime`/`datetime2`/`datetimeoffset` (`datetime.date`, `datetime.time`, `datetime.datetime` with `datetime.timezone`), `uniqueidentifier` (`uuid.UUID`), `(n)varchar(max)` and `varbinary(max)` PLP chunks, `xml`, and `json`.
- **Zero-Copy `VTBARROW` `ShmCapsule`**: `select_to_arrow` and `stream_arrow_batches` materialize TDS token streams into Arrow `RecordBatch`es and seal them into `VTBARROW` `ShmCapsule`s via `vertebra_db::arrow_shm::write_record_batches_to_shm` [^arrow-shm].
- **`BulkLoadRequest` & `BrokerEngineBackend`**: `load_from_arrow(table, source, *, columns=None)` streams Arrow `RecordBatch` rows through TDS `BulkLoad` (`0x07` `BulkLoadRequest`) and `MssqlBrokerBackend` implements `vertebra_core::broker::BrokerEngineBackend` [^broker-rs].
<!-- truth: end -->

## Context

Completes the data-plane capabilities of `vertebra-mssql`: rich Python/Arrow type fidelity across all SQL Server data types, zero-copy `VTBARROW` `ShmCapsule` Arrow export, high-throughput `BulkLoadRequest` Arrow ingestion, and `BrokerEngineBackend` integration.

## Public Seam Contract

- `SyncMSSQLClient` and `AsyncMSSQLClient`:
  - `select_to_arrow(sql, params=None, *, batch_size=65536) -> CapsuleType` (`VTBARROW` `ShmCapsule`)
  - `stream_arrow_batches(sql, params=None, *, batch_size=65536) -> list[CapsuleType]`
  - `load_from_arrow(table, source, *, columns=None) -> int`
- `MssqlBrokerBackend` implementing `vertebra_core::broker::BrokerEngineBackend` (`dialect() -> "mssql"`).

## Implementation Blueprint

1. Implement `src/rs/vertebra-mssql/src/codecs.rs` for bidirectional conversion between Python objects (`bool`, `int`, `float`, `Decimal`, `str`, `bytes`, `UUID`, `date`, `time`, `datetime` naive/aware) and `tiberius::ColumnData<'static>`.
2. Implement `src/rs/vertebra-mssql/src/arrow_export.rs` converting TDS `Column` and `Row` streams into Arrow `Schema` and `RecordBatch`es (`Int64`, `Float64`, `Boolean`, `Decimal128`, `Date32`, `Time64Microsecond`, `TimestampMicrosecond`, `Utf8`, `Binary`) and sealing into `VTBARROW` `ShmCapsule` via `write_record_batches_to_shm` [^arrow-shm].
3. Implement `src/rs/vertebra-mssql/src/bulk_load.rs` decoding `VTBARROW` `ShmCapsule` or Arrow IPC `bytes` via `decode_arrow_ipc_bytes` and streaming `TokenRow`s into `client.bulk_insert(table)`.
4. Implement `src/rs/vertebra-mssql/src/broker.rs` (`MssqlBrokerBackend` implementing `BrokerEngineBackend`) [^broker-rs].

## Acceptance Criteria

- [x] `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mssql --locked` passes including codec round-trip, `VTBARROW` `ShmCapsule`, `BulkLoad`, and `MssqlBrokerBackend` tests.
- [x] `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mssql --all-targets --locked -- -D warnings` passes.
- [x] `mypy.stubtest vertebra._mssql` passes.

[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs` — VTBARROW ShmCapsule and Arrow IPC decoder/encoder in vertebra-db.
[^broker-rs]: `src/rs/vertebra-core/src/broker.rs` — BrokerEngineBackend trait for SHM DB broker registration.
