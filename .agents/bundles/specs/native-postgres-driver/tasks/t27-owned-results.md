---
type: Task
id: "native-postgres-driver:t27"
title: "Owned query results and PostgreSQL value records"
description: "Owned query results and PostgreSQL value records for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t26"
files:
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/py/vertebra/db/adapters/postgres/types.py
  - src/rs/vertebra-postgres/tests/result_contract.rs
  - src/py/tests/unit/db/test_postgres_types.py
tests:
  - src/rs/vertebra-postgres/tests/result_contract.rs
  - src/py/tests/unit/db/test_postgres_types.py
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
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T04:15:00Z"
commit: null
tags:
  - postgres
  - driver
  - rust
---

# Owned query results and PostgreSQL value records

## Objective

Define the single owned result/value contract used by query, COPY, Arrow, pipeline and Python conversion.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED public-constructor tests for owned results surviving source-buffer destruction, repeated/empty column names, ordered duplicate columns, NULL versus empty bytes, and all three transaction states. Python tests construct immutable lossless wrappers and reject inconsistent dimensions/field counts without native I/O. Add exact PgBitString byte-length/padding/empty cases and PgVector kind/dimension/nonfinite cases, using the complete fields/defaults/invariants in contracts.md.

2. Add public result.rs::{Format,TransactionStatus,Column,QueryResult,PgValue,PgNumeric}. Column carries name:String,type_oid:u32,format:Format,type_modifier:i32,table_oid:u32,attribute_number:i16. QueryResult carries ordered columns, owned rows, affected_rows:u64, command_tag:String and transaction_status. Keep row-width validation centralized. Numeric retains sign, arbitrary coefficient digits, scale and NaN/positive/negative infinity; never narrow through f64 or fixed-precision Decimal.

3. Define PgValue variants for null, scalar integer widths/floats, UTF8 text/bytes/UUID, numeric, PG-epoch temporal raw integers, lossless interval, validated JSON bytes, dimensioned arrays, typed ordered composites, vector/halfvec and packed bit values. Text fallback retains column OID through Column rather than pretending arbitrary binary is UTF8. Add a TemporalText variant carrying temporal kind, exact wire_text and captured DateStyle/TimeZone for t59 unresolved display output. No PyObject or borrowed message body enters the rlib model.

4. Create frozen gc=False msgspec records in adapter-owned types.py: PostgresColumn; PostgresQueryResult; PgInterval(months,days,microseconds); PgArray(values,dimensions,lower_bounds,element_oid); PgComposite(type_oid,field_names,field_oids,values); PgTemporal(kind,value,special,wire_text=None,date_style=None,time_zone=None); PgJson(value); PgBitString(data:bytes,bit_length:int); PgVector(values:tuple[float,...],kind:Literal["vector","halfvec"]); PgParameter(value,type_oid). Define the exact recursive PostgresValue and PostgresJsonValue aliases in the complete value contract; PgParameter is an outer marker rather than a nested PostgresValue. Decode vector/halfvec to PgVector and SQL bit/varbit to PgBitString, never ambiguous lists, bytes or unspecified views. PgBitString uses exact MSB-first bytes with zero unused low padding and accepts empty(b"",0); PgVector validates kind, dimensions1..16000 and finite numeric values. PgTemporal stores exact PG epoch units and special tag for infinities/out-of-range/24:00; fields and units are fixed in the full spec public value contract. None with PgParameter supplies a typed SQL NULL; PgJson(None) intentionally binds JSON null. PgTemporal wire_text fallback carries exact server text plus captured DateStyle/TimeZone and is filled in t59; it never pretends to be a raw epoch integer.

5. Add adapter-private types.py::_project_public_row: it maps only top-level PgJson(None) to None for established high-level driver/pipeline projections and leaves every other value unchanged. Native query/query_multi records retain the rich JSON-null marker. Export Rust models from lib.rs with explicit names. Keep Python engine types in the adapter package; do not eagerly import the optional wheel or add a generic database engine switch. Commit feat(postgres): define owned query results and lossless values.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test result_contract
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_types.py -q
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R10, R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Owned Rust/Python model fields and exact units agree with the full spec public value contract.
- [ ] No column order, numeric scale, interval month, array bound or composite OID information is lost.
- [ ] Wrapper imports work without vertebra._postgres installed.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
