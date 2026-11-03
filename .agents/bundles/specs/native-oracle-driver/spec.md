---
type: Spec
flow_id: native-oracle-driver
title: Rust Oracle Thin Client
description: Complete Rust Oracle thin-protocol client (TNS, O5LOGON, TTC) with TLS/wallets, binds/defines, LOBs, OSON/VECTOR, DRCP, 23ai pipelining, and Arrow results, replacing the sqlite3 stand-in in the Python Oracle driver.
state: draft
parent_prd: native-data-platform
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-03T04:05:00Z
tags: [rust, oracle, driver, pipeline]
---

# Flow Charter: Rust Oracle Thin Client

## Approach

- Build on Oracle's official pure-Rust thin driver: the `oracledb` crate from `oracle/rust-oracledb`, a Thin-mode TNS/TTC implementation with no Instant Client. Reported features: pooling, DRCP, statement caching, native network encryption, JSON/VECTOR/LOB types, and optional Arrow.
- Wrap it behind the Vertebra `Connection` trait and driver contract instead of writing the protocol from scratch.
- Before the plan is locked, verify against the crate source:
  - async/Tokio support;
  - 23ai pipelining;
  - Arrow output that can feed VTBARROW zero-copy;
  - license compatibility;
  - release maturity (reported as a 26.0.0 beta).
- Contribute or patch only the gaps.
- Rejected: `oracle` (rust-oracle) on ODPI-C and `sibyl` on OCI both need Oracle Instant Client at runtime. The community `oracle-rs` crate is a fallback if the official crate lacks async.
- Vertebra's existing `oracle.rs`/`tns.rs` codecs (OSON, VECTOR, TTC piggyback, tnsnames/wallet parsing) are kept only where the crate does not already cover them.

## Scope

1. **Connection.**
   - TNS connect/accept/redirect/resend using the existing `tns.rs` descriptors, `parse_tnsnames_ora`, and `resolve_tns_alias`.
   - Network data/marker packets.
   - TLS (TCPS) with wallets (`OracleWallet`, PEM/PKCS#12).
   - O5LOGON auth (AUTH phase one/two, SHA-512 PBKDF2 verifiers).
   - Session attributes through the existing TTC piggyback (`OracleSessionAttrs`).
2. **TTC operations.** Execute with binds and defines, fetch with array size and prefetch, commit/rollback, describe, PL/SQL calls (callproc/callfunc), implicit results, and error decoding with ORA codes and offsets.
3. **Types.**
   - NUMBER (exact Decimal/int/float), VARCHAR2/NVARCHAR2/CHAR, RAW, DATE/TIMESTAMP/TZ/LTZ, INTERVAL, BOOLEAN (23ai).
   - CLOB/BLOB/NCLOB, both inline and through LOB locators.
   - JSON/OSON (existing codec) and VECTOR (existing f32/f64/i8/binary codecs).
4. **Pooling.** Leases through the broker, DRCP (`:pooled`, connection class/purity), and the statement cache.
5. **Pipelining.** `pipeline()` sends multiple TTC operations in one round trip on 23ai+ and executes sequentially on older servers. Sync and async forms.
6. **Bulk and Arrow.** `execute_many` (array DML with batch errors), `select_to_arrow`/`stream_arrow_batches` into VTBARROW, and `load_from_arrow`.
7. **Notifications.** TxEventQ dequeue/enqueue for `NotificationSource`.
8. **Python driver.** `db/adapters/oracle/driver.py` runs on the Rust client, and the `sqlite3.connect(":memory:")` stand-in is removed.

## Public Seams

`SyncOracleDriver`, `AsyncOracleDriver`, `driver.pipeline()`, and the Oracle `NotificationSource`.

## Dependencies

`db-layout-and-contract`.

## Acceptance Boundary

- The conformance suite passes against Oracle 23ai Free and 19c through `pytest-databases`.
- Golden fixtures cover TNS, O5LOGON, and TTC execute/fetch/pipeline.
- `make bench-check` is at or above the python-oracledb Thin baseline.

## Draft Task Outline

- t01 TNS + TLS + wallets.
- t02 O5LOGON + session attributes.
- t03 TTC execute/fetch/describe + errors.
- t04 Type codecs incl. LOBs.
- t05 PL/SQL, implicit results, `execute_many`.
- t06 DRCP + statement cache + broker leases.
- t07 Pipelining.
- t08 Arrow + TxEventQ + Python cutover.

This chapter may split into two flows (protocol core, and types/features) at `/flow:plan` time.
