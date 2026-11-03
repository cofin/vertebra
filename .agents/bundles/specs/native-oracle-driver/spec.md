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

- Build on Oracle's official pure-Rust thin driver: `oracledb` pinned `=26.0.0-beta.5`, with the `arrow` feature. Details are in the [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).
  - It is maintained by Oracle, licensed UPL-1.0 OR Apache-2.0, and needs no Instant Client.
  - Tested on 19c, 21c, and 26ai.
  - Supports DRCP, statement caching, array DML, JSON/VECTOR/LOBs, and `query_arrow` on arrow 59.
- **Verified gaps** (docs Appendix A):
  - The API is synchronous, with no Tokio dependency.
  - Unsupported: 26ai pipelining, token auth, AQ/TxEventQ, CQN, direct path loads, Application Continuity, TPC, implicit results.
  - v1 runs each broker-owned connection on a dedicated blocking thread (`tokio::task::spawn_blocking`) and bridges Arrow 59 to VTBARROW through the C Data Interface.
  - The pipelining decision is open; see below.
- Wrap it behind the Vertebra `Connection` trait and driver contract instead of writing the protocol from scratch.
- **Open decision: 23ai pipelining and native async.** The crate has neither. Options:
  - (a) Implement TTC pipelining and an async transport in a Vertebra extension of the crate and offer it upstream to `oracle/rust-oracledb`.
  - (b) Ship v1 with sequential pipeline execution and add true pipelining once upstream supports it.
  - Recommended: (a). It is the only way to meet the user requirement that Oracle supports pipelining, and Vertebra's existing `oracle.rs` TTC encoders are the starting point.
- The scope items below that the crate lacks (AQ/TxEventQ, token auth, implicit results) are built on top of it.
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
