---
type: Spec
flow_id: native-mysql-mssql-drivers
title: Rust MySQL and MSSQL Clients
description: Complete Rust MySQL (handshake v10, caching_sha2, TLS, binary protocol) and MSSQL (TDS 7.4 prelogin, TLS, login7, RPC, token stream, bulk copy) clients replacing pymysql and the sqlite3 stand-in.
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
tags: [rust, mysql, mssql, driver]
---

# Flow Charter: Rust MySQL and MSSQL Clients

## Scope

1. **MySQL connection.** Handshake v10, `caching_sha2_password` (existing scramble) and `mysql_native_password`, TLS, capability flags, and Cloud SQL connector/IAM through `dialer.rs`.
2. **MySQL protocol.**
   - Text and binary protocols: `COM_QUERY`, `COM_STMT_PREPARE`/`EXECUTE`/`CLOSE` (existing encoders), multi-results, and error/warning decoding.
   - Codecs for DECIMAL, temporal types with fractional seconds, JSON, BLOB/TEXT, BIT, and ENUM/SET.
   - Statement cache through `MysqlPool`.
   - `LOAD DATA LOCAL INFILE` from Arrow.
3. **MSSQL connection.** TDS 7.4 PRELOGIN, TLS inside TDS, LOGIN7 (SQL auth), and ENVCHANGE/routing.
4. **MSSQL protocol.**
   - SQL batch (existing encoder) and RPC `sp_executesql` with typed parameters (existing `rewrite_mssql_placeholders`).
   - Token stream decoding: COLMETADATA, ROW, NBCROW, DONE*, ERROR, INFO, RETURNVALUE.
   - Codecs for numeric/decimal/money, datetime2/datetimeoffset, uniqueidentifier, (n)varchar(max) PLP, varbinary, XML, and JSON.
   - Transactions through TDS transaction descriptors.
   - Bulk copy (BULK LOAD) from Arrow.
   - Attention-based cancellation.
5. **Python drivers.** `db/adapters/mysql/driver.py` runs on Rust, with `pymysql`, `memory://`, and `_in_memory_rows` removed. `db/adapters/mssql/driver.py` runs on Rust, with the `sqlite3` stand-in removed.

## Public Seams

`SyncMySQLDriver`, `AsyncMySQLDriver`, `SyncMSSQLDriver`, and `AsyncMSSQLDriver`.

## Dependencies

`db-layout-and-contract`.

## Acceptance Boundary

- The conformance suite passes for MySQL 8.4/9 and MariaDB, and for SQL Server 2022/2025, through `pytest-databases`.
- Golden fixtures cover the MySQL handshake and binary protocol, and the TDS prelogin/login7/RPC exchanges.

## Draft Task Outline

- t01 MySQL connection/auth/TLS.
- t02 MySQL text and binary protocol + codecs.
- t03 MySQL bulk + Python cutover.
- t04 TDS prelogin/TLS/login7.
- t05 TDS batch/RPC + token stream.
- t06 TDS codecs + transactions + cancel.
- t07 TDS bulk copy + Python cutover.
