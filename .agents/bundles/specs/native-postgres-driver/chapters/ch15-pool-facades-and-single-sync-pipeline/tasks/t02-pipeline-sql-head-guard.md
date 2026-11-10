---
type: Task
id: "native-postgres-driver:ch15:t02"
chapter_id: ch15-pool-facades-and-single-sync-pipeline
title: "Implement native pipeline SQL-head transaction and COPY command guard"
description: "Build a bounded linear UTF-8 SQL-head scanner in vertebra-postgres that rejects transaction-control aliases and COPY before any pipeline batch operation is emitted."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch15:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch15-pool-facades-and-single-sync-pipeline/spec.md
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-py/src/errors.rs
files:
  - src/rs/vertebra-postgres/src/pipeline_guard.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/src/pipeline_guard.rs
estimated_loc: 225
plan_revision: 5
plan_commit: 73c8a6ff
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
commit: 7b67a4ed
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen native pipeline SQL-head scanner rules, InterfaceError/NotSupportedError mapping, and outcomes=() contract
  - id: db-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical InterfaceError and NotSupportedError exception classes in vertebra._vertebra
tags:
  - postgres
  - pipeline
  - lexer
  - validation
  - rust
---

# Task t02: Implement Native Pipeline SQL-Head Transaction and `COPY` Command Guard

## Objective & Scope Boundary

Implement a crate-private, bounded linear UTF-8 SQL-head scanner (`pipeline_guard.rs`) and `PostgresError::PipelineValidation` diagnostic wrapper that reject all transaction-control aliases and `COPY` statements at the native pipeline boundary before any operation in the batch can be emitted [^pg-contracts] [^db-errors-rs]. Wire-level pipeline batch encoding (`ch15:t03`) and Python pipeline contexts (`ch16:t01`–`t02`) consume this helper without introducing any new Python exception class or regex/SQL-splitting parser.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 15 SQL-head guard rules and error attachment contracts.
- [contracts.md](../../../contracts.md) — Exact forbidden transaction aliases, allowed `PREPARE name AS` / quoted / literal cases, `outcomes = ()` + `failing_index` invariants, and server `Parse` authority for multi-statement strings [^pg-contracts].
- `src/rs/vertebra-postgres/src/errors.rs` — `PostgresError` variants, exhaustive category/SQLSTATE/`Display` matches, and `postgres_error_to_py`.
- `src/rs/vertebra-postgres/src/lib.rs` — Private module registration (`mod pipeline_guard;`).
- `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Shared `InterfaceError` and `NotSupportedError` classes [^db-errors-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/pipeline_guard.rs` (private `mod pipeline_guard;` in `lib.rs`):
  - `pub(crate) fn validate_pipeline_command(sql: &str) -> Result<(), PostgresError>`:
    - Linear single-pass scanner over UTF-8 bytes (`O(len)` time, `O(1)` space, zero regex, zero statement splitting).
    - Leading trivia skip (ONLY before the first non-semicolon token): skip ASCII whitespace, empty `;` semicolons, `--` line comments through `\r` / `\n` / EOF, and nested `/* ... */` block comments.
    - Between multi-keyword head tokens (after the first token has been read): skip ASCII whitespace, `--` line comments, and nested `/* ... */` block comments, and NEVER skip `;` semicolons so a semicolon ends the head rather than joining a second statement.
    - Identifier boundary rule: an unquoted ASCII keyword matches case-insensitively ONLY when not followed by an identifier continuation byte (`b'a'..=b'z'`, `b'A'..=b'Z'`, `b'0'..=b'9'`, `b'_'`, `b'$'`, or any non-ASCII byte `>= 0x80`), so `COMMIT_suffix` and `COMMITé` are ordinary identifiers rather than `COMMIT`. Double-quoted identifiers (`"..."`), single-quoted strings (`'...'`), `E'...'` strings, and dollar-quoted strings (`$tag$...$tag$`) are opaque non-keywords.
    - Rejected heads:
      - Single-keyword transaction control -> `PostgresError::Interface(...)` (`sqlstate = None`): `BEGIN`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, `SAVEPOINT`, `RELEASE` (covering `COMMIT PREPARED`, `ROLLBACK PREPARED`, `COMMIT AND CHAIN`, `END WORK`, `SAVEPOINT s`, `RELEASE SAVEPOINT s`).
      - Multi-keyword transaction control -> `PostgresError::Interface(...)` (`sqlstate = None`): `START TRANSACTION`, `PREPARE TRANSACTION`, `SET TRANSACTION`, `SET LOCAL TRANSACTION`, `SET SESSION TRANSACTION`, `SET SESSION CHARACTERISTICS`.
      - Bulk stream -> `PostgresError::UnsupportedFeature(...)` (`sqlstate = None`): `COPY`.
    - Allowed heads: `PREPARE plan_name AS SELECT ...`, `PREPARE "TRANSACTION" AS SELECT ...`, `SELECT 'COMMIT'`, `WITH ...`, `DO $$ BEGIN ... END $$`, `CALL ...`, and `SELECT 1; COMMIT` (left to server extended `Parse`, which rejects multiple non-empty statements with `42601`).
  - `pub(crate) fn validate_pipeline_commands<'a>(sqls: impl IntoIterator<Item = &'a str>) -> Result<(), PostgresError>`:
    - Validates every SQL string in index order and wraps the first failure in `PostgresError::PipelineValidation { source: Box<PostgresError>, failing_index: usize }`.
- `src/rs/vertebra-postgres/src/errors.rs`:
  - Add `PipelineValidation { source: Box<PostgresError>, failing_index: usize }` to `PostgresError`, delegating category/SQLSTATE matches to `source` (`sqlstate() == None`), keeping `Display` bounded without logging raw SQL or parameters, and updating `postgres_error_to_py` to instantiate the exact canonical `InterfaceError` or `NotSupportedError` with `.outcomes = PyTuple::empty(py)` (`()`) and `.failing_index = failing_index`.

## Edge Cases & Error Semantics

- **Unterminated comments or malformed SQL**: Scanner terminates cleanly at EOF without panicking or infinite loops; malformed non-control SQL passes through to become a canonical server `Parse` error.
- **No synthetic SQLSTATE or custom exception class**: Guard rejections carry `sqlstate = None`, `outcomes = ()` (denoting an entirely unsubmitted batch), and integer `failing_index` on `vertebra._vertebra.InterfaceError` / `NotSupportedError` [^pg-contracts].

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/src/pipeline_guard.rs` (`#[cfg(test)] mod tests`) testing semantic classification, index reporting, and Python exception attribute projection (`outcomes == ()`, `failing_index`, `sqlstate is None`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Unquoted transaction aliases & nested comments | `";/*outer/*nested*/x*/;--line\nEnD WORK"`, `"BEGIN ISOLATION LEVEL SERIALIZABLE"`, `"COMMIT AND CHAIN"`, `"ROLLBACK PREPARED 'gx'"`, `"ABORT"`, `"SAVEPOINT s1"`, `"RELEASE s1"` | `Err(PostgresError::Interface)` (`InterfaceError`, `sqlstate is None`). |
| Multi-keyword transaction heads | `";;PREPARE/*x*/TRANSACTION 'gid'"`, `"START\n--c\nTRANSACTION"`, `";SET/**/SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"`, `"SET LOCAL TRANSACTION READ ONLY"` | `Err(PostgresError::Interface)` (`InterfaceError`, `sqlstate is None`). |
| `COPY` head rejection | `"; /*bulk*/ COPY items FROM STDIN BINARY"` | `Err(PostgresError::UnsupportedFeature)` (`NotSupportedError`, `sqlstate is None`). |
| Allowed `PREPARE`, quotes, bodies, & identifier prefixes | `"PREPARE my_stmt AS SELECT 1"`, `"PREPARE \"TRANSACTION\" AS SELECT 1"`, `"SELECT 'COMMIT', $$ROLLBACK$$"`, `"DO $$ BEGIN PERFORM 1; END $$"`, `"SELECT COMMIT_suffix, COMMITé FROM t"`, `"SELECT 1; COMMIT"` | `Ok(())` from `validate_pipeline_command` (multi-statement `"SELECT 1; COMMIT"` remains for server extended `Parse` rejection). |
| Whole-batch index & Python attrs | `validate_pipeline_commands(["INSERT INTO t VALUES ($1)", ";;/*x*/COMMIT", "COPY t TO STDOUT"])` mapped via `postgres_error_to_py` | Raises `vertebra._vertebra.InterfaceError` with `exc.failing_index == 1`, `exc.outcomes == ()`, and `exc.sqlstate is None`. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib pipeline_guard::tests
  ```
- Final Green, test-listing check, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib pipeline_guard::tests -- --list
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib pipeline_guard::tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [x] Every transaction-control alias (`BEGIN`/`COMMIT`/`END`/`ROLLBACK`/`ABORT`/`SAVEPOINT`/`RELEASE`, `START`/`PREPARE TRANSACTION`, `SET [LOCAL|SESSION] TRANSACTION`, `SET SESSION CHARACTERISTICS`) is rejected across leading `;`, whitespace, `--`, and nested `/* ... */` comments (R13, R17).
- [x] `PREPARE name AS ...`, quoted `"TRANSACTION"`, string/dollar-quoted markers, `DO`/`WITH`/`CALL` bodies, and identifier-continuation prefixes (`COMMIT_suffix`, `COMMITé`) pass without false positives (R17).
- [x] `COPY` maps to `NotSupportedError` and transaction control maps to `InterfaceError` with `sqlstate = None`, `outcomes = ()`, and exact `failing_index` on the first failing batch item (R13, R17).
- [x] Named semantic unit tests in `pipeline_guard::tests`, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen SQL-head pipeline scanner specification and diagnostic attribute invariants.
[^db-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical `InterfaceError` and `NotSupportedError` classes.
