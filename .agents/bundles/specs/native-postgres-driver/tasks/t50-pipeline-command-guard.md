---
type: Task
id: native-postgres-driver:t50
title: Native pipeline SQL-head command guard
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t49
files:
- src/rs/vertebra-postgres/src/pipeline_guard.rs
- src/rs/vertebra-postgres/src/errors.rs
- src/rs/vertebra-postgres/src/lib.rs
tests:
- src/rs/vertebra-postgres/src/pipeline_guard.rs
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-06T06:30:00Z'
requirements:
- R17
- R13
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Native pipeline SQL-head command guard

## Objective

Reject transaction-control aliases and COPY at the native pipeline boundary before any operation in the owned batch can be emitted. The guard is independent of the later Python binding compiler.

## Context

Read `../spec.md`, `../contracts.md`, t49 facade/ownership dependencies and local Rust/PyO3 review guidance. Ownership is exactly the listed files, with semantic unit tests colocated in the private production lexer module. No new public Python API, generic SQL compiler or exception class is introduced. One invocation makes one signed local commit in the complete-driver PR. Estimate 280 handwritten changed lines including tests and canonical diagnostic mapping; keep this focused guard below approximately 300 lines. Preserve other work. Task state remains pending, revision 1 and plan_commit:null.

## Steps

1. Write RED semantic table tests in pipeline_guard.rs::tests: every case-insensitive unquoted head BEGIN/COMMIT/END/ROLLBACK/ABORT/SAVEPOINT/RELEASE, START TRANSACTION, PREPARE TRANSACTION, SET TRANSACTION and SET SESSION CHARACTERISTICS is rejected, including COMMIT/ROLLBACK PREPARED and AND CHAIN forms. Cover nested block comments, line comments, whitespace and any number of leading empty semicolons/comments. COPY is rejected as unsupported. Pair these with allowed ordinary PREPARE name AS SELECT, PREPARE "TRANSACTION" AS SELECT, SELECT 'COMMIT', quoted/dollar-quoted marker text, WITH/DO/CALL bodies containing those words, and exact identifier-boundary/non-ASCII cases. Concrete rejected samples include `;/*outer/*nested*/x*/;--line\nEnD WORK`, `;;PREPARE/*x*/TRANSACTION 'gid'` and `;SET/**/SESSION CHARACTERISTICS AS TRANSACTION READ ONLY`. The head guard may allow `SELECT 1; COMMIT`; the extended server Parse still rejects that multiple-nonempty-statement input rather than executing it. Tests assert guard meaning/classification, not that accepted heads guarantee valid SQL, and never inspect implementation text/private layout.

2. Add private `mod pipeline_guard` and pub(crate) validate_pipeline_command(sql:&str)->Result<(),PostgresError>. Implement a bounded linear SQL-head scanner over UTF8 bytes: skip ASCII whitespace, -- line comments ending at CR/LF/EOF and /* nested */ block comments; only while seeking the first token also skip empty semicolons. Compare complete unquoted ASCII identifiers case-insensitively. Identifier continuation includes ASCII alnum/underscore/$ and non-ASCII bytes so COMMIT_suffix/COMMITé is not the COMMIT keyword. Quoted/string/dollar markers are opaque non-keywords; never scan inside them for control text. No regex, SQL splitting, token replacement or full statement/body parser is used. Unterminated/otherwise malformed SQL may remain a canonical server Parse error; scanner termination remains bounded and cannot hide an executable control head.

3. Reject first-token BEGIN/COMMIT/END/ROLLBACK/ABORT/SAVEPOINT/RELEASE; reject START+TRANSACTION and PREPARE+TRANSACTION; reject SET+TRANSACTION, SET+(LOCAL|SESSION)+TRANSACTION and SET+SESSION+CHARACTERISTICS. Comments may separate these head keywords but semicolons after the first token are not silently joined into a new command. Ordinary SQL PREPARE of an unquoted non-TRANSACTION name or a quoted name is allowed. COPY maps to existing PostgresError::UnsupportedFeature/NotSupportedError; transaction-control heads map to existing Interface/InterfaceError. No synthetic SQLSTATE is assigned. Server extended Parse remains authoritative for multiple nonempty statements; do not try to split or execute them separately.

4. Add pub(crate) validate_pipeline_commands<'a>(sqls:impl IntoIterator<Item=&'a str>)->Result<(),PostgresError>, independent of the future PipelineOperation type. Validate all statements in order before returning success, and wrap the first failure in PostgresError::PipelineValidation{source:Box<PostgresError>,failing_index:usize}. Update existing exhaustive category/SQLSTATE/Display matches and postgres_error_to_py in the same commit: unwrap source to the exact canonical class, attach immutable outcomes=() plus failing_index, and keep Display bounded without logging SQL/parameters. There is no Python PostgresPipelineError. Colocated tests check first failing index and error category across an earlier valid INSERT followed by a rejected command; actual zero-wire emission is t51.

5. Expose the private helper only within the crate, and leave normal single-query/transaction APIs unchanged. t51 invokes the whole-list guard before any batch frame/flush; t53 persists native scope failure before propagation; t54 preserves Python failure even when caught. PREPARE normal SQL remains allowed, COPY remains unsupported in extended batching, and the guard never depends on a future driver/compiler. Commit fix(postgres): reject pipeline transaction control before emission.

## Verification

Capture initial RED for the missing/incorrect guard, then GREEN on the semantic module tests and canonical error conversion compilation:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib pipeline_guard::tests
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
git diff --check
```

The focused test listing must contain named semantic cases (transaction aliases, leading empty/comment handling, allowed marker text, complete keyword boundaries and first batch failure), not cfg-empty or zero-test success. t51 proves no operation frames/flush; t53/t54 prove owned/saved-scope rollback and durable no-DML behavior. No runtime validation has run during planning; record fresh commands/assertions when implemented.

## Acceptance Criteria

- [ ] R17/R13: every listed transaction-control alias is blocked through the native SQL-head guard, including leading empty semicolons and nested/line comments.
- [ ] Ordinary PREPARE name AS SELECT and quoted/string/body marker text is not mistaken for top-level transaction control; server multiple-statement errors remain authoritative.
- [ ] COPY yields canonical NotSupportedError; transaction control yields InterfaceError with no fake SQLSTATE, correct failing_index and immutable outcomes=() for ALL-unsubmitted operations.
- [ ] The helper is private, independent of later binding compilers, and executable in one focused approximately 280-line invocation with colocated semantic tests.
- [ ] Fresh RED/GREEN, lint and clean diff evidence is linked in one signed local commit; t51/t53/t54 complete wire/ownership/rollback acceptance.

## Notes & Discoveries

None.
