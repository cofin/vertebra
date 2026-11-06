---
type: Task
id: native-postgres-driver:t40
title: Cold query parameter discovery and owned result normalization
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t39
files:
- src/rs/vertebra-postgres/src/metadata.rs
- src/rs/vertebra-postgres/src/query.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/src/types.rs
- src/rs/vertebra-postgres/src/errors.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/tests/metadata_normalization.rs
tests:
- src/rs/vertebra-postgres/tests/metadata_normalization.rs
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
- R12
- R13
- R15
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Cold query parameter discovery and owned result normalization

## Objective

Resolve custom parameter metadata before Parse, and normalize fully materialized cold text results only after their Execute and ReadyForQuery complete. The ordinary cold query remains one extended group without an implicit transaction.

## Context

Read `../spec.md`, `../contracts.md`, t35 TypeCatalog and t39 codecs, PostgreSQL protocol flow section 54.2.3, and local data/PyO3 knowledge and skills. This worksheet owns only its listed files and one signed local feature-branch commit in the complete-driver PR. Preserve unrelated edits. Estimated 430 handwritten lines including stateful behavior tests, ambient enrichment isolation and completed-result diagnostics. This cohesive helper exceeds the approximate 400-line heuristic; keep the honest estimate and one invocation/commit. No source implementation is authorized while plan_commit is null. The dependency worksheet and parent checklist define the execution order.

PostgreSQL simple Query destroys the unnamed prepared statement. An idle Sync may permit transaction-pooler backend rotation. Catalog lookup must therefore never be placed between an unnamed Parse and its Bind. This helper adds no alternative query engine, no automatic BEGIN and no replay policy. Existing t28 simple-query and t35 catalog APIs suffice for helper tests; t41 introduces ordinary extended execution.

## Steps

1. Write RED public-helper/stateful-peer tests in tests/metadata_normalization.rs. The peer models unnamed statement existence, clears it on every Query, and rotates its backend identity on every idle ReadyForQuery/Sync but retains it while T. Prove custom parameter lookup finishes before the caller's first Parse, and result lookup occurs after the original Execute/ReadyForQuery with no later Bind referencing that destroyed unnamed statement. Include unknown OID text fallback, malformed metadata, catalog error, session-context changes and one original Execute only on every error path. Cover recoverable catalog denial at I, catalog denial isolated/recovered by enrichment savepoint at T, release/rollback failure and transport failure retaining original completed-result diagnostics. Test ordinary no-metadata preparation requests no BEGIN; t41/t96 prove real VACUUM behavior.

2. Implement production metadata.rs::{ResolvedParameters,OwnedTextResult,OwnedTextField,resolve_parameter_metadata,normalize_materialized_result}. resolve_parameter_metadata(&mut PostgresConnection,parameters,explicit_oids) resolves only metadata actually needed for custom wrapper serialization through the existing TypeCatalog before any Parse for that operation. It returns owned parameter values/OIDs and codec metadata; it does not send Parse/Bind/Execute or open a transaction. Built-ins/inferable parameters need no catalog exchange. If required parameter catalog lookup runs under ambient T, reuse the small metadata-only savepoint isolation policy from step 4 before Parse; a failed lookup cannot poison unrelated caller work. If mandatory encoding metadata remains unavailable, fail before the original query is emitted, without a completed_result diagnostic or invented type. Type-name lookup for PgVector uses extension identity; it does not guess a dynamic OID.

3. OwnedTextResult retains ordered Column metadata, owned NULL/text field bytes, command_tag/affected_rows, final server transaction status and the effective CodecContext snapshot for each result/row generation when received. Capture DateStyle/IntervalStyle/TimeZone before any later catalog exchange can change the current context; share immutable snapshots between rows rather than copy settings per cell. Add a completion gate so normalize_materialized_result(&mut PostgresConnection,OwnedTextResult) refuses an uncompleted/in-flight exchange. It never holds a reference into a backend frame.

4. Only after the original complete ReadyForQuery, collect unresolved result OIDs, perform bounded TypeCatalog discovery, then consume already-owned text fields through the shared scalar/array/composite/vector/temporal dispatcher using each captured context. No original query is re-executed, no Bind follows the catalog Query, and no prior backend/unnamed-statement affinity is assumed. Unknown/catalog-unresolvable types remain exact text with Column OID. For catalog-only enrichment at original I, a recoverable catalog error drained back to I retains exact text/OID fallback. At original T, create an internally named savepoint before catalog work using existing simple-query control; a catalog error rolls back/releases only that enrichment savepoint so original user work and T survive, then retains text/OID fallback. Successful enrichment releases that savepoint; it never commits an ambient transaction or opens BEGIN at I. These small internal helpers do not depend on future t42 TransactionScope. Unknown OIDs are ordinary fallback, not invented exceptions. Unrecoverable transport/protocol/cleanup failure marks the session broken and surfaces its canonical source error, attaching the original completed result diagnostically; never retry the completed original SQL. Consume fields progressively without a second whole-result copy; use existing field, recursion and catalog-work bounds.

5. Extend the existing errors.rs::PostgresError with an AfterCompletion{source:Box<PostgresError>,completed_result:QueryResult} diagnostic wrapper, without a new Python exception class. Update every existing exhaustive error/category/SQLSTATE/Display match in this same commit to delegate to source so the foundation continues to compile; Display must not log completed rows. Rust consumers retain the owned diagnostic now. The t48 query mapper attaches the typed completed_result through the t47 converter when that public query facade exists; no future converter is referenced here. The diagnostic QueryResult preserves the original command/count/I-T-E and owned fallback text plus original column metadata, never catalog query descriptions. Inject metadata failure after a side-effecting Execute and prove the error contains that original outcome without replay. Track invalidation of unnamed state on every simple Query emission in the shared session/query path. Export only the production helper types/functions for t41 and later engine consumers. Pipeline submission never calls this post-result discovery helper and keeps unknown custom OIDs as text within its one-Sync contract. Named prepared preflight, if used by t44, separately pins an explicit transaction/savepoint before Parse/Describe and re-Parses an unnamed statement after catalog Query before final Bind. Commit feat(postgres): normalize cold query metadata at safe boundaries.

## Verification

Capture initial RED from absent helper behavior, then GREEN through the production helper and stateful loopback peer:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test metadata_normalization
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test type_catalog
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
git diff --check
```

The later t96 acceptance owns live transaction-mode PgBouncer with protocol cache disabled, first-use custom result/parameter types, backend rotation and VACUUM/no-implicit-BEGIN. t41's final command runs the production extended engine atop this helper; do not claim stateful fixture success substitutes for live pooler acceptance. No runtime commands have run during planning. Capture actual command/exit/assertion evidence during execution and keep the full driver pending until all acceptance tasks pass.

## Acceptance Criteria

- [ ] R12/R13/R15: required parameter metadata precedes Parse; result normalization follows complete ReadyForQuery and never executes or binds the original query again.
- [ ] Stateful Query-clears-unnamed and idle-backend-rotation tests demonstrate the actual lifecycle hazard and safe ordering.
- [ ] Owned raw text and captured codec settings survive later discovery/context changes; custom values become typed when resolved, otherwise exact text/OID remains.
- [ ] No BEGIN or pipeline pre/post metadata exchange is introduced; ambient T enrichment uses only its own savepoint. Recoverable catalog errors preserve typed text fallback without poisoning user work.
- [ ] Broken metadata transport/protocol/cleanup surfaces the canonical source error with original completed_result retained and no original SQL replay; live cache0 pooler/VACUUM acceptance remains t96.
- [ ] Focused RED/GREEN, lint and clean task diff evidence are recorded in one signed local commit.

## Notes & Discoveries

None.
