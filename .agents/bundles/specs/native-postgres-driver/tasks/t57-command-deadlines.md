---
type: Task
id: native-postgres-driver:t57
title: Optional native command I/O deadlines
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t56
files:
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/src/query.rs
- src/rs/vertebra-postgres/src/extended.rs
- src/rs/vertebra-postgres/src/metadata.rs
- src/rs/vertebra-postgres/src/types.rs
- src/rs/vertebra-postgres/src/cancel.rs
- src/rs/vertebra-postgres/src/errors.rs
- src/rs/vertebra-postgres/src/sync_client.rs
- src/rs/vertebra-postgres/src/async_client.rs
- src/py/vertebra/_postgres.pyi
- src/rs/vertebra-postgres/tests/command_deadlines.rs
- src/py/tests/integration/db/test_postgres_command_deadlines.py
tests:
- src/rs/vertebra-postgres/tests/command_deadlines.rs
- src/py/tests/integration/db/test_postgres_command_deadlines.py
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-05T20:03:00Z'
requirements:
- R6
- R7
- R10
- R12
- R18
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Optional native command I/O deadlines

## Objective

Bound an explicitly timed native query's complete command I/O with one absolute deadline, then cancel/retire uncertain sessions safely. Keep normal untimed queries unbounded and distinguish client deadline expiry from actual server cancellation.

## Context

Read `../spec.md`, `../contracts.md`, t40 metadata normalization, t41 execution and t55/t56 generation-fenced cancellation, plus local PyO3/data ownership guidance. This worksheet owns only the listed modules/tests and one signed local feature-branch commit in the complete-driver PR. Estimated 390 handwritten changed lines including focused native/live tests; retain the approximate 400-line heuristic. Preserve other edits. State is pending, revision 1 and plan_commit:null; no implementation starts from this draft.

There is no new global command-timeout default, config registry, alternate client view or source-protocol signature. The existing pool-acquisition, reset and close budgets remain separate. A command timeout is an I/O budget measured after native operation reservation, not a preemptible end-to-end wall-time bound on argument conversion, CPU decoding or Python result projection.

## Public signatures

Retain existing parameter names/defaults and add one keyword-only option to BOTH clients:

```python
query(sql: str, parameters: Sequence[PostgresValue | PgParameter] = (), *,
      parameter_oids: Sequence[int] | None = None,
      timeout: float | None = None) -> PostgresQueryResult
query_multi(sql: str, *, timeout: float | None = None) \
    -> tuple[PostgresQueryResult, ...]
```

AsyncPostgresClient returns Awaitable of the same result types through the existing completion ring. None adds no native command I/O deadline (existing server limits/cancellation still apply); finite positive seconds requests a command deadline. There is no new timeout parameter on pipeline/COPY/portal/source protocols in this task.

## Steps

1. Write RED engine tests in tests/command_deadlines.rs using actual loopback TCP/silent peers and the established cancellation fixture, not preconstructed timeout errors. Hold the command read pending, observe the separate exact CancelRequest(pid,key), then observe old-socket EOF/retirement and a fresh physical connection on next acquire. A silent peer that ignores CancelRequest must still close within the finite cleanup cap. Use barriers and Tokio test-clock advancement for cleanup boundaries where possible; never assert an unrealistically exact wall-clock millisecond. Dribble multiple frames to prove each frame does not renew the original deadline. Cover expiry in required parameter discovery and active result enrichment, skipped optional enrichment after original ReadyForQuery, timeout=None remaining pending until the peer is released, and zero original-statement replay.

2. Validate timeout before native operation reservation: None or positive finite float only, with checked Duration/Instant conversion and no network/lease reservation for invalid zero/negative/NaN/infinity/overflow values. Add session.rs::CommandBudget holding Option<tokio::time::Instant>; compute its absolute Instant exactly once when the existing operation reservation succeeds, before async worker scheduling. Queue delay after that reservation consumes the budget; pool acquire/connect happens under its own configured budget before this raw command reservation. Reuse the existing scope-aware reservation gate, so a timeout option never bypasses pipeline exclusivity. Blocking sync work remains detached from the GIL.

3. Thread this same optional absolute deadline through the existing query/extended/simple-query, parameter catalog, write/flush/read and metadata paths. Introduce production PostgresConnection::{query_with_deadline,simple_query_with_deadline} taking Option<tokio::time::Instant>; existing Rust query/simple_query entry points delegate with None. Internal TypeCatalog/metadata helpers accept the caller's remaining absolute deadline and never start a new per-frame/per-result/per-phase budget. Required custom parameter discovery before Parse counts; query_multi's complete simple Query exchange and all its result sets share one command budget. Retain the existing sole protocol parser; do not add another read loop.

4. On expiry of in-flight command I/O, claim only the existing active OperationGeneration, return native PostgresError::CommandDeadline through the exact canonical OperationalError with sqlstate=None/vendor_code=None, and use t55/t56's independent cleanup owner to send CancelRequest and retire the physical session. Cleanup has one separate absolute cap based on the already validated connection connect_timeout (the fresh cancellation transport's existing bound); it covers cancel dialing/write plus original drain/close together, not a renewed budget per frame/phase. Preserve the sole reader state where available; if an interrupted partial frame makes continued drain uncertain, close/retire rather than restart parsing or reuse that socket. If no command I/O ever began, expiry emits no stale CancelRequest. Query EOF, ignored cancellation or cleanup timeout cannot leak the retired socket/permit beyond the existing owner release boundary. Tests release the owned raw client/lease in finally (or use its managed scope), then prove next acquire gets a different physical session and full capacity. A deliberately retained raw lease or failed ambient transaction is not silently stolen/reacquired; preserve the existing ownership rules. Ambient transactions stay logically failed until explicit rollback, as t55 requires.

5. Keep deadline precedence explicit: once client expiry claims the active command, subsequent actual server 57014 during cleanup does not relabel the primary error as QueryCancelledError or fabricate 57014 on OperationalError. An actual ErrorResponse 57014 received before client deadline expiry remains canonical QueryCancelledError with its real sqlstate. Never replay the SQL, even if the deadline races CommandComplete or the server may have committed. If original ReadyForQuery was fully recorded before optional enrichment starts and the remaining budget is exhausted, skip that optional metadata I/O and return the original completed result with exact text/OID fallback and its captured CodecContext. If expiry occurs during active enrichment and cleanup/transport is unrecoverable, preserve PostgresError::AfterCompletion{source:CommandDeadline,completed_result}; the existing mapper attaches the ORIGINAL completed_result, not catalog results. Required pre-execution metadata expiry has no fabricated completed result.

6. Extend both existing client methods/stubs and canonical error classification/Display matches without adding a Python exception class. Map CommandDeadline to OperationalError (no synthetic SQLSTATE and no retry promise); preserve actual server cancellation and asyncio CancelledError behavior. CPU decoder work and Python projection are not interruptible/preemptible by this I/O deadline; a large synchronous decode/projection or event-loop scheduling delay can exceed the requested duration. Do not advertise timeout as a total wall-time guarantee. Retain nested parameter ownership and canonical rich/public-row projection unchanged.

7. Add focused sync/async tests in integration/db/test_postgres_command_deadlines.py using the EXISTING pytest-databases postgres_service primitive fields (host/port/user/password/database) and direct native clients/pools, sslmode=disable for that local service. No future high-level adapter or DSN parser is a prerequisite. Test invalid values before I/O, None/default successful calls, query_multi's shared budget, a command deadline during pg_sleep yielding exact OperationalError/sqlstate None and a replacement backend PID, and actual server statement_timeout yielding QueryCancelledError/sqlstate 57014 BEFORE a larger client timeout. Parameterize both clients and use max_size=1 for replacement proof. Forwarding from high-level Driver kwargs is owned by later cutover tasks: timeout is COMMAND-only, execute_many applies it per command rather than claiming a whole-batch budget. The later specialized native notification publisher passes timeout=10.0 explicitly while acquisition/reset retain their own bounds; its publish protocol signature is unchanged. Commit feat(postgres): bound explicitly timed native command I/O.

## Verification

Capture initial RED for the absent timeout surface/native expiry behavior, then focused GREEN with meaningful silent-peer and real service assertions:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test command_deadlines
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test cancellation_protocol
make dev
uv run --no-sync pytest src/py/tests/integration/db/test_postgres_command_deadlines.py -q
uv run --no-sync python -m mypy.stubtest vertebra._postgres
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
git diff --check
```

The integration file uses the pre-existing postgres_service fixture; raw constructor primitives avoid an unavailable later adapter/compiler. Native rebuild precedes Python tests. Record commands, exit codes, named cases and wire/EOF/new-PID assertions. These are planned commands, not executed evidence. The complete parent still owns matrix/wheel/free-threaded/benchmark/full review acceptance.

## Acceptance Criteria

- [ ] R6/R7/R12: query/query_multi on sync/async clients accept optional keyword-only timeout=None; None preserves untimed commands and invalid values fail before native reservation/I/O.
- [ ] One absolute reservation deadline covers every command/catalog/write/read/result-set phase without resets; acquisition/reset budgets remain independent.
- [ ] R10/R18: expiry uses canonical OperationalError/sqlstate None plus fenced protocol cancellation and bounded retirement; real server 57014 stays QueryCancelledError and asyncio cancellation retains its own semantics.
- [ ] Silent/blocked peers prove CancelRequest, old-socket EOF, finite cleanup, recovered capacity and fresh backend use without command replay or premature ambient-transaction reuse.
- [ ] Optional enrichment may be skipped after completed ReadyForQuery on budget exhaustion; fatal active enrichment preserves the original AfterCompletion.completed_result/captured context.
- [ ] Stubs and tests prove timeout is native command I/O only, not decoder/Python/wall-time preemption; specialized notification use is explicit and no global command default is introduced.
- [ ] Fresh RED/GREEN, native/live SQLSTATE and lint evidence is recorded in one signed local commit; full acceptance remains pending.

## Notes & Discoveries

None.
