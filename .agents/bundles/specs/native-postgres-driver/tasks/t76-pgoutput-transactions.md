---
type: Task
id: native-postgres-driver:t76
title: Order logical transactions and change envelopes
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t75
files:
- src/rs/vertebra-postgres/src/replication/pgoutput.rs
- src/rs/vertebra-postgres/src/replication/mod.rs
- src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs::committed_transaction_order
- src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs::truncate_origin_and_messages
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-06T06:30:00Z'
commit: null
tags:
- postgres
- native
- data-events
requirements:
- R22
---

# Order logical transactions and change envelopes

## Objective

Order logical transactions and change envelopes; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 400 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Golden Begin(B)/Commit(C), Insert(I), Update(U with K/O old and N new), Delete(D), Truncate(T), Origin(O) and Message(M) transcripts assert exact transaction fields/order, multiple relations, replica identity keys/full old row, unchanged TOAST, truncate cascade/restart-identity and transactional/nontransactional logical messages. Reject DML outside transaction, nested Begin, wrong XID/order, unsupported streaming/two-phase tags and nonzero reserved flags.
2. Produce bounded owned `ReplicationRecord` variants Begin/Insert/Update/Delete/Truncate/Message/Commit with immutable relation snapshot and `PgValue` tuples. Transaction state retains only XID/final_lsn/commit_time/ordinal plus current message, not whole transaction. Proto-v1 streaming=false means PostgreSQL publishes committed transactions; nevertheless consumers receive BEGIN...COMMIT and must stage until COMMIT because disconnect can interrupt delivery. Truncate stores ordered relation identities and flags.
3. Every record payload includes source identity, xid, begin_final_lsn, transaction_ordinal, schema/table/OIDs, old/new/key values and unchanged-toast markers where relevant. Stable dedup key is source(system_identifier,slot)+begin_final_lsn+ordinal; never pack LSN into generic signed64 event_id. Origin updates transaction metadata. Message includes prefix/content bytes/transactional/LSN; standalone nontransactional M is emitted immediately but cannot advance commit-only acknowledgement until next COMMIT.
4. Bound record bytes(default16MiB), queue records(default1024) and queue bytes(default8MiB); no silent drop. Reader/app backpressure does not block independent feedback writer(t77). One oversized record fails explicitly without acknowledging; disconnect midtransaction discards decoder state for replay. Commit `feat(postgres): order pgoutput transaction records`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_pgoutput_tests
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Require final exit0 with actual command/exit and asserted behavior recorded. Rebuild before Python tests; no stale-extension evidence. No external write, push, tags or hosted workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R22: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All listed normal, malformed, reconnect, backpressure and lifetime cases pass.
- [ ] Dedicated session ownership and bounded resources hold; feedback never acknowledges unprocessed data.
- [ ] One owned local commit is reviewable; worksheet state and spec checklist synchronize after verification.

## Notes & Discoveries

None.
