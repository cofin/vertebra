---
type: Task
id: "native-postgres-driver:t45"
title: "Concrete Rust physical connection pool and leases"
description: "Concrete Rust physical connection pool and leases for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t44"
files:
  - src/rs/vertebra-postgres/src/pool.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/tests/physical_pool.rs
tests:
  - src/rs/vertebra-postgres/tests/physical_pool.rs
plan_revision: 2
plan_commit: 9af810f5
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

# Concrete Rust physical connection pool and leases

## Objective

Own authenticated physical PostgreSQL sockets in a reusable Rust rlib pool with bounded real leases.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED multi-connection scripted-server tests through PostgresPool::new/acquire for min/max bounds, saturation timeout, cancelled waiter, connect failure, close during acquisition and unique simultaneous backend PIDs. A lease must execute an actual query; SHM slot counters alone do not satisfy the test.

2. Implement public pool.rs::{PostgresPool,PostgresPoolOptions,PooledConnection,PoolStats}. Construction validates/copies and performs no network or credential refresh; the first acquire starts bounded lazy minimum-size initialization. The pool owns idle PostgresConnection objects, bounded permits and a connector built from PostgresConfig. PooledConnection owns exactly one physical socket plus permit/generation and dereferences explicitly to PostgresConnection for Rust consumers; no trait-object ABI crosses extension boundaries.

3. Reserve capacity before connect; cancellation/failure drops reservation and partially opened stream. Bounds count opening+idle+borrowed sessions. Acquisition deadline covers waiting and connection setup without extending a configured authentication/connect deadline. Store one connector/config credential identity per pool; token refresh applies on new connects without mixing existing authentication identity. Pool construction owns the complete immutable connection identity; never trust a caller-provided cache key as proof that credentials/TLS/GUC/IAM configuration match. The adapter registry may share an existing pool only after the same canonical native configuration comparison, and share_pool=False bypasses that sharing.

4. Track idle age/max lifetime and broken-session eviction. Ensure fair waiter wakeup without holding pool locks across network I/O. A leased connection is unavailable to every other caller, including transactions and portals. Install an RAII return state that t46 completes with reset; until then only known-idle/unchanged sessions can be requeued, all others retire.

5. Export concrete Rust types with explicit lib.rs names for broker-pool-sharing consumers (`driver-fail-loud` already removed the connectionless SHM admission pool helpers from `vertebra-db`). Commit feat(postgres): pool real PostgreSQL connections in Rust.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test physical_pool
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R14: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every successful acquire owns an actual authenticated socket and unique lease.
- [ ] Saturation/cancel/connect failure/close preserve bounded capacity without leaks.
- [ ] The rlib exports concrete pool/lease types without cross-cdylib Rust trait pointers.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
