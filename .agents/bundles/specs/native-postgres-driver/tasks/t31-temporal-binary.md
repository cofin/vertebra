---
type: Task
id: "native-postgres-driver:t31"
title: "Lossless temporal and interval binary codecs"
description: "Lossless temporal and interval binary codecs for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t30"
files:
  - src/rs/vertebra-postgres/src/codecs/temporal.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/tests/temporal_codecs.rs
tests:
  - src/rs/vertebra-postgres/tests/temporal_codecs.rs
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

# Lossless temporal and interval binary codecs

## Objective

Decode and encode date/time/timestamp/timestamptz/interval binary data with exact PostgreSQL epoch units.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 360 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED send-format fixtures around 2000-01-01, pre-epoch dates/timestamps, leap days, timezone-equivalent instants, 24:00 time, date/timestamp infinities, server-supported BC/out-of-Python-range values, interval mixed signs/months and PG18 interval infinity sentinels. Exercise wrong widths, microsecond overflow and unsupported temporal OIDs.

2. Implement fixed-width big-endian codecs preserving raw date i32 days, time i64 microseconds since midnight, timestamp/timestamptz i64 microseconds from PostgreSQL epoch, and interval independent i32 months/i32 days/i64 microseconds. Validate legal finite ranges, time24 boundary and infinity sentinel combinations. Do not convert interval months to timedelta days or raw times to nanoseconds.

3. Keep timestamp without timezone distinct from an instant. Python conversion is t47: supported values map to native date/time/datetime, UTC-aware timestamptz, otherwise PgTemporal(kind,value,special). Interval always maps to PgInterval, including exact sentinel triples. Register the binary handlers and expose reuse by COPY and Arrow.

4. Use checked calendar arithmetic for boundary tests; no local machine timezone or floating point epoch conversion enters the wire codecs. Commit feat(postgres): preserve temporal binary ranges and intervals.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test temporal_codecs
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Temporal binary bytes and PG epoch units match live-server fixtures.
- [ ] Intervals preserve independent fields and special values without timedelta loss.
- [ ] Infinities, time24 and out-of-Python-range values have exact model representations.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
