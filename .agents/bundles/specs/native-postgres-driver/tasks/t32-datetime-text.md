---
type: Task
id: native-postgres-driver:t32
title: Temporal text decoding across session output styles
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t31
files:
- src/rs/vertebra-postgres/src/codecs/datetime_text.rs
- src/rs/vertebra-postgres/src/codecs/temporal.rs
- src/rs/vertebra-postgres/src/codecs/mod.rs
- src/rs/vertebra-postgres/tests/datetime_text.rs
tests:
- src/rs/vertebra-postgres/tests/datetime_text.rs
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
- R11
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Temporal text decoding across session output styles

## Objective

Honor valid TimeZone and DateStyle session settings while decoding cold/simple-query temporal text losslessly.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

The worksheet parses PostgreSQL output, not the unrestricted SQL input grammar. It introduces no new timezone engine. t59 finalizes exact captured-text fallback binding, optional vetted zone resolution and validation; this task must not guess a missing numeric offset.

## Steps

1. Write RED fixtures for DateStyle ISO/SQL/Postgres/German with MDY/DMY/YMD ordering, finite date/time/timestamp/timestamptz, BC years, large years, infinities, fractional seconds, 24:00, signed numeric UTC offsets including seconds, and unambiguous UTC/GMT labels emitted by Postgres style. Unresolved timezone abbreviation and tagged fallback coverage belongs to t59, not a silently omitted style. Fixtures include ParameterStatus updates between results and explicit session_gucs overrides.

2. Implement datetime_text.rs::{decode_date_text,decode_time_text,decode_timestamp_text,encode_temporal_text}. Dispatch by current CodecContext DateStyle and TimeZone, retain timestamp timezone semantics, and convert calendars to raw PG epoch units with checked integer arithmetic. Parse server output, not the unrestricted SQL input grammar. Use existing chrono utilities only within their supported ranges and owned checked Gregorian arithmetic for PostgreSQL raw ranges; never assume chrono covers the server date/timestamp range.

3. For timestamptz explicit numeric offsets determine the instant; recognized unambiguous UTC/GMT spellings have offset zero. Return the owned TemporalText representation with original wire_text and captured DateStyle/TimeZone for other abbreviation-only forms, to be validated and resolved/finalized by t59. Do not guess an abbreviation globally or reject a valid session configuration. Preserve a tagged exact raw temporal value on Python-range overflow; malformed unambiguous server text raises DataError. All unambiguous supported output ranges map to exact PG epoch integers.

4. Update CodecContext immediately on ParameterStatus and sample it at each result description; never reject a valid existing session_gucs setting as a shortcut. Canonical text bind output uses unambiguous ISO forms with explicit offsets and preserves infinity/BC/time24. Verify every DateStyle under both simple and cold extended text decoders. Commit feat(postgres): decode temporal text under session styles.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test datetime_text
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All four DateStyle output forms decode under the active session configuration.
- [ ] TimeZone overrides and explicit UTC offsets preserve exact instants; uncertain abbreviation text remains tagged for t59.
- [ ] Valid session_gucs remain supported and Python range overflow uses PgTemporal.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
