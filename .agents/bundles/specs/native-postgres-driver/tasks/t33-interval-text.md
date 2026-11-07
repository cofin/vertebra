---
type: Task
id: "native-postgres-driver:t33"
title: "Lossless interval text for all PostgreSQL styles"
description: "Lossless interval text for all PostgreSQL styles for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t32"
files:
  - src/rs/vertebra-postgres/src/codecs/interval_text.rs
  - src/rs/vertebra-postgres/src/codecs/temporal.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/tests/interval_text.rs
tests:
  - src/rs/vertebra-postgres/tests/interval_text.rs
plan_revision: 2
plan_commit: cdb978d3
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

# Lossless interval text for all PostgreSQL styles

## Objective

Support postgres, postgres_verbose, sql_standard and iso_8601 output without flattening months or mixed signs.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 350 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED server fixtures for each IntervalStyle over year/month/day/time fields, zero, mixed-sign month/day/microsecond triples, negative fractional seconds, verbose ago, SQL year-month signs, large values and PG18 interval infinities. Include ParameterStatus style changes between results and malformed/missing designators/overflow cases.

2. Implement interval_text.rs::{decode_interval_text,encode_interval_text} as bounded style-specific output grammars selected through CodecContext. Accumulate years into months and hours/minutes/seconds into microseconds with checked integers and fixed decimal fractions. Apply each style sign convention exactly; never use float seconds or timedelta.

3. Canonical text parameter output is an unambiguous ISO8601 interval preserving independent signs and microseconds; retain server-compatible infinity spellings only where server version supports them. Register interval text dispatch and test binary-versus-text agreement for each finite fixture. Reject malformed forms with DataError before entering a COPY stream or submitting a pipeline.

4. Preserve session_gucs IntervalStyle, and ensure warm binary results/cold text results produce the same PgInterval fields. Commit feat(postgres): decode interval text without month loss.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test interval_text
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test temporal_codecs
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All four IntervalStyle outputs preserve independent interval fields.
- [ ] Mixed signs, fractional microseconds and infinity behavior agree with server fixtures.
- [ ] Text and binary interval projections share one lossless model.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
