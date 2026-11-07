---
type: Task
id: "native-postgres-driver:t59"
title: "Captured timezone text and exact ambiguous temporal fallback"
description: "Captured timezone text and exact ambiguous temporal fallback for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t58"
files:
  - src/rs/vertebra-postgres/src/codecs/timezone_text.rs
  - src/rs/vertebra-postgres/src/codecs/datetime_text.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/python_values.rs
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/Cargo.lock
  - src/py/vertebra/db/adapters/postgres/types.py
  - src/rs/vertebra-postgres/tests/timezone_text.rs
  - src/py/tests/unit/db/test_postgres_native_queries.py
tests:
  - src/rs/vertebra-postgres/tests/timezone_text.rs
  - src/py/tests/unit/db/test_postgres_native_queries.py
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

# Captured timezone text and exact ambiguous temporal fallback

## Objective

Close every valid session-style temporal path with either a uniquely resolved instant or exact tagged source text and context.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED fixtures for non-ISO timestamptz abbreviations under IANA/fixed/POSIX TimeZone, DST fold/gap, historical LMT and out-of-library-range PG years. Prove an ambiguous abbreviation cannot be interpreted through a global abbreviation table. Test exact fallback survives query/pipeline/Python conversion, typed rebind raises DataError before submission in text/binary/pipeline/COPY paths even when captured context matches, and verify changed timezone_abbreviations cannot bypass this refusal.

2. Implement timezone_text.rs::resolve_or_capture_temporal. Use jiff = "=0.2.39" with the maintained bundled timezone database (tzdb-bundle-always feature) for named/POSIX resolution strictly within its documented timestamp/civil range, or recognize explicit fixed numeric offsets directly. Require displayed local fields and abbreviation to identify exactly one instant; never extrapolate beyond the vetted library range. Owned lexical PG calendar arithmetic from t32 remains the only raw full-range calculation.

3. When no unique instant is recoverable, retain PgValue::TemporalText {kind,wire_text,date_style,time_zone}; Python uses PgTemporal(kind,value=None,special="wire_text",wire_text=...,date_style=...,time_zone=...). Exact text/context is a lossless documented value, not a plain fallback str. Ordinary binary and unambiguous ISO-offset paths keep raw PG units and native datetime where representable.

4. Reject every unresolved TemporalText/PgTemporal(special="wire_text") typed bind with canonical DataError before submission in text, binary, pipeline and COPY paths, even when captured DateStyle and TimeZone match. Matching output settings cannot prove input invertibility: timezone_abbreviations independently configures accepted abbreviations and is not fully represented by those captured fields. No first-operation pipeline exception exists. Preserve the exact display/context as output; callers must explicitly resolve the instant into a raw temporal value or intentionally bind their chosen raw text. Never decode arbitrary binary into this wrapper.

5. Document field invariants, invalid construction and exact fallback behavior in the public type docstrings; Arrow/COPY tasks consume this same variant and own their output mapping/preflight. Resolve lockfile and commit feat(postgres): preserve ambiguous temporal output with context.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test timezone_text
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py -q
cargo check --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every valid output style preserves an exact epoch value or tagged source/context without guessed offsets.
- [ ] Unresolved tagged temporal text rejects typed rebinds before submission in all text/binary/pipeline/COPY paths, including matching-context and first-operation cases; exact output/context survives unchanged.
- [ ] Jiff range and DST ambiguity are explicit; binary temporal support remains full and lossless.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
