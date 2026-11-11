---
type: Task
id: "native-postgres-driver:t30"
title: "Arbitrary precision numeric binary and text codecs"
description: "Arbitrary precision numeric binary and text codecs for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t29"
files:
  - src/rs/vertebra-postgres/src/codecs/numeric.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/tests/numeric_codecs.rs
tests:
  - src/rs/vertebra-postgres/tests/numeric_codecs.rs
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

# Arbitrary precision numeric binary and text codecs

## Objective

Preserve PostgreSQL numeric precision, declared scale and special values in both formats.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 370 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED PostgreSQL numeric_send golden fixtures for 0, negative values, 1.2300, leading/trailing base10000 groups, negative weights, very large precision, maximum accepted scale, NaN and positive/negative infinity. Add malformed sign, digit>=10000, negative digit count, truncated header/body, impossible length, unchecked exponent/allocation and trailing-byte rejection.

2. Implement numeric.rs::{decode_numeric_binary,encode_numeric_binary,decode_numeric_text,encode_numeric_text} using checked base10000 groups and the t27 arbitrary coefficient/scale representation. Preserve dscale independently of coefficient trailing zeros; zero normalizes sign without dropping scale. Validate server wire limits before allocation. Parse exponent text without fixed-width coefficient or float intermediary.

3. Register numeric OID1700 and its array element behavior in the dispatcher. Encode Python Decimal-compatible coefficient/scale/special values without invoking the current decimal Context precision or rounding. Define exact behavior for signaling NaN input: reject with DataError before submission because PostgreSQL has no signaling-NaN distinction.

4. Test encoder output against native server numeric_recv/send fixtures and independent text expectations, including values beyond rust_decimal capacity. Commit feat(postgres): preserve numeric precision and special values.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test numeric_codecs
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Numeric encoding never narrows through float or fixed precision.
- [ ] Scale, finite extremes, NaN and infinities round-trip with independently checked bytes.
- [ ] Malformed numeric packets fail within configured size bounds.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
