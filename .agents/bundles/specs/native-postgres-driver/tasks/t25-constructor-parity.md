---
type: Task
id: native-postgres-driver:t25
title: Characterize the complete sync and async constructor contract
state: pending
priority: P1
verification_strategy: characterization
depends_on:
- native-postgres-driver:t24
files:
- src/py/tests/unit/db/test_postgres_driver.py
tests:
- src/py/tests/unit/db/test_postgres_driver.py::test_native_constructor_signature_parity
- src/py/tests/unit/db/test_postgres_driver.py::test_native_constructor_full_options_are_pure
- src/py/tests/unit/db/test_postgres_driver.py::test_native_constructor_mapping_copy
plan_revision: 1
plan_commit: null
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
updated_at: '2026-11-05T20:03:00Z'
commit: null
tags:
- postgres
- connection
- native
requirements:
- R2
- R6
- R7
---

# Characterize the complete sync and async constructor contract

## Objective

Characterize the complete sync and async constructor contract, making the full public constructor reachable before facade acceptance.

## Context

Read ../spec.md, ../contracts.md::Connection constructors, direct dependencies and .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md. All work stays in the one complete-driver feature branch/PR. No implementation starts with plan_commit null. Ownership is exactly the listed files, sequenced after predecessors; preserve other edits. This is one invocation and one signed conventional local commit, estimated250 handwritten lines including tests. If near400 cannot hold, revise worksheets without shrinking the required one-PR scope.

## Steps

1. Add characterization tests for both real native classes using the exact signature/default inventory in ../contracts.md. Inspect signature and exercise valid optional kwargs, unexpected/positional arguments and every semantic conflict from t20. Both classes must expose Unix, binding, session GUC, keepalive, cloud URI/network/IAM and separate Admin/login maps; no reduced direct-only constructor is accepted. Stubtest remains the signature runtime/stub gate.
2. For both classes, construct cloud and Unix configurations while local server accepts, DNS spies and credential refresh endpoints record no calls. Unavailable credentials are not fetched until connect. Mutate caller session_gucs after construction; a real controlled startup observes the original copied values. For both auth maps verify str:str type enforcement and pure construction, while native t20 config tests/code ownership establish owned snapshots rather than borrowed Python maps. Reuse t23/t24 fixture; do not add imaginary public Admin endpoint kwargs or contact Google services. Include TypeError for non-string map entries and secret-redaction assertions.
3. Test public channel_binding=require against a TLS PLUS-capable scripted server and Unix sslmode disable startup with session_gucs through the actual Python constructor. Native Rust TLS/auth tests still own exhaustive wire matrices; this test proves Python option reachability rather than duplicating their cryptography. Pool constructor parity uses this same connection signature in later owning tasks.
4. No production parser or signature is invented in this task; if characterization reveals projection drift, fix only the owning earlier worksheet via /flow:revise before committing. Commit test(postgres): preserve complete constructor parity.

## Verification

Run characterization against rebuilt native classes and record actual baseline. No manufactured RED is required for already implemented behavior; discovered failures must be repaired in their owning worksheet.

```bash
make dev
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_constructor -q
uv run --no-sync python -m mypy.stubtest vertebra._postgres
```

Record actual command exits/assertions during execution; these are planned checks, not runtime evidence. No external repository write or workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R2, R6, R7: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every named public option is reachable through owned validation/projection, with no ignored fields or duplicate DSN parser.
- [ ] The listed canonical positive/negative tests pass and all commands exit0.
- [ ] Task and parent checklist state agree after one verified local commit.

## Notes & Discoveries

None.
