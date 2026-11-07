---
type: Task
id: native-postgres-driver:t99
title: Implement reproducible PostgreSQL benchmark capture and comparison harness
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t98
files:
- tools/benchmarks/postgres.py
- src/py/tests/unit/tools/test_postgres_benchmarks.py
- pyproject.toml
- uv.lock
tests:
- src/py/tests/unit/tools/test_postgres_benchmarks.py
requirements:
- R27
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
created_at: '2026-11-05T00:00:00Z'
updated_at: '2026-11-06T06:30:00Z'
commit: null
tags:
- postgres
- benchmarks
- harness
---

# Implement reproducible PostgreSQL benchmark capture and comparison harness

## Objective

Create a truthful machine-readable native/reference PostgreSQL capture format and a rejecting comparison gate before measuring the complete driver.

## Context

Read `../spec.md`, tools.benchmarks store/report/CLI conventions and frozen native client/pipeline contracts. This task follows t98 and precedes t100; t100 owns native scenario wiring, service orchestration, Makefile targets and actual measurements. One executor invocation/one signed conventional local commit; shared files preserve others' edits. Implementation plus tests320–390 lines, generated lock diff reviewed separately. No production implementation while plan_commit is null. Dev-only database reference clients never enter application driver dependencies or execution fallbacks.

## Steps

1. RED unit tests feed small deterministic capture fixtures into comparison: each individual median/p95/throughput boundary, threshold equality, missing scenario, mismatched dataset/server/interpreter/run identity, zero counts/nonfinite durations, malformed schema, driver-version mismatch, retry exhaustion and secret-bearing metadata rejection. A constant-success comparator or comparison of unmatched scenarios must fail these tests. Unit tests do not run real performance measurements.
2. Add dev bench-group `psycopg[binary]==3.2.13` and `asyncpg==0.31.0`, retaining exact transitive binary package resolution in uv.lock; register postgres_benchmark marker and default `addopts=-m "not postgres_benchmark"` so normal test gates never run performance capture, with dedicated CLI `-m postgres_benchmark` overriding that selection; no broad dependency upgrade. Verified pinned APIs are psycopg.connect(...,autocommit=True,prepare_threshold=0), Connection.execute(...,prepare=True), Connection.pipeline()/Pipeline.sync() and asyncpg.connect(...,ssl=False), connection.prepare(...), prepared.fetch/fetchval and connection.close. Pipeline reference uses psycopg3 sync and AsyncConnection pipeline APIs rather than pretending asyncpg exposes pipeline. These imports are local to reference capture, never production postgres driver code.
3. Implement `python -m tools.benchmarks.postgres capture --implementation reference|native --dsn-env VERTEBRA_POSTGRES_BENCH_DSN --output <path>` and `python -m tools.benchmarks.postgres compare --reference <reference.json> --native <native.json> --output <comparison.json>`. Capture DSN comes only from named environment variable and is never printed, logged, embedded in exceptions or report. Compare reads only the two report files, requires no DSN/environment lookup and opens no network connection. Capture validates CPython3.12 standard GIL build, test-owned PostgreSQL18 endpoint with explicit sslmode=disable, scenario/dataset config and owner token. Native scenario implementation is supplied by t100 through private owned function dispatch; reference capture has real pinned clients now, not success placeholders for unimplemented native mode (native invocation fails clearly until t100).
4. Freeze schema_version=1. Capture fields: capture_id/paired_run_id, implementation, CPython/platform/GIL, package/native/git versions, server_version_num, server settings, host hardware/cpu/cores/memory, transport/TLS mode, dataset_fingerprint, preparation/reset policy, warmup_count, per-scenario repetition/sample counts, raw integer duration_ns samples, median_ns, p95_ns, statements_s, coefficient_of_variation and retry_count. Exclude endpoint credentials and SQL parameter values. Use adapter serializers and frozen msgspec records. Each matching scenario's native median<=1.25*reference, p95<=1.50*reference and statements_s>=0.80*reference; missing/noncomparable report fails. CV is standard deviation/mean of the five per-sample mean per-operation latencies (cold groups20, singles1000, pipeline batches100 normalized to64 statements), while median/p95 use all recorded measured operation latencies. CV>0.15 marks run invalid and allows exactly one fresh whole paired-capture retry, then fails; never discard individual slow samples or silently loosen thresholds.
5. Capture protocol:10 unmeasured warmups per scenario; cold direct authenticated connect+close100 measured repetitions total grouped5 samples of20. Warm retained-lease/prepared single-query1000 operations/sample,5 samples. Pipeline64 statements/batch×100 batches/sample,5 samples, matching ambient retained transaction plus nested savepoint and all row consumption included per measured batch; outer commit/reset is outside that metric and separately reported. Sync single/cold reference psycopg3; async single/cold asyncpg; sync/async pipeline reference psycopg3 corresponding API. Every sample verifies actual result count/value and completed transaction. Record pooled acquire/reset/DISCARD cost as separate diagnostic scenarios, not mislabeled prepared-query speed or unequal native/reference gate. Shared dataset setup/truncate/rollback belongs to t100. Commit test(postgres): add reproducible benchmark comparison harness.

## Verification

Record actual deterministic RED failures and passing comparison/CLI behavior. No live benchmark runs during planning. Commands planned for execution:

```bash
uv lock --check
uv run --no-sync pytest src/py/tests/unit/tools/test_postgres_benchmarks.py -q
uv run --no-sync python -m tools.benchmarks.postgres --help
uv run --no-sync ruff check tools/benchmarks/postgres.py src/py/tests/unit/tools/test_postgres_benchmarks.py
uv run --no-sync mypy tools/benchmarks/postgres.py
```

## Acceptance Criteria

- [ ] R27: capture/reference APIs are pinned, report schema/counts are explicit and bad/mismatched/slow/noisy reports fail with deterministic tests.
- [ ] Reference dependencies are bench-only and no credentials or runtime acceptance claims enter planning evidence.
- [ ] One local signed conventional commit and task-first state/evidence synchronization precede native measurements.

## Notes & Discoveries

None.
