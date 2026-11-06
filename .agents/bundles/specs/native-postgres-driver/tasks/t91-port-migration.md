---
type: Task
id: native-postgres-driver:t91
title: Activate both permanent native PostgreSQL drivers and consumers atomically
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t90
files:
- src/py/vertebra/db/adapters/postgres/driver.py
- src/py/vertebra/db/adapters/postgres/__init__.py
- src/py/vertebra/db/adapters/postgres/embeddings.py
- src/py/vertebra/db/adapters/postgres/notify.py
- src/py/vertebra/extensions/sqlspec/adapters.py
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/tests/unit/db/test_postgres_driver.py
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
- src/py/tests/unit/db/test_postgres_native_adapters.py
- src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
- src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
- src/py/tests/unit/db/test_postgres_event_hooks.py
- src/py/tests/unit/extensions/test_postgres_compiler_transport.py
tests:
- src/py/tests/unit/db/test_postgres_driver.py
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
- src/py/tests/unit/db/test_postgres_native_adapters.py
- src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
- src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
- src/py/tests/unit/db/test_postgres_event_hooks.py
- src/py/tests/unit/extensions/test_postgres_compiler_transport.py
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
- cutover
- native
requirements:
- R24
---

# Activate both permanent native PostgreSQL drivers and consumers atomically

## Objective

Publish the completed native sync/async pair, parameter compiler defaults and helper SQL together, preserving every existing regression assertion in one green activation commit.

## Context

Read `../spec.md`, final `_sync.py`/`_async.py` implementations and t85, t86, t87, t88, t89, t90 completion evidence. Legacy Async.__init__ constructs legacy Sync with executor and has a static JSONB reference; replacing only Sync earlier breaks that class and Mypy. Before this task both old public classes remain selected. This task imports both final classes into canonical driver.py in the same commit and never selects either old implementation afterward.

One executor invocation creates one signed conventional local commit; own only listed symbols/files and preserve unrelated edits. Estimated activation plus test migration300–400 changed lines, using already completed native port support. Legacy-body deletion is explicitly split into immediate t92/t93 characterization commits, avoiding an800-line deletion here; all three commits remain on the same feature branch and complete PR. No implementation with null plan_commit or public/hosted action is authorized.

## Steps

1. Rename old driver.py class definitions PRIVATE `_LegacySyncPostgreSQLDriver` and `_LegacyAsyncPostgreSQLDriver`. Adjust both legacy async references to `_LegacySyncPostgreSQLDriver`: constructor and decode_binary_jsonb static call. Import final SyncPostgreSQLDriver from ._sync and AsyncPostgreSQLDriver from ._async at grouped module imports, retain canonical __all__ names and package lazy `_EXPORT_MAP` targets pointing at driver.py. No old class remains in __all__, config class names, registry selectors or consumer imports, and there is no runtime fallback/flag/dynamic engine selection. The two private bodies are unreachable cleanup remnants, not compatibility aliases; t92/t93 remove them immediately.
2. Activate completed t87 SQLSpec transport helper at actual PostgreSQL execute/select/many/Arrow/stack boundaries exactly once, set both PostgreSQL SQLSpec default execution profiles NUMERIC and wire registered PostgreSQL SQLAlchemy numeric_dollar DBAPI/default profile. Preserve explicit caller StatementConfig identity and its original question-mark compilation/provenance, while the native transport copy is numeric. Existing MySQL/SQLite DBAPI formats and filter/typed-parameter semantics remain unchanged. Non-idempotent transformer regression proves no double application.
3. In the same commit change PostgresEmbeddingPushdown sync marker choice to numeric for both drivers and verify PostgresNotificationSource.publish selects the completed uniform async hook on both newly activated public drivers, preserving bound values/order/normalization/identifier validation. Both native publisher implementations already use numeric builders. Rewrite real-psycopg-converter regression to native ports while retaining tenant setter failure propagation, notify, emit-message, embedding/search and exact parameter assertions. Retain the completed source notification/CDC behavior.
4. Migrate every PostgreSQL legacy test port to shared native client/pool ports from t85/t86, preserving real observable store behavior. Replace _SQLiteAsyncPGPool/Transaction/Connection and MockCursor/MockConnectionPool, memory:///_in_memory_rows and executor injection, retaining DDL/DML counts, failed begin retry, rollback/cancellation, balanced leases, empty metadata and cloud-dialer wiring. Preserve useful CPU codecs and unrelated MySQL/SQLite branches. Migrate existing _NativeBindConnection PostgreSQL cases while leaving MySQL coverage. Keep explicit question-mark/config identity assertions at original compiler boundary, assert numeric transport separately, and retain stack result mismatch/rollback/many/Arrow provenance checks. Delete unused legacy test fixtures only after their assertions are represented by native-shaped support.
5. Switch the new native adapter/pipeline/Arrow/event tests from final-module direct imports to canonical driver/package imports. Change staged pure transport-helper tests to exercise the same behavior through real canonical PostgreSQL SQLSpec/SQLAlchemy public methods and prepared objects, rather than retain a private test-only entry point. Assert public package and driver exports are the exact final classes defined in `_sync.py`/`_async.py`; config.resolve_driver_classes/DatabaseRegistry and SQLSpec/SQLAlchemy consumers instantiate those same classes. Exercise real native-shaped store mutation/query/transaction through canonical paths; no private test-only method/import surface remains. Verify optional wheel missing-extra message names vertebra-core[postgres]. Run every touched test module, full relevant Ruff and Mypy before commit. Commit feat(postgres): activate complete native adapter pair.

## Verification

Record behavioral RED cases for the new canonical activation/identity/bind assertions before atomic edit, then require the entire touched suite green. Existing baseline passing tests remain passing; no acceptance is postponed to the deletion commits. Commands are planned:

```bash
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/extensions/test_postgres_compiler_transport.py -q
uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy src/py/tests/unit/db src/py/tests/unit/extensions
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions
```

## Acceptance Criteria

- [ ] R24: every canonical sync/async/registry/consumer path selects the completed final native classes atomically; no old execution path is selected.
- [ ] All existing touched assertions retain behavior through native ports and explicit compiler/helper activation, with no intermediate Mypy/runtime coupling failure.
- [ ] Public test imports are canonical; immediate t92/t93 remove unreachable private bodies before conformance/delivery; one signed local activation commit is recorded.

## Notes & Discoveries

None.
