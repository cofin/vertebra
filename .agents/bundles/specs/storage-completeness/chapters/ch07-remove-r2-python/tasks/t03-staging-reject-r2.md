---
type: Task
id: storage-completeness:ch07:t03
chapter_id: ch07-remove-r2-python
title: Reject removed R2 URIs before staging local-path resolution
description: Reject removed R2 URIs before staging local-path resolution
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch07:t02
read_set:
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/vertebra/db/adapters/bigquery/driver.py
- src/py/vertebra/db/adapters/bigquery/config.py
- src/py/vertebra/db/_staging.py
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
files:
- src/py/vertebra/db/_staging.py
tests:
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 130
plan_revision: 6
plan_commit: 43c2e3fe
state_revision: 0
claimed_by: agent:r2-staging-withdrawal
claimed_at: '2026-11-07T22:46:57.858525+00:00'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-07T23:09:15.383841+00:00'
last_verified_commit: 38757809
verification_evidence: |
  Signed source and canonical test checkpoint 38757809.
  Initial R2 Red: uv run --no-sync flock /tmp/vertebra-flow-shm-tests.lock
  pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  src/py/tests/unit/extensions/test_data_storage_integrations.py -q -k staging
  exits 1 (10 failed, 4 passed, 92 deselected). Real explicit Memory store and
  SQLSpec validation/read/write cases expose accepted removed qualification.
  Amended qualification Red: the same command exits 1 (30 failed, 15 passed,
  92 deselected) with real native/backend mismatches and actual BigQuery HTTP
  connections to /bigquery/v2/projects/unit/queries. Matching rooted aliases and
  literal percent keys pass before mismatch assertions expose the bypass.
  Removed explicit store Red: uv run --no-sync flock
  /tmp/vertebra-flow-shm-tests.lock pytest
  src/py/tests/unit/extensions/test_data_storage_integrations.py -q
  -k 'bigquery_export and relative' exits 1 (2 failed, 81 deselected).
  Fresh native: CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"
  PYO3_PYTHON="$PWD/.venv/bin/python3.12" UV_NO_SYNC=1 make dev exits 0,
  rebuilding/installing all five CPython 3.12 extensions after OCI checkpoint
  2e343bc3. The native build source checkpoint is bdc7799a.
  Compatibility Red: uv run --no-sync flock /tmp/vertebra-flow-shm-tests.lock
  pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  src/py/tests/unit/extensions/test_data_storage_integrations.py -q
  -k 'native_memory_namespace or native_file_authority or sqlspec_memory_authority'
  exits 1 (4 failed, 1 passed, 139 deselected) against the initial qualification
  guard. The identical focused command exits 0 (5 passed, 139 deselected) after
  limiting authority checks to remote protocols or declared backend store_uri.
  Both native variants preserve Memory logical namespaces and file authority
  path mapping; SQLSpec's declared Memory authority still rejects mismatches.
  Native Rust source is unchanged after the all-five-extension checkpoint.
  Post-build Green: uv run --no-sync flock /tmp/vertebra-flow-shm-tests.lock
  pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  src/py/tests/unit/extensions/test_data_storage_integrations.py -q
  exits 0 (139 passed, 5 existing optional-driver skips, 12 existing
  Advanced Alchemy/SQLAlchemy deprecation warnings, 13.64s).
  uv run --no-sync ruff check src/py/vertebra/db/_staging.py
  src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  src/py/tests/unit/extensions/test_data_storage_integrations.py exits 0.
  The same file set with uv run --no-sync ruff format --check exits 0.
  uv run --no-sync mypy src/py/vertebra/db/_staging.py exits 0 (1 file).
  git diff --check exits 0. Logs: /tmp/vertebra-ch07-t03-red.log,
  /tmp/vertebra-ch07-t03-qualification-red.log,
  /tmp/vertebra-ch07-t03-store-uri-red.log, /tmp/vertebra-ch07-t03-green.log,
  /tmp/vertebra-ch07-t03-dev.log, /tmp/vertebra-ch07-t03-mypy.log,
  /tmp/vertebra-ch07-t03-compat-red.log, /tmp/vertebra-ch07-t03-compat-green.log.
  uv run --no-sync flock /tmp/vertebra-flow-shm-tests.lock python -m
  mypy.stubtest vertebra._storage exits 0 (1 module); log:
  /tmp/vertebra-ch07-t03-stubtest.log. All processes complete before handoff.
  No platform limitation or hosted action applies.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: 38757809
sources:
- id: implementation
  resource: src/py/vertebra/db/_staging.py
  title: Owned implementation seam
tags:
- storage
- repair
---

# Reject removed R2 URIs before staging local-path resolution

## Objective & Scope Boundary

Reject removed R2 URIs before staging local-path resolution. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — configured qualification or public export/remote-endpoint contract; read-only collaborator.
- `src/py/vertebra/db/adapters/bigquery/driver.py` — configured qualification or public export/remote-endpoint contract; read-only collaborator.
- `src/py/vertebra/db/adapters/bigquery/config.py` — configured qualification or public export/remote-endpoint contract; read-only collaborator.

- `src/py/vertebra/db/_staging.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owned implementation or canonical regression surface.

## Symbol & Signature Contracts

`ObjectStoreStaging.validate_staging_target`, `_parse_implicit_uri`, `_resolve_backend_path` and `_resolve_store_key` reject URL scheme r2 with a clear ValueError identifying unsupported storage scheme. Remove r2 from `_REMOTE_SCHEMES` only together with explicit unknown/removed-URI rejection so `r2://bucket/key` never becomes a local filename or directory. Preserve plain paths, file:// and supported remote schemes, and explicit passed-store compatibility checks. No R2 alias or special endpoint normalization.

For an explicitly configured store/backend, validate every fully qualified URI against its actual `store_uri` (SQLSpec backend) or `url` (native store) before stripping qualification. Normalize only the existing protocol aliases (`gcs`→`gs`, `azure`→`az`) for comparison and require matching remote authority/bucket. Never turn a mismatched scheme or bucket into a relative path by discarding its prefix. Preserve the matching fully qualified backend path so SQLSpec performs its own raw-key/root-containment validation. `validate_staging_target` performs the same qualification rejection before upstream query or producer consumption. No metadata request or object mutation is needed for this validation; local/file and supported matching remote behavior remain accepted.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Implicit rejected URI | resolve_sync_store/resolve_async_store(None,"r2://bucket/data.parquet") | ValueError; no r2: local path created |
| Removed explicit store URI | BigQuery sync/async export with store="r2://unit" and relative destination "relative.ipc" | Unsupported-scheme ValueError before any HTTP connection/query. |
| Explicit store rejected URI | Memory store plus r2://bucket/key passed through staging public export/read path | ValueError before any write; store unchanged |
| Native nonremote qualification | Sync/Async native Memory store with memory://namespace/key; native Local store with file://localhost/key | Existing namespace/key and key resolution remains accepted; real read/write round-trips preserve exact keys and bytes. SQLSpec declared Memory authorities continue rejecting foreign qualification. |
| Supported staging | file path and s3://bucket/key with configured store | Existing path resolution unchanged |
| Mismatched explicit remote qualification | SQLSpec backend/native store configured for s3://unit, supplied s3://other/key or gs://unit/key through public staging validation/read/write sync and async | ValueError before upstream query/producer consumption or object mutation; original configured-store objects remain exact bytes. A matching s3://unit/key still round-trips. |

Use `ObjectStoreStaging.write_cursor_sync` / `write_cursor_async` and `read_cursor_sync` / `read_cursor_async` with a real ArrowCursor and `SQLSpecObjectStoreBackend("gs://bucket/root", store=SyncObjectStore("memory://"), base_path="scope")`. Exercise matching `gs://` and `gcs://` URIs for `root/literal%2F` and `root/literal%252F`. Assert actual underlying native keys are exactly `scope/literal%2F` and `scope/literal%252F`, their bytes and decoded rows remain distinct, and staging reads round-trip both. No `scope/root/...` alias is created. For mismatched scheme/bucket, assert original bytes and exact listed keys remain unchanged.

The upstream public callers are `SyncBigQueryDriver.export_to_object_store` and `AsyncBigQueryDriver.export_to_object_store`. Construct real drivers with `BigQueryConfig(project_id="unit", rest_endpoint=local_http_endpoint, storage_endpoint=local_http_endpoint, access_token="test-only-token", use_shm_cache=False)`. The real local HTTP server counts requests/connections and returns a bounded failure for an unexpected request. Export to removed or mismatched qualified destinations with the configured real backend: the exact validation ValueError must occur and the server count remains zero. This is the pre-query consumption tripwire; do not patch select_to_arrow, execute, staging methods, SQLSpec, or native clients. Valid row/cursor staging uses a real local ArrowCursor; it does not need a cloud query. External responder/caller-owned data are the only substitutes.

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/vertebra/db/_staging.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_data_storage_integrations.py`
- `uv run --no-sync mypy src/py/vertebra/db/_staging.py`
- `uv run --no-sync pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

Staging validates removed schemes and explicit remote qualification before path resolution, query execution or object effects. Matching rooted aliases, literal percent keys, native Memory namespaces and native local file authorities retain their verified mappings. Resolved logical backend URI handling is covered by completed t04. Signed source and verification fields are complete.

[^implementation]: `src/py/vertebra/db/_staging.py` — Current repository source for this task.
