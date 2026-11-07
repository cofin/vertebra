---
type: Task
id: storage-completeness:ch07:t04
chapter_id: ch07-remove-r2-python
title: Reject removed SQLSpec logical URI overrides before staging
description: Reject R2 logical URI overrides and declared backend URIs before object or export query effects.
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch07:t03
read_set:
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/vertebra/db/_staging.py
- src/py/vertebra/db/adapters/bigquery/driver.py
- src/py/vertebra/db/adapters/bigquery/config.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
files:
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/vertebra/db/_staging.py
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
estimated_loc: 100
plan_revision: 6
plan_commit: 43c2e3fe
state_revision: 0
claimed_by: agent:r2_logical_uri_repair
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: Reproduce the logical URI bypass and complete the independently approved repair.
last_verified_at: null
last_verified_commit: a7036538
created_at: '2026-11-07T23:24:00Z'
updated_at: '2026-11-07T23:24:00Z'
sources:
- id: sqlspec
  resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend.__init__
  title: Explicit-store logical URI override and public backend metadata
- id: staging
  resource: src/py/vertebra/db/_staging.py::ObjectStoreStaging._validate_uri_qualification
  title: Shared staging validation before path resolution
- id: export
  resource: src/py/vertebra/db/adapters/bigquery/driver.py::SyncBigQueryDriver.export_to_object_store
  title: Real BigQuery export validates staging before executing its query
- id: tests
  resource: src/py/tests/unit/extensions/test_data_storage_integrations.py::staging_http_tripwire
  title: Canonical native-object and real HTTP tripwire regression surfaces
tags:
- storage
- r2-withdrawal
- sqlspec
---

# Reject removed SQLSpec logical URI overrides before staging

## Objective & Scope Boundary

Complete first-class R2 removal at SQLSpec's logical URI boundary and staging's resolved declared backend URI boundary. Own exactly the two production modules and two canonical test modules above. Depend on completed ch07:t03 (`38757809`), not native publication repair. Revision 6 is independently approved at `43c2e3fe`. Execute the bounded repair in one isolated task and record its signed source commit and verification evidence. [^sqlspec] [^staging]

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — `ObjectStoreBackend.__init__`, `store_uri`, `protocol`, rooted qualification and public byte/Arrow operations.
- `src/py/vertebra/db/_staging.py` — `_validate_uri_qualification`, `validate_staging_target`, read/write cursor methods and native Memory/local resolution.
- `src/py/vertebra/db/adapters/bigquery/driver.py` — actual sync/async `export_to_object_store` validates before `select_to_arrow`; read-only collaborator.
- `src/py/vertebra/db/adapters/bigquery/config.py` — actual `BigQueryConfig` loopback endpoints/token/cache configuration; read-only collaborator.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — existing `staging_http_tripwire`, native backend cases and real BigQuery drivers.
- `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — canonical native Memory namespace and local file-authority compatibility cases.

## Symbol & Signature Contracts

Keep `ObjectStoreBackend.__init__(uri=None, *, store=None, base_path="", prefix="", options=None, signing=None, token_cache=None, **kwargs)` unchanged. A string `uri` that declares scheme `r2` is invalid even when an explicit supported `store` supplies the native engine. Raise `ImproperConfigurationError` (a `ValueError`) with an unsupported-scheme diagnostic identifying only scheme `r2` before exposing backend `store_uri`/`protocol` or performing native object work. The diagnostic must not echo a potentially credential-bearing URI. Apply the existing scheme normalization; do not introduce aliases, hostname restrictions or a new public helper. Rejection may live in the class's existing protocol normalization seam to cover both construction and qualified paths. Supported logical S3/GCS overrides over explicit native stores retain their metadata, configured root, raw keys and sync/async operations. [^sqlspec]

Keep `ObjectStoreStaging` signatures unchanged. In `_validate_uri_qualification(store, uri)`, resolve the existing declared URI from string `store`, backend `store_uri`, native `url`, or existing config `to_url_and_options()` as applicable, and reject a removed declared scheme before the relative-destination early return. Both owned rejection paths use `Unsupported storage URI scheme 'r2'`, without echoing the full URI, query or userinfo. Retain destination rejection and supported qualification checks. All public validation/read/write cursor routes inherit this refusal before native reads, writes, serialization or query execution. Native Memory namespace and local file-authority semantics remain unchanged. [^staging] [^export]

Caller → seam → collaborator:

| Caller | Owned seam | Collaborator and observable boundary |
| --- | --- | --- |
| SQLSpec backend construction with explicit Memory store | `ObjectStoreBackend.__init__` | Actual native store remains unchanged; no backend advertises R2 metadata. |
| Public staging validate/read/write with relative destination | `_validate_uri_qualification` | Declared backend URI is checked before key resolution and native effects. |
| Actual sync/async BigQuery export | `validate_staging_target` → `_validate_uri_qualification` | No connection or POST to the configured loopback query endpoint. |

Choose validation at the two existing deep boundaries. Relying only on the native store scheme misses logical metadata; deleting logical overrides breaks supported S3/GCS rooted use. A facade alias or new wrapper adds an unnecessary seam and retains removed provider semantics.

## Edge Cases & Error Semantics

| Input or condition | Required behavior |
| --- | --- |
| `r2://unit` or `r2://unit/root`, explicit sync/async Memory engine | Constructor raises the clear unsupported `r2` scheme error; native seed bytes and object list survive. |
| Supported backend whose writable public `store_uri` is set to `r2://unit`, `relative.ipc` destination | Staging rejects its actual declared configuration before the relative return, for validation and sync/async read/write. |
| Same declared configuration passed to real BigQuery export | Unsupported-scheme error occurs before query connection, query POST or object mutation. |
| Logical S3/GCS rooted override and supported alias | Keep `store_uri`, normalized `protocol`, root containment and literal percent keys; foreign scheme/bucket still fails. |
| Explicit generic S3 custom endpoint | Preserve native endpoint configuration and SigV4 signing; no provider hostname inspection. |
| Existing native Memory namespace and local file authority | Preserve the existing key bytes and path mapping; SQLSpec's declared Memory authority remains strict. |

Do not make a remote metadata request to validate a URI. Preserve unrelated provider/auth errors, optional dependencies, raw-key rules and native capabilities.

## Public-Seam Test Blueprint

Add the new negative cases to `test_data_storage_integrations.py`; reuse its real loopback fixture and actual native Memory objects. Retain and run native compatibility cases in `test_pg_dataplane_and_datafusion.py`. No private attribute mutation, fabricated exception, replacement staging/driver method, constructor mock or query mock is allowed. The public writable `backend.store_uri` field provides the independent staging configuration case after construction validation is fixed. [^tests]

| Case | Concrete setup and operation | Red/Green observable result |
| --- | --- | --- |
| Removed logical constructor override | Seed native Memory `keep.ipc` with bytes `b"keep"`; parameterize explicit `SyncObjectStore(MemoryConfig())` / `AsyncObjectStore(MemoryConfig())` and `uri` values `r2://unit`, `r2://unit/root`, `r2://test-user:test-only-secret@unit/root?token=test-only-token`; construct the actual SQLSpec backend. | Red constructs an operational backend advertising R2; Green raises exact `ImproperConfigurationError` and `str(exc.value) == "Unsupported storage URI scheme 'r2'"`; no URI/userinfo/query or test credential appears in the message. Native `get("keep.ipc") == b"keep"` and sole listed key `keep.ipc` remain exact. |
| Relative declared-backend bypass | Create actual `SQLSpecObjectStoreBackend("memory://unit", store=native)`; write Arrow rows `[{"id": 1}]` at `relative.ipc`; record native bytes, then set public `backend.store_uri` to each of `"r2://unit"` and `"r2://test-user:test-only-secret@unit/root?token=test-only-token"`. Parameterize public validate/read/write and sync/async cursor calls with relative `relative.ipc`, replacement rows `[{"id": 2}]`. | Red validates or reads/publishes through the R2 declaration; Green raises exact `ImproperConfigurationError` with `str(exc.value) == "Unsupported storage URI scheme 'r2'"`; no URI/userinfo/query/test credential is echoed. Verify retained raw bytes and exact native object list through `native`, independent of rejected backend metadata. |
| Zero export query | With the same actual declared-backend setup, configure `BigQueryConfig(project_id="unit", rest_endpoint=endpoint, storage_endpoint=endpoint, access_token="test-only-token", use_shm_cache=False)` from `staging_http_tripwire`; use real `SyncBigQueryDriver` / `AsyncBigQueryDriver` and call `export_to_object_store("SELECT 1 AS id", "relative.ipc", store=backend)`, closing each real driver in `finally`. | Red reaches the loopback query boundary (`/bigquery/v2/projects/unit/queries`) rather than scheme validation; Green raises the unsupported-scheme error and fixture `events == []`. Seed bytes/list remain unchanged. A later HTTP exception never counts as the expected rejection. |
| Supported logical metadata and round trip | Parameterize existing supported rooted cases over `s3://bucket/root` and `gs://bucket/root`, explicit actual native Memory engine and `base_path="scope"`; assert exact `store_uri` and `protocol` (`s3` / `gs`), use sync/async public operations for rows `[{"id": 7}]` at qualified `root/literal%2F` and relative `literal%252F`; exercise `gcs` alias for GS. | Native keys remain exactly `scope/literal%2F` and `scope/literal%252F`, with independent row round trips; existing foreign scheme/bucket and root-escape cases retain rejection. |
| Native supported controls | Run existing `test_framework_storage_accepts_s3_custom_endpoint`, configured S3 staging, rooted aliases, SQLSpec Memory authority, native Memory namespace and native local authority tests. | Existing generic S3 native SigV4 behavior and exact native Memory/local keys remain green. |

Capture failures from both constructor and independent relative declared-backend regressions before production changes. Constructor-only rejection must not hide the staging defect.

## Verification

Use the installed native extensions at source checkpoint `c7868b02`; this Python-only task requires no Rust rebuild unless the executor detects changed native source/artifacts. Record actual loaded extension/checkpoint evidence. Run each focused lane with `uv run --no-sync` and the shared SHM lock:

- Red and Green: `uv run --no-sync flock /tmp/vertebra-flow-shm-tests.lock pytest src/py/tests/unit/extensions/test_data_storage_integrations.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -q -k 'logical_uri or declared_backend or staging or framework_storage'`.
- Complete canonical lane: `uv run --no-sync flock /tmp/vertebra-flow-shm-tests.lock pytest src/py/tests/unit/extensions/test_data_storage_integrations.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -q`.
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/db/_staging.py src/py/tests/unit/extensions/test_data_storage_integrations.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`.
- `uv run --no-sync ruff format --check src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/db/_staging.py src/py/tests/unit/extensions/test_data_storage_integrations.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`.
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/db/_staging.py`.
- `uv run --no-sync python -m mypy.stubtest vertebra._storage` and `git diff --check`.

Name new tests with `logical_uri` / `declared_backend` so focused discovery includes them. Record exact failures, passes, extension evidence and any environment limit. Chapter closure additionally requires correctness review followed by fresh quality review; structural plan checks are not runtime evidence.

## Acceptance Criteria

- [x] Actual SQLSpec construction rejects removed logical URI overrides with explicit sync/async native Memory stores; seed bytes/list remain exact.
- [x] Independent actual declared-backend regressions prove relative validation/read/write refusal and zero real BigQuery query connections for sync/async export.
- [x] Supported rooted S3/GCS metadata, operations, aliases, generic custom S3 capabilities and native Memory/local compatibility pass.
- [x] Initial failing public regressions and focused/full canonical Green, lint, format, typing, stubtest and whitespace evidence are recorded.
- [x] Six-file read set, two production files and estimated 100 changed lines fit one isolated invocation and one signed local conventional commit.
- [x] ch09:t03 owns storage OKF correction; ch04:t03 owns final user documentation. The chapter stays open for correctness then fresh quality review.

## Notes & Discoveries

At committed checkpoint `49cbfcbb`, `SQLSpecObjectStoreBackend("r2://unit", store=SyncObjectStore(MemoryConfig()))` exposes protocol `r2` and permits qualified read/write; relative staging validation and cursor publication also succeed. At current source checkpoint `c7868b02`, the constructor's explicit-store logical override remains unchecked and staging checks removed schemes only in the destination or string store before returning on a relative destination. These are the adopted Important `PY-NEW` contract gap; the declared-backend test isolates the second boundary without mocking the system under test.

Execution preflight passes at `e178deca`: revision 6 / `43c2e3fe` matches the chapter, t03 is completed at `38757809`, all six manifest files and target symbols exist, and the task/checklist agree on `in_progress` / `[~]`. The coordinator retains Git and completion metadata ownership; the verified source change awaits its signed source commit.

The initial focused command in Verification exits 1 with **22 failed, 61 passed, 84 deselected** (`/tmp/vertebra-ch07-t04-red.log`). Six actual constructor cases and twelve independent declared-backend validate/read/write cases report `DID NOT RAISE ImproperConfigurationError`. Four actual sync/async BigQuery exports reach the loopback endpoint, recording exactly `['connection', '/bigquery/v2/projects/unit/queries']`, and raise native `BigQueryError: unexpected query`; these failures establish the query boundary bypass before production changes.

The unchanged focused command exits 0 with **83 passed, 84 deselected** (`/tmp/vertebra-ch07-t04-green.log`). The complete canonical command exits 0 with **162 passed, 5 skipped** (`/tmp/vertebra-ch07-t04-full.log`); the skips retain optional-driver requirements. All new negative cases assert exact scheme-only `ImproperConfigurationError`, unchanged native bytes and exact native keys; real export cases assert no connection/request events. Supported rooted S3/GS metadata, GCS alias, distinct literal percent keys, configured S3/custom-endpoint signing, SQLSpec Memory authority and native Memory/local controls pass.

All remaining exact Verification commands exit 0: Ruff check, Ruff format check (four files), Mypy (two source files), `python -m mypy.stubtest vertebra._storage` (one module) and `git diff --check`. Runtime lanes use `uv run --no-sync flock /tmp/vertebra-flow-shm-tests.lock`; all test processes release the lock on completion.

Native evidence: all five actual modules load from `$PWD/src/py/vertebra/<module>.cpython-312-x86_64-linux-gnu.so`: `_vertebra` (136805760 bytes), `_bundler` (105435520), `_storage` (177530416), `_bigquery` (216038856), `_spanner` (273171256). `git diff c7868b02 HEAD --stat -- src/rs` is empty at preflight, so this Python-only repair uses the existing rebuilt extensions without another native build. The patch changes only SQLSpec protocol rejection, the existing staging declared-URI guard and the canonical integration tests: **98 additions / 17 deletions** (115 changed lines; production **6 additions / 5 deletions**). The second canonical test module remains unchanged and passes. No API, signature, helper wrapper, mock system under test or provider hostname rule is added; unrelated concurrent edits are preserved. Correctness review and fresh quality review remain coordinator-owned before chapter closure.

[^sqlspec]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend.__init__` — Public explicit native-store override and logical metadata assignment.
[^staging]: `src/py/vertebra/db/_staging.py::ObjectStoreStaging._validate_uri_qualification` — Shared destination/store qualification checks and relative early return.
[^export]: `src/py/vertebra/db/adapters/bigquery/driver.py::SyncBigQueryDriver.export_to_object_store` and `AsyncBigQueryDriver.export_to_object_store` — Real query ordering before object publication.
[^tests]: `src/py/tests/unit/extensions/test_data_storage_integrations.py::staging_http_tripwire` and `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — Real HTTP connections, native object state and compatibility fixtures.
