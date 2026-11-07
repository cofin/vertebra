---
type: Spec
parent_flow: storage-completeness
chapter_id: ch07-remove-r2-python
title: Withdraw First-Class R2 Python APIs
description: Remove R2 configuration, classifiers, logical URI overrides and staging declarations while preserving supported native stores.
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 6
plan_commit: 43c2e3fe
state_revision: 0
last_verified_checkpoint: a7036538
last_reviewed_commit: null
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T23:24:00Z'
generated:
  by: flow/0.28.0
  at: '2026-11-07T12:00:00Z'
verified: []
sources:
- id: sqlspec
  resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
  title: SQLSpec classifiers, logical URI metadata and native object operations
- id: facade
  resource: src/py/vertebra/storage/_store.py
  title: Supported storage configuration, facade URI construction and signing
- id: staging
  resource: src/py/vertebra/db/_staging.py::ObjectStoreStaging
  title: Staging target qualification before path resolution and export queries
- id: regression
  resource: src/py/tests/unit/extensions/test_data_storage_integrations.py
  title: Canonical framework, native object and real HTTP regressions
tags:
- storage
- r2-withdrawal
- python
---

# Withdraw First-Class R2 Python APIs

<!-- truth: start -->
- Framework classifiers and public R2 config/convenience exports are removed; direct facade `r2://` construction rejects the removed scheme. [^facade]
- Destination and explicit string-store staging rejection, qualification matching and native Memory/local compatibility are implemented at `38757809`. [^staging]
- SQLSpec rejects R2 logical URI overrides even with a supported explicit native store. Staging checks the resolved declared store URI before returning for relative destinations; both guards use a scheme-only diagnostic. [^sqlspec] [^staging]
- All four tasks are completed; 162 canonical tests pass with five existing optional-driver skips. Correctness re-review and fresh quality review remain open. [^regression]
<!-- truth: end -->

## 1. Specification, Context & Provenance

The first-class R2 Python surface is withdrawn while generic S3 custom endpoints and supported explicit native-store overrides remain operational. Production scope stays within each worksheet's ownership. The verified Python source checkpoint is `a7036538`; completed task artifacts remain authoritative at their recorded commits. SQLSpec logical metadata and staging declarations reject R2 before native object effects or export queries. Native withdrawal is completed and reviewed independently in ch08; native publication repair retains its own acceptance boundary. [^sqlspec] [^staging]

## 2. Requirements, Concrete Contracts & Architecture

### R1: Remove R2 from framework backend classifiers

Remove `R2Config` imports and `_STORAGE_CONFIGS` entries from Advanced Alchemy, SQLSpec and Starlette. Remove advertised R2 URI support. Preserve `S3Config` with arbitrary explicit custom endpoints and native SigV4 signing. No provider hostname blacklist, R2 alias or compatibility shim is introduced. Classifier withdrawal precedes public config removal to keep every task commit importable.

### R2: Delete public R2Config and convenience APIs

Remove `R2Config`, its uniform-chunk helper, `from_r2`, config union/validation entries, signing inference and public exports. Facade URI construction rejects `r2://` with `ImproperConfigurationError` (a `ValueError`) before native construction. Preserve supported provider exports, generic S3 endpoint signing and OCI framing coverage. Native helper/stub withdrawal remains with ch08. [^facade]

### R3: Reject removed staging destinations and mismatched qualification

`ObjectStoreStaging` rejects removed destinations and explicit string-store URLs before local-path resolution, native object effects or upstream query execution. Supported qualified URIs must match the actual declared backend `store_uri` or native `url`, normalizing only existing `gcs`→`gs` and `azure`→`az` aliases. Preserve matching SQLSpec qualification for its root containment and raw-key checks. Native Memory namespaces and local file-authority mapping retain their existing behavior; declared SQLSpec Memory authorities remain strict. [^staging]

### R4: Reject removed SQLSpec logical overrides and resolved declared store URIs

Keep `ObjectStoreBackend.__init__(uri=None, *, store=None, base_path="", prefix="", options=None, signing=None, token_cache=None, **kwargs)` unchanged. Supplying a supported native `store` cannot authorize an R2 logical string `uri` or expose an operational backend with `protocol == "r2"`. Reject it with an unsupported-scheme `ImproperConfigurationError` identifying only scheme `r2`; the diagnostic must not echo a potentially credential-bearing URI. Preserve supported logical S3/GCS overrides, exact `store_uri`/normalized `protocol`, configured root, literal keys and native capabilities. [^sqlspec]

Staging resolves the existing declared URI from string store, backend `store_uri`, native `url` or config `to_url_and_options()` before its relative-destination early return. Reject a removed declared scheme before validation/read/write effects and before real sync/async BigQuery exports query their configured endpoint. Validation performs no remote metadata request. The independent regression uses actual writable public backend configuration and native object state, never mocked staging methods or fabricated errors. [^staging] [^regression]

| Caller | Owned seam | Collaborator and observable contract |
| --- | --- | --- |
| Public framework/config construction | Existing adapter classifiers and facade constructor | Supported native stores and signing remain operational; removed config/URI refuses explicitly. |
| SQLSpec construction with explicit native store | `ObjectStoreBackend.__init__` | Logical metadata cannot retain R2 while its engine uses another provider. |
| Public staging validation/read/write | `_validate_uri_qualification` before resolution | Declared URI rejection precedes relative return and preserves exact native bytes/keys. |
| Actual BigQuery sync/async export | `validate_staging_target` | No query connection or POST is made for an invalid declared backend. |

| Edge or failure | Required behavior |
| --- | --- |
| Removed URI with explicit native Memory | Clear unsupported `r2` scheme error; seed bytes and object list survive. |
| Public backend `store_uri = "r2://unit"`, relative destination | Validation/read/write and export reject before effects, independently of constructor validation. |
| Rooted logical S3/GCS override | Exact supported metadata and sync/async round trips remain accepted; foreign scheme/bucket/root remains rejected. |
| Native Memory/local authority | Existing namespace/path mapping and exact keys remain accepted. |
| Explicit S3 custom endpoint | Preserve endpoint configuration and native SigV4 capabilities. |

| Alternative | Decision |
| --- | --- |
| Validate removed scheme at existing SQLSpec and staging boundaries | Chosen: covers logical metadata and relative staging with two owned modules and no new interface. |
| Inspect native scheme alone | Rejected: explicit native Memory does not establish the logical backend declaration. |
| Remove supported logical overrides or add an R2 compatibility wrapper | Rejected: breaks valid rooted callers or retains the removed provider surface. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[1/1 files documented]` for the verified Python withdrawal, logical override and staging declaration contracts in `.agents/bundles/knowledge/domains/storage/object-storage.md`. ch09:t03 retains final native guarantee consolidation and ch04:t03 owns final user guides. Canonical public regression tests document exact errors, supported metadata, native object retention and query ordering.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch07:t01](tasks/t01-framework-classifiers.md) | regression_tdd | Canonical framework classifier and S3 endpoint cases |
| R2 | [ch07:t02](tasks/t02-withdraw-config-and-exports.md) | regression_tdd | Canonical facade, signing and public export cases |
| R3 | [ch07:t03](tasks/t03-staging-reject-r2.md) | regression_tdd | Canonical staging, real HTTP qualification and native compatibility cases |
| R4; R3 declared-backend completion | [ch07:t04](tasks/t04-reject-removed-logical-uri.md) | regression_tdd | Actual SQLSpec metadata/native objects, independent relative staging and real BigQuery zero-query tripwire |

## 5. Implementation Plan

- [x] [4486f6fe] storage-completeness:ch07:t01 — Remove R2 from framework backend classifiers (tasks/t01-framework-classifiers.md)
- [x] [75f18e3b] storage-completeness:ch07:t02 — Delete public R2Config and from_r2 convenience APIs (tasks/t02-withdraw-config-and-exports.md)
- [x] [38757809] storage-completeness:ch07:t03 — Reject removed R2 URIs before staging local-path resolution (tasks/t03-staging-reject-r2.md)
- [x] [a7036538] storage-completeness:ch07:t04 — Reject removed SQLSpec logical URI overrides before staging (tasks/t04-reject-removed-logical-uri.md)

**Chapter Recovery Checkpoint:** all four task receipts are completed and scoped knowledge is synchronized. `/flow:review storage-completeness` routes the required correctness re-review and subsequent fresh quality review before chapter closure.

## 6. Current State & Remaining Plan Snapshot

- All four tasks are completed at `4486f6fe`, `75f18e3b`, `38757809` and `a7036538`. Canonical acceptance passes 162 tests with five existing optional-driver skips; focused Mypy, Ruff, storage stubtest and whitespace checks pass.
- t04 records 22 failing public regressions before its six-added/five-removed-line production repair. Its final regressions verify exact scheme-only errors, retained native bytes/keys, supported rooted S3/GCS metadata and real sync/async BigQuery exports with zero query connections.
- The Python repair uses the rebuilt native withdrawal extensions at `c7868b02`. The newer Memory publication source checkpoint `d50cf6a2` has Rust acceptance only; its Python rebuild/acceptance and other provider publication repairs remain open.
- The chapter remains active with `last_reviewed_commit: null`. Correctness re-review and fresh quality review remain required; native publication acceptance and other chapters retain their independent ownership.
- Scoped documentation is synchronized: `[1/1 files documented]`. Final native guarantee consolidation remains ch09:t03 and final user guides remain ch04:t03.

[^sqlspec]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — Classifiers, explicit native-store logical override, metadata and public operations.
[^facade]: `src/py/vertebra/storage/_store.py` — Supported configuration, facade scheme rejection and native signing.
[^staging]: `src/py/vertebra/db/_staging.py::ObjectStoreStaging` — Destination/store qualification and relative resolution behavior.
[^regression]: `src/py/tests/unit/extensions/test_data_storage_integrations.py` and `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — Canonical native object, HTTP query and Memory/local compatibility evidence.
