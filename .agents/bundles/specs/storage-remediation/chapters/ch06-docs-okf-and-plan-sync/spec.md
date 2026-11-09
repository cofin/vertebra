---
type: Spec
flow_id: storage-remediation
parent_flow: storage-remediation
chapter_id: ch06-docs-okf-and-plan-sync
title: "Chapter 06: Zensical Storage Documentation and OKF Synchronization"
description: Update Zensical storage usage guides (storage.md, storage-auth-and-signing.md, storage-parquet-and-orm.md) and API reference (reference/storage.md) for PreconditionError, memory://<namespace>, shared token_cache=, bulk file transfers, open_reader read-ahead, open_writer, conditional copy, glob, server-side compose, Parquet dictionary/byte-stream-split options, and spawn multiprocessing guidance, and synchronize OKF product.md and object-storage.md.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch05-framework-adapters-and-test-hygiene
replan_trigger: null
current_task: "storage-remediation:ch06:t01"
plan_revision: 1
plan_commit: fd571e3a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-08T20:55:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
sources:
  - id: doc-usage-storage
    resource: docs/usage/storage.md
    title: Zensical Object Storage usage guide
  - id: doc-usage-auth
    resource: docs/usage/storage-auth-and-signing.md
    title: Zensical Storage Authentication and Signed URLs guide
  - id: doc-usage-parquet
    resource: docs/usage/storage-parquet-and-orm.md
    title: Zensical Range Reads, Parquet, and ORM Attachments guide
  - id: doc-ref-storage
    resource: docs/reference/storage.md
    title: Zensical vertebra.storage API reference
  - id: okf-product
    resource: .agents/bundles/product/product.md
    title: Vertebra Product Definition OKF chapter
  - id: okf-object-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: Object Storage, Streaming, and Presigning OKF domain chapter
tags:
  - storage
  - docs
  - zensical
  - okf
---

# Chapter 06: Zensical Storage Documentation and OKF Synchronization

<!-- truth: start -->
- **Zensical Usage Guides (`docs/usage/storage*.md`)**: Document `PreconditionError`, raw object key preservation, `get_file`, `download_many`, `upload_many`, `staged_download`, `open_reader(..., read_ahead=4_194_304)`, `open_writer` (`SyncWritableFile` / `AsyncWritableFile`), `copy(..., overwrite=False)`, `glob`, server-side GCS `compose`, backend config options (`virtual_hosted_style`, `custom_domain`, `headers`), shared cross-extension `token_cache=`, custom GCS `endpoint` presigning, explicit `memory://documents` namespace URIs, Parquet `pre_buffer=True` / `use_dictionary` / `use_byte_stream_split` / `options`, and `multiprocessing` `spawn` start-method guidance for native Tokio workers without leaking contributor `make *` commands [^doc-usage-storage] [^doc-usage-auth] [^doc-usage-parquet].
- **Storage API Reference & OKF Bundle Parity (`DOC-01`..`DOC-09`)**: `docs/reference/storage.md` exposes the complete `SyncObjectStore` / `AsyncObjectStore` member surface plus `SyncWritableFile`, `AsyncWritableFile`, `PreconditionError`, `PutResult`, `StreamingStorageResponse`, `ByteRange`, and `parse_byte_range`; `.agents/bundles/product/product.md` keeps its `<!-- truth: start -->` block `<= 40` flat bullet lines (zero `##` headings inside truth block) and lists `SyncGcpTokenCache` / `AsyncGcpTokenCache` / `GcpTokenSnapshot` under `vertebra._vertebra` rather than `vertebra._storage`; and `.agents/bundles/knowledge/domains/storage/object-storage.md` stays `<= 200` lines with accurate C-ABI capsule, native GCS, bulk I/O, and zero-copy codec contracts [^doc-ref-storage] [^okf-product] [^okf-object-storage].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Zero-context reviews (`CR-09`, `QUAL-009`..`QUAL-012`) plus the expanded storage API surface require full user-guide, API-reference, and OKF synchronization:
1. `docs/usage/storage.md`, `docs/usage/storage-auth-and-signing.md`, and `docs/usage/storage-parquet-and-orm.md` still contain stale `SyncObjectStore("memory://")` / `SyncObjectStore.from_memory(...)` calls, state `pre_buffer=False` instead of `pre_buffer=True`, and omit `PreconditionError`, `get_file`, `download_many`, `upload_many`, `staged_download`, `open_reader(..., read_ahead=...)`, `open_writer`, `copy(..., overwrite=False)`, `glob`, `compose`, Parquet `use_dictionary` / `use_byte_stream_split` / `options`, custom GCS presigning endpoints, and `multiprocessing` `spawn` worker guidance [^doc-usage-storage] [^doc-usage-auth] [^doc-usage-parquet].
2. `docs/reference/storage.md` restricts `SyncObjectStore` and `AsyncObjectStore` `members:` to 10 basic methods, hiding streaming, bulk file I/O, writable handles, `glob`, `compose`, Arrow/Parquet codecs, and `PreconditionError` from generated API docs [^doc-ref-storage].
3. `.agents/bundles/product/product.md` embeds `##` headings inside `<!-- truth: start -->` and attributes `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot` to `vertebra._storage` instead of `vertebra._vertebra`, while `.agents/bundles/knowledge/domains/storage/object-storage.md` (`208` lines) exceeds the `<= 200`-line domain chapter target and predates `GcpTokenCacheCapsule`, GCS conditional `version` / emulator / `compose` behavior, zero-copy `PyBackedBytes`, and bulk transfer contracts [^okf-product] [^okf-object-storage].

**Non-Goals**:
- Database driver fail-loud documentation changes (owned by `driver-fail-loud`).

### 1.2 Upstream Execution Baseline

- Depends on `ch05-framework-adapters-and-test-hygiene`; parent spec [`../../spec.md`](../../spec.md).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Zensical Usage Docs | `docs/usage/storage.md` [^doc-usage-storage], `docs/usage/storage-auth-and-signing.md` [^doc-usage-auth], `docs/usage/storage-parquet-and-orm.md` [^doc-usage-parquet] | Public user documentation rendered by `zensical build --clean` and validated by `scan_public_docstrings` + `verify_docs_artifacts`. |
| API Reference & OKF | `docs/reference/storage.md` [^doc-ref-storage], `.agents/bundles/product/product.md` [^okf-product], `.agents/bundles/knowledge/domains/storage/object-storage.md` [^okf-object-storage] | `mkdocstrings` reference directives and OKF v0.2 bundle chapters validated by `make docs` and `make review-check`. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [`workflow.md`](../../../../knowledge/workflow.md), [`review-docs-okf`](../../../../../skills/review-docs-okf/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R6.1 (`t01`)**: Update `docs/usage/storage.md`, `docs/usage/storage-auth-and-signing.md`, and `docs/usage/storage-parquet-and-orm.md` so every example uses current clean-break APIs (`MemoryConfig(namespace=...)` or `"memory://<namespace>"`), documents `PreconditionError`, `get_file`, `download_many`, `upload_many`, `staged_download`, `open_reader(..., read_ahead=4_194_304)`, `open_writer`, `copy(..., overwrite=False)`, `glob`, `compose`, `S3Config.virtual_hosted_style`, `R2Config.custom_domain`, `HTTPConfig.headers`, cross-extension `token_cache=` sharing, custom GCS `endpoint` presigning, `pre_buffer=True` Parquet defaults + `use_dictionary` / `use_byte_stream_split` / `options`, and `multiprocessing.get_context("spawn")` guidance for native Tokio-backed stores, with zero contributor `make *` commands.
- **R6.2 (`t02`)**: Expand `docs/reference/storage.md` `mkdocstrings` directives to cover the full public `vertebra.storage` API (`SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `AsyncReadableFile`, `SyncWritableFile`, `AsyncWritableFile`, `PutResult`, `PreconditionError`, `StreamingStorageResponse`, `ByteRange`, `parse_byte_range`), update `.agents/bundles/product/product.md` (`<= 40` flat bullet lines in `<!-- truth: start -->` with zero `##` headings inside the truth block and accurate `_storage` vs `_vertebra` class lists), and resynthesize `.agents/bundles/knowledge/domains/storage/object-storage.md` within `<= 200` total lines and `<= 40` truth-block lines.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- All `mkdocstrings` `::: vertebra.storage...` targets in `docs/reference/storage.md` resolve cleanly under `make docs` (including `--only-group doc` Griffe static inspection).
- `.agents/bundles/product/product.md` and `.agents/bundles/knowledge/domains/storage/object-storage.md` pass `uv run --no-sync python -m tools.scripts.review_setup validate` (`DOC-01`..`DOC-09`).

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `make docs` -> `scan_public_docstrings` + `generate_cli_docs --check` + `zensical build --clean` + `postbuild` + `verify_docs_artifacts` -> validates every docstring, `mkdocstrings` member list, and cross-link.
- `make review-check` -> `ReviewSetupValidator` -> validates OKF frontmatter, `<= 40`-line truth blocks, source path resolution, relative Markdown links, and privacy rules.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Public docs contributor command leak | Mentioning `make dev` / `make test` in `docs/` | Prohibited; public docs show end-user Python / `vtb` usage only | Developer command segregation. |
| OKF truth block budget & structure | `<!-- truth: start -->` in `product.md` or `object-storage.md` | `<= 40` lines, flat bullets only (zero `##` headings), `object-storage.md` `<= 200` total lines | `DOC-02` & `review_setup.py` compliance. |
| Privacy hygiene | `docs/**/*.md` and `.agents/bundles/**/*.md` | Zero workstation paths, internal app/engagement names, or live OAuth2 access token prefixes | `DOC-06` & `git-secrets` compliance. |

### 2.5 Acceptance Scenarios (Given / When / Then)

1. **Given** the updated Zensical storage usage and reference pages, **When** `make docs && make docs-clean` runs, **Then** docstring scanning, Griffe reference rendering, and artifact verification exit `0` with zero missing member warnings.
2. **Given** the updated `product.md` and `object-storage.md` OKF files, **When** `make review-check` runs, **Then** all bundle, link, truth-budget, and privacy checks exit `0` and `wc -l .agents/bundles/knowledge/domains/storage/object-storage.md` is `<= 200`.

### 2.6 Alternatives Considered

- **Chosen**: Explicit `members:` lists in `docs/reference/storage.md` covering every public method on `SyncObjectStore` and `AsyncObjectStore` in logical grouping order.
- **Rejected**: Leaving `members:` truncated at 10 methods (hides streaming, bulk file I/O, `open_writer`, `glob`, `compose`, and Arrow/Parquet methods from the published reference).

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/6 files documented]`
- `[ ]` `docs/usage/storage.md` (`t01`)
- `[ ]` `docs/usage/storage-auth-and-signing.md` (`t01`)
- `[ ]` `docs/usage/storage-parquet-and-orm.md` (`t01`)
- `[ ]` `docs/reference/storage.md` (`t02`)
- `[ ]` `.agents/bundles/product/product.md` (`t02`)
- `[ ]` `.agents/bundles/knowledge/domains/storage/object-storage.md` (`t02`)

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Gate |
| --- | --- | --- | --- |
| R6.1 | `ch06 / t01` | `documentation_validation` | `make docs && make docs-clean` |
| R6.2 | `ch06 / t02` | `documentation_validation` | `make docs && make docs-clean && make review-check` |

## 5. Implementation Plan

- [x] [ac8532d3] storage-remediation:ch06:t01 — Update Zensical storage usage guides (`storage.md`, `storage-auth-and-signing.md`, `storage-parquet-and-orm.md`) (tasks/t01-zensical-storage-usage-docs.md)
- [x] [5d2fc5d7] storage-remediation:ch06:t02 — Expand `docs/reference/storage.md` API coverage and synchronize `product.md` and `object-storage.md` OKF chapters (tasks/t02-storage-reference-and-okf-sync.md)
- **Chapter / Phase Recovery Checkpoint**: `make docs && make docs-clean && make review-check`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `fd571e3a` — `0/2` tasks completed in `ch06`.
- **Remaining Tasks in Chapter**: `t01`, `t02`.
- **Remaining Chapters**: `ch01`–`ch06`.

[^doc-usage-storage]: `docs/usage/storage.md` — Zensical Object Storage usage guide.
[^doc-usage-auth]: `docs/usage/storage-auth-and-signing.md` — Zensical Storage Authentication and Signed URLs guide.
[^doc-usage-parquet]: `docs/usage/storage-parquet-and-orm.md` — Zensical Range Reads, Parquet, and ORM Attachments guide.
[^doc-ref-storage]: `docs/reference/storage.md` — Zensical `vertebra.storage` API reference.
[^okf-product]: `.agents/bundles/product/product.md` — Vertebra Product Definition OKF chapter.
[^okf-object-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — Object Storage, Streaming, and Presigning OKF domain chapter.
