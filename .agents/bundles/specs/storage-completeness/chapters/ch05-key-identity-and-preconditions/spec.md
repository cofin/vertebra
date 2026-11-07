---
type: Spec
parent_flow: storage-completeness
chapter_id: ch05-key-identity-and-preconditions
title: Raw Object Identity and Typed Conditional Conflicts
description: Raw Object Identity and Typed Conditional Conflicts
status: stable
state: completed
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 4
plan_commit: 5fe477fa4f6e08bab9278dba3906e7c556ceb3a4
state_revision: 0
last_verified_checkpoint: 8d073a38
last_reviewed_commit: 0bce1806
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T22:27:00Z'
generated:
  by: flow/0.28.0
  at: '2026-11-07T12:00:00Z'
verified:
- correctness_review: 0bce1806
- quality_review: 0bce1806
sources:
- id: source
  resource: src/rs/vertebra-storage/src/store.rs
  title: Grounded implementation seam
tags:
- storage
- repair
---

# Raw Object Identity and Typed Conditional Conflicts

<!-- truth: start -->
Native, facade and adapter key operations preserve raw Unicode and percent-text identities and reject ambiguous relative paths. [^source]
`vertebra.storage.PreconditionError` lazily exposes the native `PermissionError` subclass for conditional conflicts; genuine permission failures remain distinct.
All three tasks are implemented and verified. Correctness review and fresh quality review pass at `0bce1806`; provider publication repair remains outside this chapter.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Raw Object Identity and Typed Conditional Conflicts addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

### R1: Preserve native raw key and prefix identity

`store.rs::validate_object_path` validates raw text before `StorePath::parse`: an object key is nonempty, has no leading/trailing `/`, no repeated `/`, no literal `.` segment and no outer whitespace. Preserve the existing `reject_traversal_segments` guard on slash/backslash separated literal `..`, `%2e%2e`, `.%2e`, `%2e.` (case insensitive). Reject backslash characters rather than interpreting them as separators. Unicode, literal `%`, `%2F` and `%252F` remain raw key text and are never URL-decoded. `Path::parse` itself strips a boundary slash, so the explicit grammar must precede it; never use `Path::from` on raw keys.

`validate_optional_prefix(None)`, `Some("")` and `Some("/")` return None (all keys), preserving existing root-prefix behavior. A relative prefix with exactly one trailing slash (e.g. `"logs/"`) is valid and strips that single suffix before `Path::parse`; a leading slash except the root sentinel, repeated separators, outer whitespace, backslash, dot or guarded parent segments reject with InvalidInput. Listing `logs/` remains valid for list, list_page and list_with_delimiter. No object-key trailing-slash normalization is allowed.

OCI listing builds returned paths with `StorePath::parse(obj.name)` after the same raw-object grammar and maps invalid foreign paths to a visible storage error. Metadata keys are SDK raw identities; never URL-unquote them. Foreign SDK/local files with valid raw names remain readable through their exact listed key. Preserve percent/Unicode identity across put/get/head/list/paging/delimiter/delete/copy/rename, including validating both operands before any mutation. The owned Rust canonical tests must name raw-key, prefix-policy and nonmutating-invalid-operation cases explicitly.

### R2: Reject silent key aliasing in facade and adapter qualification

`_BaseObjectStore._normalize_key` rejects outer whitespace, plain leading/trailing slash, repeated slash, backslash, dot segments and the existing literal/encoded parent-segment guard before passing raw keys to native code. Preserve Unicode and literal `%`, `%2F`, `%252F` without decoding. Prefix validation is distinct: empty string and `/` mean root; a relative `dir/` prefix is valid with one trailing slash removed for native lookup. No config-string whitespace behavior changes.

Alchemy `_qualify` preserves raw relative keys, validates before applying `_prefix`, and uses the same key/prefix policy in get/save versus list calls. SQLSpec `_strip_matching_uri_prefix` and `_qualify` first classify an explicit full URI or root-qualified local `Path`. Match canonical scheme and exact bucket/authority; for file:// or absolute local Path require containment in configured local root and derive its relative key before raw-key validation. A matching qualified file path is supported, not rejected merely because its original representation begins `/`. Reject mismatched scheme, bucket or outside-root local path before effects. Never strip whitespace from residual raw text, URL-decode key percent text, or collapse separators to manufacture a different key. Keep SQLSpec base_path semantics and glob operators; apply raw validation to literal operation keys and the static prefix of glob operations without treating wildcard text as object content.

Observable callers are Alchemy `save_object`/`save_object_async`, `get_content`/`get_content_async`, `list_objects`/`list_objects_async`; SQLSpec `write_bytes_sync`/`write_bytes_async`, `read_bytes_sync`/`read_bytes_async`, `list_objects_sync`/`list_objects_async`, copy/move sync+async; facade put/get/head/list/copy/rename. Validate both source and destination before a native mutation.

### R3: Export lazy typed PreconditionError for native CAS conflicts

Use PyO3 create_exception! to define `_storage.PreconditionError(PermissionError)`. Both `StorageError::Precondition` and nested `StorageError::ObjectStore(object_store::Error::Precondition)` map to this exact class in `lib.rs::to_py_err`. Register the exception in `_storage`; declare it in `_storage.pyi`. Expose `vertebra.storage.PreconditionError` through module-level lazy __getattr__ and __all__; add a `TYPE_CHECKING`-only `from vertebra._storage import PreconditionError as PreconditionError` so static consumers resolve the exact native class without eager runtime import; importing vertebra.storage while the optional native wheel is absent continues to succeed, and requesting the exception then uses the existing MissingDependencyError contract. Keep genuine IO PermissionDenied as built-in PermissionError, not the CAS subtype. Add owned `lib.rs` cfg(test) unit `test_precondition_error_mapping` after Python::initialize() inside Python::attach(...); construct BOTH StorageError::Precondition and nested object_store::Error::Precondition and assert exact native PreconditionError type, construct IO PermissionDenied and assert exact built-in PermissionError, and both AlreadyExists forms assert exact FileExistsError. Create-only conflicts remain FileExistsError. Consumers such as Litestar renewal may catch only this type.

Caller → seam → collaborator: public storage/framework calls → owned deep module boundary → native SDK, transport or upstream framework. Public signatures remain stable except the deliberate removal of first-class R2 and addition of typed PreconditionError.

| Failure or edge | Required invariant |
| --- | --- |
| Competing writers | The winning object remains intact; stale CAS fails with typed PreconditionError or create-only FileExistsError. |
| Provider/transport error | Preserve the provider failure and cleanup ownership; no silent fallback or full-object buffering. |
| Optional dependency absent | Imports remain lazy and configured construction fails explicitly. |

| Alternative | Decision |
| --- | --- |
| Deep owned boundary with exact provider semantics | Chosen: hides credentials, staging and concurrency while preserving configured SDK behavior. |
| Generic head-then-write, Python lock, copy-create publication or blanket exception conversion | Rejected: loses atomicity, update results, metadata or error identity. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[1/1 files documented]`. [Object-storage knowledge](../../../../knowledge/domains/storage/object-storage.md) records raw object identities, prefix rules, qualified adapter inputs and lazy typed conflicts. The final supported-provider and HTTP/Arrow consolidation remains assigned to ch09:t03 and ch04:t03; this chapter does not claim those later contracts are complete.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch05:t01](tasks/t01-native-raw-paths.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R2 | [ch05:t02](tasks/t02-public-key-validation.md) | regression_tdd | `src/py/tests/unit/storage/test_storage_store.py` |
| R3 | [ch05:t03](tasks/t03-typed-conflict.md) | regression_tdd | `src/py/tests/unit/storage/test_storage_store.py` |

## 5. Implementation Plan

- [x] [c90285cf] storage-completeness:ch05:t01 — Preserve native raw key and prefix identity (tasks/t01-native-raw-paths.md)
- [x] [8d073a38] storage-completeness:ch05:t02 — Reject silent key aliasing in facade and adapter qualification (tasks/t02-public-key-validation.md)
- [x] [682aaaff] storage-completeness:ch05:t03 — Export lazy typed PreconditionError for native CAS conflicts (tasks/t03-typed-conflict.md)

**Chapter Recovery Checkpoint:** all focused verification, correctness review and fresh quality review pass. The reviewed source and documentation checkpoint is `0bce1806`.

## 6. Current State & Remaining Plan Snapshot

- Plan revision 4 is independently approved; completed task receipts match signed source commits.
- All three tasks are implemented and verified: native paths `c90285cf`, typed errors `682aaaff`, public qualification `8d073a38`. Correctness and fresh quality reviews pass at `0bce1806`.
- Native engine tests pass all 23 cases; the rebuilt extension passes the mapping, optional-import and stub checks. The combined facade/adapter/Django checkpoint passes all 74 tests. Local and cloud atomic publication remains pending.
- Documentation: `[1/1 files documented]`; the shared knowledge contract matches the implemented raw-key and exception boundaries. Final provider, HTTP and Arrow documentation remains assigned to the consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/rs/vertebra-storage/src/store.rs` — Verified repository implementation seam.
