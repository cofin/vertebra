---
type: Task
id: storage-completeness:ch05:t02
chapter_id: ch05-key-identity-and-preconditions
title: Reject silent key aliasing in facade and adapter qualification
description: Reject silent key aliasing in facade and adapter qualification
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch05:t01
read_set:
- src/py/vertebra/storage/_store.py
- src/py/vertebra/extensions/alchemy/storage.py
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/tests/unit/storage/test_storage_store.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
files:
- src/py/vertebra/storage/_store.py
- src/py/vertebra/extensions/alchemy/storage.py
- src/py/vertebra/extensions/sqlspec/storage.py
tests:
- src/py/tests/unit/storage/test_storage_store.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 160
plan_revision: 4
plan_commit: 5fe477fa4f6e08bab9278dba3906e7c556ceb3a4
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: null
sources:
- id: implementation
  resource: src/py/vertebra/storage/_store.py
  title: Owned implementation seam
tags:
- storage
- repair
---

# Reject silent key aliasing in facade and adapter qualification

## Objective & Scope Boundary

Reject silent key aliasing in facade and adapter qualification. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/extensions/alchemy/storage.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/extensions/sqlspec/storage.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/storage/test_storage_store.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owned implementation or canonical regression surface.

## Symbol & Signature Contracts

`_BaseObjectStore._normalize_key` rejects outer whitespace, plain leading/trailing slash, repeated slash, backslash, dot segments and the existing literal/encoded parent-segment guard before passing raw keys to native code. Preserve Unicode and literal `%`, `%2F`, `%252F` without decoding. Prefix validation is distinct: empty string and `/` mean root; a relative `dir/` prefix is valid with one trailing slash removed for native lookup. No config-string whitespace behavior changes.

Alchemy `_qualify` preserves raw relative keys, validates before applying `_prefix`, and uses the same key/prefix policy in get/save versus list calls. SQLSpec `_strip_matching_uri_prefix` and `_qualify` first classify an explicit full URI or root-qualified local `Path`. Match canonical scheme and exact bucket/authority; for file:// or absolute local Path require containment in configured local root and derive its relative key before raw-key validation. A matching qualified file path is supported, not rejected merely because its original representation begins `/`. Reject mismatched scheme, bucket or outside-root local path before effects. Never strip whitespace from residual raw text, URL-decode key percent text, or collapse separators to manufacture a different key. Keep SQLSpec base_path semantics and glob operators; apply raw validation to literal operation keys and the static prefix of glob operations without treating wildcard text as object content.

Observable callers are Alchemy `save_object`/`save_object_async`, `get_content`/`get_content_async`, `list_objects`/`list_objects_async`; SQLSpec `write_bytes_sync`/`write_bytes_async`, `read_bytes_sync`/`read_bytes_async`, `list_objects_sync`/`list_objects_async`, copy/move sync+async; facade put/get/head/list/copy/rename. Validate both source and destination before a native mutation.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Whitespace collision | Put key then attempt put/get/head/copy/rename of " key " | ValueError; original key remains unchanged |
| Literal identity | Facade put/get/list; Alchemy save_object/get_content/list_objects (+async); SQLSpec write_bytes/read_bytes/list_objects sync+async for café%, literal%2F, literal%252F, dir/file | All accepted identities round-trip exactly through facade/Alchemy/SQLSpec |
| Prefix validation | list prefix=" x" or "/x"; empty, "/", "dir/" prefix | Ambiguous inputs reject; empty and / list all; dir/ retains valid prefix behavior |

| Local qualified path compatibility | SQLSpec configured LocalConfig(tmp_path); matching `Path(tmp_path)/"dir/literal%2F"` and `file://<tmp_path>/dir/literal%2F` | write/read/list retain relative raw identity and bytes; root qualification remains accepted. |
| Mismatched qualification | SQLSpec other scheme/bucket URI or absolute path outside configured root | ValueError and no object created, overwritten, copied or moved. |
| Invalid copy/move destination | Existing source b"source" and destination b"dest"; malformed whitespace/slash key in each operand position | Error before mutation; source and destination remain exact bytes through sync and async callers. |

## Verification

Record the failing regression before implementation. Use this task's reserved target directory in each native command: `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"` and `PYO3_PYTHON="$PWD/.venv/bin/python3.12"`. Run `cargo fmt --manifest-path src/rs/Cargo.toml --all --check` and `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings` for native changes. Run `make dev` with those same environment values before Python native regressions and stubtest. Execute `uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`. For Python ownership run `uv run --no-sync ruff check src/py/vertebra/storage/_store.py src/py/vertebra/extensions/alchemy/storage.py src/py/vertebra/extensions/sqlspec/storage.py src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py` and `uv run --no-sync mypy src/py/vertebra/storage/_store.py src/py/vertebra/extensions/alchemy/storage.py src/py/vertebra/extensions/sqlspec/storage.py`; omit these two commands for Rust-only tasks. Record exact commands and exit status in verification_evidence.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/py/vertebra/storage/_store.py` — Current repository source for this task.
