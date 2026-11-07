---
type: Task
id: storage-completeness:ch06:t01
chapter_id: ch06-adapter-correctness
title: Retain max_length across real Django create collisions
description: Retain max_length across real Django create collisions
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
- src/py/vertebra/extensions/django/storage.py
- src/py/tests/unit/extensions/test_django_plugin.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
files:
- src/py/vertebra/extensions/django/storage.py
tests:
- src/py/tests/unit/extensions/test_django_plugin.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 100
plan_revision: 4
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
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: null
sources:
- id: implementation
  resource: src/py/vertebra/extensions/django/storage.py
  title: Owned implementation seam
tags:
- storage
- repair
---

# Retain max_length across real Django create collisions

## Objective & Scope Boundary

Retain max_length across real Django create collisions. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/storage.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_django_plugin.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owned implementation or canonical regression surface.

## Symbol & Signature Contracts

`DjangoStorage._save(name, content)` performs one create-only native put_stream attempt and propagates FileExistsError. `DjangoStorage.save(name, content, max_length)` owns the collision retry loop around `super().save(..., max_length=max_length)` so each retry retains Django validation, length truncation and available-name selection. Rewind seekable file contents before sync or async retries so an upload consumed before conflict does not publish empty bytes. If a consumed input cannot be rewound, propagate the original FileExistsError without publishing a different object; do not buffer the complete input or silently retry an exhausted stream. No instance mutable per-call max_length or process lock. `asave` retains equivalent length and rewind behavior. file_overwrite=True preserves overwrite behavior. Work against current committed Django API, not a fabricated alternative storage hierarchy.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use a real native store and a seekable Django `File` subclass whose `chunks()` yields from the current file position without implicitly rewinding. A controlled competing native writer consumes the first upload body before creating the winning object and reporting the create-only collision. This exercises replay after consumption, rather than relying on `ContentFile.chunks()` automatically seeking to zero.

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Concurrent native collision | Inject competing real store create after exists check, before put_stream completion; save("longname.txt", ContentFile(b"payload"), max_length=16) | Racer payload preserved; returned alternative <=16 chars; stored bytes exactly payload |
| Impossible length | Force collision when suffix and extension cannot fit max_length=5 | Django SuspiciousFileOperation propagates; no object overwritten |
| Independent invocations | Interleave saves with max_length=16 and 24 on same instance | Each respects own max_length; no shared mutable retry state |
| Sync consumed-source retry | `save("12345678.txt", CurrentPositionFile(b"new data"), max_length=16)`; first consumed upload loses to native `put(..., b"racer")` | Returned alternative length is <= 16; original key remains b"racer" and alternative contains b"new data" in full. |
| Async consumed-source retry | `await asave("12345678.txt", CurrentPositionFile(b"async data"), max_length=16)` with a consumed native create-only collision | Exact original racer preserved, bounded alternative name, and full async payload retained. |
| Async impossible maximum | `asave("abcdefgh.txt", CurrentPositionFile(b"payload"), max_length=12)` after the first consumed upload loses to the racer (the seven-character Django suffix plus extension cannot fit) | Django `SuspiciousFileOperation`; racer remains unchanged and no truncated/empty replacement appears. |
| Same-instance interleaving | Two independent files saved through one `DjangoStorage` instance with different `max_length` values; bounded barriers/events interleave publication | Each name obeys its own operation's limit; both complete payloads survive, with no shared mutable per-call field. |
| Non-seekable consumed input | A file-like source cannot seek after the native collision consumes it | Original FileExistsError propagates; racer preserved and no empty alternative object is created. |

## Verification

Record the failing regression before implementation. Use this task's reserved target directory in each native command: `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"` and `PYO3_PYTHON="$PWD/.venv/bin/python3.12"`. Run `cargo fmt --manifest-path src/rs/Cargo.toml --all --check` and `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings` for native changes. Run `make dev` with those same environment values before Python native regressions and stubtest. Execute `uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`. For Python ownership run `uv run --no-sync ruff check src/py/vertebra/extensions/django/storage.py src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py` and `uv run --no-sync mypy src/py/vertebra/extensions/django/storage.py`; omit these two commands for Rust-only tasks. Record exact commands and exit status in verification_evidence.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/py/vertebra/extensions/django/storage.py` — Current repository source for this task.
