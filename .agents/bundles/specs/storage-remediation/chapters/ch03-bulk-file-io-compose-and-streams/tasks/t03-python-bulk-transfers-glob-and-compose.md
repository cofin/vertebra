---
type: Task
id: "storage-remediation:ch03:t03"
chapter_id: ch03-bulk-file-io-compose-and-streams
title: Implement Python get_file, download_many, upload_many, staged_download, conditional copy, glob, compose, and S3/R2 config options
description: Add get_file, download_many, upload_many, staged_download, copy(..., overwrite=True), glob, and compose on SyncObjectStore and AsyncObjectStore, and wire S3Config.virtual_hosted_style and R2Config.custom_domain in _store.py.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch03:t02"
read_set:
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/storage/test_storage_store.py
  - src/py/tests/unit/storage/test_signing.py
files:
  - src/py/vertebra/storage/_store.py
tests:
  - src/py/tests/unit/storage/test_storage_store.py
  - src/py/tests/unit/storage/test_signing.py
estimated_loc: 235
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
claimed_by: executor
claimed_at: "2026-11-09T00:36:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T00:45:00Z"
last_verified_commit: 70c23f63
verification_evidence: "pytest src/py/tests/unit/storage (235 passed); ruff check/format, mypy, review-check clean"
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T00:45:00Z"
commit: 70c23f63
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: executor
    at: "2026-11-09T00:45:00Z"
    commit: 70c23f63
sources:
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: Python SyncObjectStore, AsyncObjectStore, S3Config, and R2Config
tags:
  - storage
  - bulk-io
  - glob
  - compose
  - tdd
---

# Task t03: Python Bulk Transfers (`get_file`, `download_many`, `upload_many`, `staged_download`), `copy`, `glob`, `compose`, and S3/R2 Config Options

## Objective & Scope Boundary

Update `src/py/vertebra/storage/_store.py` [^py-store] (`1` production file):
1. Wire `S3Config.virtual_hosted_style` (`opts["virtual_hosted_style"] = "true"` + `SigningConfig.virtual_hosted_style: bool = False` passed to `py_sign_s3_v4_url` / `py_verify_s3_v4_url`) and `R2Config.custom_domain` (`opts["custom_domain"]` + `SigningConfig` custom-domain endpoint resolution when configured).
2. Add `get_file(self, path: str, file_path: str | Path) -> ObjectMeta`, `download_many(self, keys: Sequence[str], destination: str | Path, *, concurrency: int = 16) -> list[Path]`, `upload_many(self, paths: Sequence[str | Path], destination_prefix: str = "", *, concurrency: int = 16) -> list[PutResult]`, and `staged_download(self, keys: Sequence[str], destination: str | Path | None = None, *, concurrency: int = 16)` (`@contextlib.contextmanager` on `SyncObjectStore`, `@contextlib.asynccontextmanager` on `AsyncObjectStore`; defaults staging base to `/dev/shm` when writable directory exists else `tempfile.gettempdir()`, stages into a unique temp directory when `destination is None` or tracks exact staged paths when `destination` is given, and removes staged files/temp dir in `finally` without using `asyncio.to_thread`).
3. Update `copy(self, src: str, dst: str, *, overwrite: bool = True) -> None` (`if_not_exists=not overwrite`), add `glob(self, pattern: str) -> list[ObjectMeta]` (strips matching store URI prefix if passed, uses `head` fast path when pattern contains no `[*?[]` wildcards, otherwise extracts longest literal prefix before `[*?[]`, calls `list(prefix)`, filters with `fnmatch.fnmatchcase`, and skips trailing-`/` directory placeholders), and add `compose(self, sources: Sequence[str], dst: str, *, content_type: str | None = None) -> PutResult` delegating directly to the fail-closed native store.

Out of scope: `SyncReadableFile` read-ahead and `open_writer` (`t04`).

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — `S3Config`, `R2Config`, `SigningConfig`, `SyncObjectStore`, `AsyncObjectStore` [^py-store].
- `src/py/tests/unit/storage/test_storage_store.py` — Bulk transfer, `copy`, `glob`, and `compose` tests.
- `src/py/tests/unit/storage/test_signing.py` — `S3Config(virtual_hosted_style=True)` and `R2Config(custom_domain=...)` signing tests.

## Symbol & Signature Contracts

- `get_file(self, path: str, file_path: str | Path) -> ObjectMeta`
- `download_many(self, keys: Sequence[str], destination: str | Path, *, concurrency: int = 16) -> list[Path]`
- `upload_many(self, paths: Sequence[str | Path], destination_prefix: str = "", *, concurrency: int = 16) -> list[PutResult]`
- `staged_download(self, keys: Sequence[str], destination: str | Path | None = None, *, concurrency: int = 16)`
- `copy(self, src: str, dst: str, *, overwrite: bool = True) -> None`
- `glob(self, pattern: str) -> list[ObjectMeta]`
- `compose(self, sources: Sequence[str], dst: str, *, content_type: str | None = None) -> PutResult`

## Edge Cases & Error Semantics

- Duplicate destination basenames in `download_many` / `staged_download` or duplicate object keys in `upload_many` raise `ValueError` before starting transfers.
- Empty `keys` / `paths` returns `[]` immediately; empty `sources` in `compose` raises `ValueError`.
- Zero `asyncio.to_thread` calls may be added to `src/py/vertebra/storage/_store.py` (`test_storage_and_alchemy_zero_to_thread` invariant).

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/storage/test_storage_store.py` and `src/py/tests/unit/storage/test_signing.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Sync & async `upload_many`, `download_many`, `staged_download` | `5` local files -> `upload_many` -> `staged_download` (both normal exit and raised `RuntimeError`) | Files upload and stage accurately; staged files are removed from disk on both normal exit and raised exception. |
| `copy(..., overwrite=False)` | `store.put("a", b"1"); store.put("b", b"2"); store.copy("a", "b", overwrite=False)` | Raises `FileExistsError`. |
| `glob` wildcard & literal fast path | Keys `runs/2026/part-00.parquet`, `runs/2026/part-01.parquet`, `runs/2026/notes.txt`, `runs/2026/` | `store.glob("runs/2026/*.parquet")` returns the 2 Parquet `ObjectMeta` items; `store.glob("runs/2026/notes.txt")` returns `[meta]`; missing literal returns `[]`. |
| `compose` sync & async | `store.compose(["p1", "p2", "p3"], "merged.bin", content_type="application/octet-stream")` | Returns `PutResult` and `store.get("merged.bin")` equals concatenated bytes. |

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/storage/_store.py src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/storage/test_signing.py
uv run --no-sync mypy src/py/vertebra/storage/_store.py
uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/storage/test_signing.py -q
```

## Acceptance Criteria

- [x] [70c23f63] `get_file`, `download_many`, `upload_many`, `staged_download`, `copy(..., overwrite=...)`, `glob`, and `compose` work across sync and async stores with zero `asyncio.to_thread` usage (R3.3).
- [x] [70c23f63] `S3Config(virtual_hosted_style=True)` and `R2Config(custom_domain=...)` forward options and presign URLs accurately (R3.3).

## Notes & Discoveries

- [2026-11-08 20:55] Use direct `Path.unlink(missing_ok=True)` / `shutil.rmtree(..., ignore_errors=True)` for staging cleanup on tmpfs/local directories so async `staged_download` remains non-`to_thread`.
- [2026-11-09 00:45] `R2Config.custom_domain` is forwarded as a store option while presigned URLs stay on the R2 S3 API domain, because Cloudflare honors presigned URLs only on that domain; recorded in [`object-storage-limitations.md`](../../../../../knowledge/domains/storage/object-storage-limitations.md). `glob()` truncates the list prefix to the last `/` because native `list(prefix)` matches whole path segments.

[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Python `SyncObjectStore`, `AsyncObjectStore`, `S3Config`, and `R2Config`.
