---
type: Task
id: "storage-completeness:ch03:t02"
chapter_id: ch03-litestar-and-range-streaming
title: Implement ObjectStoreLitestarStore and ObjectStoreFileSystemAdapter in extensions.litestar
description: Add an AsyncObjectStore-backed Litestar Store (ObjectStoreLitestarStore) with persistent TTL envelopes and an async static-files adapter (ObjectStoreFileSystemAdapter) in vertebra.extensions.litestar.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-completeness:ch03:t01"
read_set:
  - src/py/vertebra/extensions/litestar/store.py
  - src/py/vertebra/extensions/litestar/__init__.py
  - src/py/vertebra/storage/_streaming.py
  - src/py/tests/unit/extensions/test_litestar_plugin.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/utils/serializers/_msgpack.py
files:
  - src/py/vertebra/extensions/litestar/store.py
  - src/py/vertebra/extensions/litestar/__init__.py
tests:
  - src/py/tests/unit/extensions/test_litestar_plugin.py
estimated_loc: 250
plan_revision: 8
plan_commit: 222cbf99
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-08T16:07:37Z"
last_verified_commit: 99317bed609e62a73d8b0036d444c1268ac2eb1d
verification_evidence:
  red:
    command: 'UV_NO_SYNC=1 uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_plugin.py -k "object_store or static" -q'
    exit_code: 1
    result: "7 cases fail before adapter implementation"
  green:
    command: "UV_NO_SYNC=1 uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_plugin.py -q"
    exit_code: 0
    result: "21 passed against native Memory and Local storage and real Litestar consumers"
  regression:
    command: "UV_NO_SYNC=1 uv run --no-sync pytest src/py/tests/unit/extensions src/py/tests/unit/storage src/py/tests/unit/server/test_zero_copy_buffer.py -q --disable-warnings"
    exit_code: 0
    result: "634 passed"
  lint:
    command: "UV_NO_SYNC=1 uv run --no-sync ruff check src/py/vertebra/extensions/litestar src/py/tests/unit/extensions/test_litestar_plugin.py"
    exit_code: 0
  typing:
    command: "UV_NO_SYNC=1 uv run --no-sync mypy src/py/vertebra/extensions/litestar"
    exit_code: 0
  format:
    command: "UV_NO_SYNC=1 uv run --no-sync ruff format --check src/py/vertebra/extensions/litestar/store.py src/py/vertebra/extensions/litestar/__init__.py src/py/tests/unit/extensions/test_litestar_plugin.py"
    exit_code: 0
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-08T16:07:37Z"
commit: 99317bed609e62a73d8b0036d444c1268ac2eb1d
sources:
  - id: litestar-store-mod
    resource: src/py/vertebra/extensions/litestar/store.py::MemoryStore
    title: Litestar Store implementations in vertebra.extensions.litestar.store
  - id: litestar-pkg-init
    resource: src/py/vertebra/extensions/litestar/__init__.py
    title: Litestar extension package exports
  - id: range-streaming
    resource: src/py/vertebra/storage/_streaming.py::StreamingStorageResponse
    title: Non-blocking HTTP Range streaming response
tags:
  - storage
  - litestar
  - extensions
  - http
---

# Task t02: Implement `ObjectStoreLitestarStore` and `ObjectStoreFileSystemAdapter`

## Objective & Scope Boundary

Implement the two adapters in `extensions/litestar/store.py` and their lazy package exports in `extensions/litestar/__init__.py`. The canonical extension test file proves persistent Store semantics, static GET/HEAD, and a separate Range response handler. Change only these two production files and the canonical test file; estimated production change is at most 250 lines. Reuse public storage readers and serializers. Framework-specific response conversion belongs to the test handler; add no response conversion API to storage.

## Bounded Read Manifest

1. `src/py/vertebra/extensions/litestar/store.py` — `_StoreBase`, `MemoryStore`, and `ResponseCacheStore` patterns [^litestar-store-mod].
2. `src/py/vertebra/extensions/litestar/__init__.py` — TYPE_CHECKING imports, `__all__`, and `_EXPORT_MAP` [^litestar-pkg-init].
3. `src/py/vertebra/storage/_streaming.py` — completed t01 `from_async_store`, headers, status, and `aiter_chunks` [^range-streaming].
4. `src/py/vertebra/storage/_store.py` — `AsyncObjectStore`, `StorageConfig`, `AsyncReadableFile`, metadata, and pagination.
5. `src/py/tests/unit/extensions/test_litestar_plugin.py` — canonical public-seam test file.
6. `src/py/vertebra/utils/serializers/_msgpack.py` — exact `encode_msgpack_bytes(value)` and `decode_msgpack(raw, *, type=...)` contracts; import these through `vertebra.utils.serializers`.

Guidance outside the six-source manifest: `.agents/skills/python-framework/SKILL.md`, `.agents/bundles/knowledge/domains/extensions/litestar.md`, and the external Litestar plugin skill. Installed Litestar 2.25.0 evidence outside the repository-source budget: `litestar/stores/base.py`, `litestar/types/file_types.py`, `litestar/file_system.py`, `litestar/static_files/config.py`, `litestar/static_files/base.py`, and `litestar/response/file.py`. These define the exact Store renewal, virtual-path containment, and async reader consumption contracts.

## Symbol & Signature Contracts

`class ObjectStoreLitestarStore(_StoreBase)` in `store.py`:

- `__init__(self, store: AsyncObjectStore | StorageConfig | str, *, namespace: str | None = None) -> None`: reuse an existing async store or construct one from explicit configuration; missing/empty configuration raises `ImproperConfigurationError`.
- `with_namespace(self, namespace: str) -> ObjectStoreLitestarStore`: share the async store and append a child namespace. Namespace components are nonempty identifiers with optional hyphens; reject separators and parent segments. Use a private fixed `vertebra-store/` root, append namespace components plus `/`, and encode each logical key as one `~`-prefixed UTF-8 hex segment. This isolates arbitrary keys from child namespaces and confines root bulk deletion to adapter-owned objects.
- `async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None`: UTF-8 encode strings. Write every value as `b"VTBSTORE\x01" + encode_msgpack_bytes((expires_at_ms, payload))`, where `expires_at_ms` is an integer epoch millisecond deadline or `None`. Also write string attribute `vertebra-expires-at-ms` when expiring. The envelope is authoritative and persists even when Local storage strips attributes. Use a wall clock, not a process-local TTL table. Convert duration with `ceil(total_seconds * 1000)`; zero/negative duration produces an immediately expired entry.
- `async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None`: load and validate the envelope with `decode_msgpack(..., type=tuple[int | None, bytes])`; return original bytes, delete expired objects lazily, and return `None` when absent. Renew only a live entry that already has a deadline, rewriting both envelope and attributes. `renew_for` on an immortal entry is a no-op.
- `async def delete(self, key: str) -> None`: missing keys are a no-op.
- `async def delete_all(self) -> None`: paginate `list_page(prefix, cursor=..., limit=...)`, delete each page, and advance its returned cursor until `None`. Enforce the full trailing-slash namespace prefix before deleting. Parent bulk deletion includes child namespaces and preserves siblings, including names with a shared textual prefix.
- `async def exists(self, key: str) -> bool`: respect expiry and treat empty bytes as present.
- `async def expires_in(self, key: str) -> int | None`: return `max(0, int((deadline_ms - now_ms) / 1000))` for a live expiring entry; delete expired entries and return `None`; return `None` for missing or immortal entries.

`class ObjectStoreFileSystemAdapter` in `store.py`:

- `__init__(self, store: AsyncObjectStore | StorageConfig | str, *, base_path: str = "") -> None`: fail closed on missing/empty store configuration. Validate `base_path` as a relative prefix without parent segments.
- `async def info(self, path: str | os.PathLike[str], **kwargs: Any) -> dict[str, Any]`: interpret leading `/` as the virtual store root, remove `.` segments, reject any `..` segment before normalization with `FileNotFoundError`, and join beneath `base_path`. Use `head` for files and bounded `list_page(prefix_with_slash, limit=1)` for directory prefixes; the virtual root is a directory. Return `name`, `size`, `type` (`file` or `directory`), and floating-point epoch `mtime` parsed from `ObjectMeta.last_modified`; raise `FileNotFoundError` for absent files/prefixes. Include the remaining required `FileInfo` stat fields with neutral values if typing the mapping as Litestar `FileInfo`.
- `async def open(self, file: str | os.PathLike[str], mode: str = "rb", buffering: int = -1, **kwargs: Any) -> AsyncReadableFile`: use the same confined key resolution, require `mode == "rb"`, raise `ValueError` for unsupported modes, resolve size with `head`, and return `store.open_reader(key, size=meta.size)`. The existing reader has asynchronous `read` and context management with synchronous nonblocking `seek`/`tell`; do not invent async seek/tell methods.

Export both classes through TYPE_CHECKING imports, `__all__`, and `_EXPORT_MAP` in the Litestar package, plus module `__all__`.

## Edge Cases & Error Semantics

Only missing-object errors become cache misses or static not-found responses. Propagate backend/auth failures. A missing/wrong envelope magic, unsupported version, malformed MessagePack, or invalid typed envelope raises `ValueError`; never reinterpret corrupt data as a legacy raw value. Set without TTL replaces any old deadline with `None`. Positive fractional TTL rounds up to milliseconds; expiry occurs at `now_ms >= deadline_ms`.

Use `create_static_files_router(path="/assets", directories=["/"], file_system=adapter, resolve_symlinks=False)`. `fs=` is not an installed argument; `directories=["."]` is unsuitable for installed static containment. Static serving proves GET/HEAD separately from Range delivery. Test traversal directly against `info`/`open` because HTTP clients can normalize literal parent segments before sending.

## Public-Seam Test Blueprint

Canonical file: `src/py/tests/unit/extensions/test_litestar_plugin.py`. Use real `AsyncObjectStore(MemoryConfig())` and `LocalConfig(tmp_path)`, real Litestar `AsyncTestClient`, and a controlled wall clock without sleeps. Name new tests with `object_store` or `static` for the Red gate.

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Construction and exports | Empty string / `None` passed to both constructors; package imports of both classes | `ImproperConfigurationError`; package exports resolve to actual classes. |
| CRUD and missing keys | Strings, non-UTF-8 bytes, empty bytes, overwrites, delete twice | Exact payload bytes; empty bytes exist; absent get/expires_in return `None`; absent exists is false. |
| Persistent TTL | Local store set at controlled epoch with `timedelta(milliseconds=1500)`; reconstruct adapter and underlying async store from same LocalConfig | Before expiry payload is preserved and whole-second TTL is 1 then 0; at deadline get is `None`, exists false, and expires_in `None`. |
| Renewal and overwrite | Renew live expiring value; reconstruct; renew immortal value; overwrite expiring value without TTL; zero/negative TTL | Renewed deadline survives reconstruction; immortal/overwritten values have `expires_in is None`; immediate expiry returns `None`. |
| Metadata and corrupt envelope | Memory backend expiring write; inspect head attributes; replace raw object payload with wrong magic/version or invalid typed MessagePack | Exact string expiry attribute; each malformed envelope raises `ValueError` instead of returning bytes or a miss. |
| Namespace deletion | Root, `s1`, `s10`, and `s1.with_namespace("child")`; more than 1000 objects under `s1` | `s1.delete_all()` deletes all its pages and descendants; `s10` and unrelated raw storage objects remain; root deletion stays within `vertebra-store/`. |
| Static GET/HEAD and missing file | Put raw `app.js` beneath adapter base_path; mount virtual `/` with `file_system=` and resolution disabled | GET `/assets/app.js` returns exact bytes and size; HEAD returns matching Content-Length with no body; absent path returns 404; prefix info reports directory. |
| Reader and confinement | Open `/app.js`, read slices, seek/tell, exit context; info/open `/../secret` and nested parent segments; unsupported mode | Correct bytes/cursor and closed reader; traversal raises `FileNotFoundError`; unsupported mode raises `ValueError`; base_path sibling remains inaccessible. |
| Separate Range handler | Handler builds `await StreamingStorageResponse.from_async_store(store, "app.js", range_header=request.headers.get("range"))`; return Litestar `Stream` with an async bytes generator over `bytes(chunk)` from `aiter_chunks()`, copying response status, string headers, and media type | No Range yields 200 and full bytes; `bytes=0-7` yields 206 with 8 bytes and exact Content-Range/Length; `bytes=9999-` yields 416, empty body, and `Content-Range: bytes */<size>`. |

Use Litestar's supported `Stream` response in the test bridge; do not return `StreamingStorageResponse` directly to Litestar or imply its static-file response implements Range. The production full-stream StorageBuffer invariant remains owned by t01; converting chunks at this third-party response boundary is explicit.

## Verification

- Red: `uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_plugin.py -k "object_store or static" -q`.
- Green: `uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_plugin.py -q`.
- Quality: `uv run --no-sync ruff check src/py/vertebra/extensions/litestar src/py/tests/unit/extensions/test_litestar_plugin.py` and `uv run --no-sync mypy src/py/vertebra/extensions/litestar`.
- Run correctness review followed by a fresh quality review; synchronize task state only after all required evidence passes.

## Acceptance Criteria

- [x] [99317bed609e62a73d8b0036d444c1268ac2eb1d] The real Litestar Store contract, restart-safe Local TTL, corruption failures, namespace isolation, and paginated confined deletion pass the matrix (R3, R7).
- [x] [99317bed609e62a73d8b0036d444c1268ac2eb1d] The real static router serves GET/HEAD through the adapter, while the supported Litestar Stream bridge independently proves t01 200/206/416 responses (R7).
- [x] [99317bed609e62a73d8b0036d444c1268ac2eb1d] Both public classes resolve through the package lazy export machinery; focused lint, typing, and format gates pass. Independent correctness and fresh quality reviews remain chapter completion gates.

## Notes & Discoveries

Native object paths escape `~` as `%7E` in listed metadata and continuation cursors. Namespace deletion unquotes each returned object path and cursor before native reuse; this preserves key identity across all 1000-item pages. Direct object keys still use `~` plus UTF-8 hex. The shared `_resolve_object_store` helper keeps both adapters fail closed without changing existing MemoryStore defaults. Both classes are public through module/package exports. Source and native consumer evidence pass; independent chapter correctness and fresh quality reviews remain pending.

Installed Litestar 2.25.0 renews only existing expirations and consumes async readers with `async with await adapter.open(...)` plus `await file.read(...)`. Native Local writes retry without user attributes, so TTL persistence requires the envelope on every write. These are implementation contracts, not deferred discoveries.

[^litestar-store-mod]: `src/py/vertebra/extensions/litestar/store.py::MemoryStore` — Existing optional Litestar Store base and adapter conventions.
[^litestar-pkg-init]: `src/py/vertebra/extensions/litestar/__init__.py` — Lazy package exports.
[^range-streaming]: `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` — Completed t01 async Range response.
