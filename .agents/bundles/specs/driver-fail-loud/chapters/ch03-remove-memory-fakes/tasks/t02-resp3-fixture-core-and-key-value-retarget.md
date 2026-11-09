---
type: Task
id: "driver-fail-loud:ch03:t02"
chapter_id: ch03-remove-memory-fakes
title: Loopback RESP3 fixture core and key-value driver tests over the wire
description: Add the threaded loopback RESP3 server fixture with its transport, reply encoder, teardown, handshake, key-value, TTL, SCAN, and UNLINK commands to the unit test tree, and move the test_redis_valkey_driver.py tests that need only those commands off memory:// while the driver emulation still exists.
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "driver-fail-loud:ch03:t01"
read_set:
  - src/py/vertebra/db/adapters/redis/core.py
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/db/test_spanner_queue_and_cdc.py
files:
  - src/py/tests/unit/conftest.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_driver.py
estimated_loc: 215
plan_revision: 2
plan_commit: fd571e3a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: resp3-parser
    resource: src/py/vertebra/db/adapters/redis/core.py::Resp3Parser
    title: RESP3 frame parser
  - id: redis-driver
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: Redis sync and async drivers and their wire commands
  - id: spanner-import
    resource: src/py/tests/unit/db/test_spanner_queue_and_cdc.py::FakeSpannerClient
    title: Precedent for importing a test helper through the tests.unit package path
tags:
  - redis
  - valkey
  - testing
  - fixtures
---

# Task t02: Loopback RESP3 Fixture Core and Key-Value Tests Over the Wire

## Objective & Scope Boundary

Create `src/py/tests/unit/conftest.py` with a threaded loopback RESP3 server (`Resp3FakeServer`) and a function-scoped `resp3_server` fixture that implements the connection handshake, key-value, TTL, `SCAN`, and `UNLINK` commands, then move the eight `test_redis_valkey_driver.py` tests listed in the retarget matrix from `memory://` to the fixture URL. Production code is unchanged; every retargeted test passes on `HEAD` before and after, exercising the real wire path of `SyncRedisDriver` and `AsyncRedisDriver`.[^redis-driver][^resp3-parser]

Out of scope: pub/sub, scripting, and their handlers (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `SPUBLISH`, `EVAL`, `EVALSHA`), the two `test_redis_valkey_driver.py` tests that call `publish` or `eval_script`, and the Advanced Alchemy, framework, and Starlette retargets (all owned by `driver-fail-loud:ch03:t03`); `test_redis_valkey_resp3.py`, `test_native_db_drivers.py`, conformance, and every production change (owned by `ch04-redis-valkey-memory-removal`).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser.parse_frame` — `parse_frame(buf, 0) -> (frame, next_offset)`; raises `ValueError` on an incomplete frame.[^resp3-parser]
- `src/py/vertebra/db/adapters/redis/driver.py` — wire commands emitted by the paths this task exercises: `HELLO` (handshake), `CLIENT TRACKING ON [PREFIX p]... BCAST`, `GET`, `SET k v [EX n|PX n]`, `DEL`, `UNLINK`, `PTTL`, `SCAN cursor [MATCH p] COUNT n`, `FLUSHDB`; `SyncRedisDriver.scan` parses `int(decode_part(reply[0]))`.[^redis-driver]
- `src/py/tests/unit/db/test_redis_valkey_driver.py` — the eight tests listed in the retarget matrix below.
- `src/py/tests/unit/db/test_spanner_queue_and_cdc.py` — the `from tests.unit.db.test_spanner import FakeSpannerClient` import, the precedent for the `tests.unit.<module>` import path.[^spanner-import]

## Symbol & Signature Contracts

`src/py/tests/unit/conftest.py` (new; no `from __future__ import annotations`; Google docstrings; no inline comments):

```python
class Resp3FakeServer:
    """Threaded loopback RESP3 server holding keys and expiries in process."""

    def __init__(self) -> None: ...
    @property
    def port(self) -> int: ...
    @property
    def redis_url(self) -> str: ...
    @property
    def valkey_url(self) -> str: ...
    @property
    def commands(self) -> list[list[bytes]]: ...
    def close(self) -> None: ...
    def _dispatch(self, conn: socket.socket, args: list[bytes]) -> bytes: ...


class _ErrorReply(str):
    """RESP3 simple error payload."""


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None: ...


class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    owner: Resp3FakeServer


def _encode(value: object) -> bytes: ...


@pytest.fixture
def resp3_server() -> Iterator[Resp3FakeServer]:
    """Yield a running loopback RESP3 server and stop it after the test."""
```

- Transport: `_Server` binds `("127.0.0.1", 0)` and declares the class attribute `owner: Resp3FakeServer`, which `Resp3FakeServer.__init__` assigns right after constructing the server; `serve_forever(poll_interval=0.05)` runs on a daemon `threading.Thread`, so `server.shutdown()` returns within one 50 ms poll. `_Handler.handle` reaches its owning server with `assert isinstance(self.server, _Server)` and then reads `self.server.owner`; the `assert` narrows the `socketserver.BaseServer` attribute type for mypy without a `type: ignore` (the repository forbids inline comments). A thread-based server serves both the blocking sync driver and the asyncio async driver from the same test.
- `redis_url == f"redis://127.0.0.1:{port}/0"`; `valkey_url == f"valkey://127.0.0.1:{port}/0"`.
- State under one `threading.Lock`: a `set[socket.socket]` of accepted client sockets (added when a handler starts, removed when it ends), `dict[bytes, bytes]` values, and `dict[bytes, float]` monotonic expiry deadlines. Expired keys are removed lazily on every access.
- `_Handler.handle` loop: `recv` into a buffer, parse every complete frame with `Resp3Parser.parse_frame`, keep a trailing partial frame for the next `recv`, and for each frame acquire the state lock, append the frame to `commands`, call `_dispatch`, and `sendall` the reply before releasing the lock. Every `sendall` runs while holding the state lock, so frames from concurrent handlers never interleave on one socket. An empty `recv` ends the loop.
- `_dispatch` runs with the state lock held, matches `args[0].upper()` with one `match` case per command, and returns encoded reply bytes.
- `commands` records each parsed frame as `list[bytes]` in arrival order.
- `close()` takes a snapshot of the accepted-socket set under the lock, then for each socket calls `sock.shutdown(socket.SHUT_RDWR)` (suppressing `OSError`) followed by `sock.close()`, then calls `server.shutdown()`, `server.server_close()`, and joins the serve thread. Shutting client sockets first unblocks handler threads parked in `recv`.
- Reply encoding (`_encode`): `None` → `_\r\n`; `_ErrorReply` → `-<msg>\r\n` (checked before `str`); `int` → `:<n>\r\n`; `str` → `+<s>\r\n`; `bytes` → `$<len>\r\n<bytes>\r\n`; `list` → `*<n>\r\n` + encoded items.

| Command | Reply |
| --- | --- |
| `HELLO ...` | `%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n` |
| `CLIENT ...` | `+OK` |
| `PING [msg]` | `+PONG` or bulk `msg` |
| `GET k` | bulk value or `_` |
| `SET k v [EX s] [PX ms]` | `+OK`; stores value and replaces or clears expiry |
| `DEL k...` / `UNLINK k...` | integer count of removed live keys |
| `PTTL k` | `-2` missing, `-1` no expiry, else remaining milliseconds (minimum `1`) |
| `SCAN c [MATCH p] [COUNT n]` | `*2` of bulk `b"0"` and an array of every live key matching `fnmatch.fnmatchcase` |
| `FLUSHDB` | `+OK`; clears values and expiries |
| anything else | `-ERR unknown command '<NAME>'` |

`driver-fail-loud:ch03:t03` adds the pub/sub and scripting cases to `_dispatch`, push-frame encoding, and the script and subscription state to this module.

## Edge Cases & Error Semantics

- Pipelined frames arriving in one `recv` are answered in order; a partial frame waits for the next `recv`.
- A client closing its socket ends its handler thread and removes its socket from the accepted set.
- `SET` without `EX` / `PX` clears an existing expiry; `PTTL` never reports `0` for a live key.
- `SCAN` returns every match in one step with cursor `0`, so driver loops terminate after one round trip.
- Store and driver tests share one server per test, so sync and async drivers in one test see the same keys.
- Unknown commands fail loudly so a new driver command surfaces as a test failure, never a silent `+OK`.
- `close()` returns even when a handler thread is blocked in `recv`, because client sockets shut down before `server.shutdown()`.

## Public-Seam Test Blueprint

- **Fixture file**: `src/py/tests/unit/conftest.py`. Dependency category: local-substitutable network boundary.
- **Fixture parameter typing**: every retargeted test declares `resp3_server: Resp3FakeServer` and the module adds `from typing import TYPE_CHECKING` plus `if TYPE_CHECKING: from tests.unit.conftest import Resp3FakeServer`. The `tests.unit.<module>` import path follows the existing `from tests.unit.db.test_spanner import FakeSpannerClient` precedent in `src/py/tests/unit/db/test_spanner_queue_and_cdc.py`.[^spanner-import] `test_redis_valkey_driver.py` keeps `from __future__ import annotations`, so the annotation stays unquoted: ruff `UP` (`UP037`) rejects quoted annotations in modules with deferred evaluation.
- **Retarget matrix** (`src/py/tests/unit/db/test_redis_valkey_driver.py`):

| Test (new name when renamed) | Concrete Change | Expected Output |
| --- | --- | --- |
| `test_valkey_subclasses_redis_and_handles_invalidation_push(resp3_server)` | `ValkeyConfig(url=resp3_server.valkey_url, enable_client_side_caching=True)` | `"user:1" in driver.cached_keys`; `handle_invalidation_push(["user:1","user:2"]) == 1` |
| `test_valkey_resp3_client_tracking_and_litestar_integration(resp3_server)` | `MemoryStore.from_valkey(resp3_server.valkey_url)` | `0 < await store.expires_in("k1") <= 30`; after `delete_all()`, `await store.get("k1") is None` |
| `test_key_value_backend_and_redis_pubsub` → `test_key_value_store_batch_and_namespace(resp3_server)` | `KeyValueStore.from_valkey(resp3_server.valkey_url)`; delete the direct `AsyncRedisDriver` pubsub half | `batch == {"a": b"1", "b": b"2", "c": b"3", "d": None}` by key; `await ns.get("user") == b"bob"` |
| `test_redis_driver_scan_unlink_and_px_in_memory_mode` → `test_redis_driver_scan_unlink_and_px(resp3_server)` | fixture URL | `await driver.scan(0, match="cache:*")` returns cursor `0` and sorted keys `["cache:a", "cache:b"]`; `await driver.unlink("cache:a", "cache:b") == 2` |
| `test_store_delete_all_removes_only_its_prefix` | add `resp3_server`; fixture URL | `await driver.get("other:keep") == b"x"` |
| `test_store_sends_sub_second_ttl_as_milliseconds` | add `resp3_server`; fixture URL | `0 < await driver.pttl("vertebra:short") <= 250` |
| `test_store_expires_in_returns_none_without_expiry` | add `resp3_server`; `driver` case uses fixture URL | `await reader.expires_in("forever") is None` |
| `test_redis_and_valkey_build_drivers_propagate_broker` | parametrize URLs become `"redis://127.0.0.1:6379/0"` and `"valkey://127.0.0.1:6379/0"` (no I/O occurs) | broker identity assertions unchanged |

## Verification

- Baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py -q` passes on `HEAD`.
- Coverage snapshot before edits: `uv run --no-sync coverage run --branch -m pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py -q && uv run --no-sync coverage json -o /tmp/ch03-t02-before.json --include="src/py/vertebra/db/adapters/redis/*"`; repeat after edits into `/tmp/ch03-t02-after.json`. Lines that lose coverage lie only inside `SyncRedisDriver._execute_memory_command` and `memory://` branches of the key-value, TTL, `SCAN`, `UNLINK`, `FLUSHDB`, and subscription methods.
- Vacuity probe: temporarily make both `redis_url` and `valkey_url` return port `1`, rerun the eight retargeted tests, confirm they fail with `ConnectionRefusedError` / `OSError` (the broker propagation case performs no I/O and stays green), then restore.
- Green + lint: rerun the baseline command, then `uv run --no-sync ruff check src/py/tests/unit/conftest.py src/py/tests/unit/db/test_redis_valkey_driver.py && uv run --no-sync ruff format --check src/py/tests/unit/conftest.py src/py/tests/unit/db/test_redis_valkey_driver.py && uv run --no-sync mypy src/py/tests/unit/conftest.py`.
- Residue gate: `rg -n 'url="memory://"|from_valkey\("memory://"\)|"memory://"' src/py/tests/unit/db/test_redis_valkey_driver.py` prints only the two `RedisConfig(url="memory://")` lines inside `test_sync_redis_driver_memory_store_operations` and `test_async_redis_driver_memory_store_operations`, which `driver-fail-loud:ch03:t03` retargets.

## Acceptance Criteria

- [ ] `resp3_server` serves sync and async drivers over a real loopback socket (R7a).
- [ ] The eight retargeted tests construct no `memory://` Redis or Valkey config (R7a).
- [ ] Coverage diff loses only memory-emulation lines; vacuity probe goes red.
- [ ] Zero inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-07 00:00] Wire pubsub with broker forwarding stays covered by `src/py/tests/unit/db/test_redis_valkey_resp3.py::test_multiplexed_resp3_pubsub_and_broker_forwarding`; the async subscription iterator blocks on a live stream, so the deleted direct-pubsub half of `test_key_value_backend_and_redis_pubsub` has no wire equivalent in this test.
- [2026-11-08 00:00] `estimated_loc` is `215`: fixture core about `180` lines including Google docstrings for the server, handler, encoder, and ten command cases; retargets about `35`.

[^resp3-parser]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — RESP3 frame parser.
[^redis-driver]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Redis sync and async drivers and their wire commands.
[^spanner-import]: `src/py/tests/unit/db/test_spanner_queue_and_cdc.py::FakeSpannerClient` — Precedent for importing a test helper through the tests.unit package path.
