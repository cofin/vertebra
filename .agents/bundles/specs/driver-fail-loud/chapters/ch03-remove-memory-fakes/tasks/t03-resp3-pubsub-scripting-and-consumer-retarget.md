---
type: Task
id: "driver-fail-loud:ch03:t03"
chapter_id: ch03-remove-memory-fakes
title: Loopback RESP3 pub/sub and scripting surface and consumer tests over the wire
description: Extend the loopback RESP3 fixture with subscribe, publish, shard publish, and EVAL/EVALSHA handling, then move the remaining memory:// constructions in the Redis driver, Advanced Alchemy, framework, and Starlette unit tests onto it and assert that SQLAlchemyCacheBackend.from_url("memory://") stays local.
status: draft
state: pending
priority: P1
verification_strategy: refactor_verification
depends_on:
  - "driver-fail-loud:ch03:t02"
read_set:
  - src/py/tests/unit/conftest.py
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/extensions/test_alchemy_cache_backend.py
  - src/py/tests/unit/framework/test_framework_subdomains.py
  - src/py/tests/unit/extensions/test_starlette_plugin.py
files:
  - src/py/tests/unit/conftest.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/extensions/test_alchemy_cache_backend.py
  - src/py/tests/unit/framework/test_framework_subdomains.py
  - src/py/tests/unit/extensions/test_starlette_plugin.py
estimated_loc: 125
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: redis-driver
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: Redis sync and async drivers and their pub/sub and scripting wire commands
  - id: alchemy-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py::SQLAlchemyCacheBackend.from_url
    title: Advanced Alchemy cache memory:// local branch
tags:
  - redis
  - valkey
  - testing
  - fixtures
  - pubsub
---

# Task t03: Loopback RESP3 Pub/Sub and Scripting Surface and Consumer Tests Over the Wire

## Objective & Scope Boundary

Extend `src/py/tests/unit/conftest.py::Resp3FakeServer` from `driver-fail-loud:ch03:t02` with `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `SPUBLISH`, `EVAL`, and `EVALSHA`, then retarget the two `test_redis_valkey_driver.py` tests that call `publish` or `eval_script` and the Advanced Alchemy, framework, and Starlette tests that construct `memory://` Redis configs. Production code is unchanged; every retargeted test passes on `HEAD` before and after, exercising the real wire path of `SyncRedisDriver` and `AsyncRedisDriver`.[^redis-driver] Add one preservation test proving `SQLAlchemyCacheBackend.from_url("memory://")` stays local.[^alchemy-cache]

Out of scope: the transport, encoder, teardown, handshake, and key-value commands (owned by `driver-fail-loud:ch03:t02`); `test_redis_valkey_resp3.py`, `test_native_db_drivers.py`, conformance, and every production change (owned by `ch04-redis-valkey-memory-removal`).

## Bounded Read Manifest

- `src/py/tests/unit/conftest.py` — `Resp3FakeServer`, `_dispatch`, `_Handler.handle`, `_encode`, `_ErrorReply`, and the state lock from `driver-fail-loud:ch03:t02`.
- `src/py/vertebra/db/adapters/redis/driver.py` — pub/sub and scripting wire commands: `PUBLISH`, `SPUBLISH` (through `execute_command`), `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `UNSUBSCRIBE`, `EVALSHA`, `EVAL`; `eval_script` sends `EVALSHA` and falls back to `EVAL` when the error text contains `NOSCRIPT`; `publish` returns `int(res)`.[^redis-driver]
- `src/py/tests/unit/db/test_redis_valkey_driver.py::test_sync_redis_driver_memory_store_operations`, `::test_async_redis_driver_memory_store_operations`.
- `src/py/tests/unit/extensions/test_alchemy_cache_backend.py::test_alchemy_cache_clear_removes_only_its_prefix`, `::test_alchemy_cache_sends_sub_second_ttl_as_milliseconds`.
- `src/py/tests/unit/framework/test_framework_subdomains.py::test_key_value_store_redis_invalidation_read_through_and_ttl_renewal`.
- `src/py/tests/unit/extensions/test_starlette_plugin.py::test_cache_store_delete_all_removes_only_its_prefix`.

## Symbol & Signature Contracts

Additions to `src/py/tests/unit/conftest.py` (no `from __future__ import annotations`; Google docstrings; no inline comments):

```python
class Resp3FakeServer:
    """Threaded loopback RESP3 server holding keys, expiries, scripts, and subscriptions in process."""


def _encode_push(items: Sequence[bytes | int]) -> bytes: ...


def _script_result(script: str, keys: list[bytes], args: list[bytes], values: dict[bytes, bytes]) -> object: ...
```

- The class docstring above replaces the `driver-fail-loud:ch03:t02` docstring; the public surface (`port`, `redis_url`, `valkey_url`, `commands`, `close`, `resp3_server`) is unchanged.
- State added under the existing `threading.Lock`: `dict[str, str]` scripts by SHA1 hex and `dict[socket.socket, set[tuple[str, bytes]]]` subscriptions keyed by connection with `(kind, name)` where kind is `message`, `pmessage`, or `smessage`. `_Handler.handle` removes the connection's subscription entry when its loop ends, in the same locked block that removes the socket from the accepted set.
- `_encode_push`: `><n>\r\n` followed by each item encoded as a bulk string (`bytes`) or integer (`int`).
- Every push frame written to another subscribed connection is sent with `sendall` inside `_dispatch`, which already runs while holding the state lock, so push frames and replies never interleave on one socket.

| Command | Reply |
| --- | --- |
| `EVALSHA sha n keys... args...` | `-NOSCRIPT No matching script. Please use EVAL.` when unknown, else the script result |
| `EVAL script n keys... args...` | stores the SHA1, returns the script result |
| `SUBSCRIBE` / `PSUBSCRIBE` / `SSUBSCRIBE` names... | one push per name: `>3` of `subscribe`/`psubscribe`/`ssubscribe`, name, subscription count; registers `(message, name)`, `(pmessage, name)`, or `(smessage, name)` |
| `UNSUBSCRIBE [names...]` | removes the named (or all) `message` subscriptions; one `>3` `unsubscribe` push per name |
| `PUBLISH ch payload` | writes `>3 message ch payload` to each connection with `(message, ch)` and `>4 pmessage pattern ch payload` to each connection with a matching `(pmessage, pattern)` (`fnmatch.fnmatchcase`) first, then integer delivery count to the publisher; `smessage` subscriptions never receive `PUBLISH` traffic |
| `SPUBLISH ch payload` | writes `>3 smessage ch payload` to each connection with `(smessage, ch)` first, then integer delivery count to the publisher (Redis 7 shard pub/sub) |

Script result rule (`_script_result`): when the script text contains `KEYS[1]` and a key is given, return `GET keys[0]` (bulk value or `None`); else when it contains `ARGV[1]` and an argument is given, return `args[0]` as bulk; else `"OK"` (encoded `+OK`).

## Edge Cases & Error Semantics

- `PUBLISH` with no subscribers returns `0`.
- `SSUBSCRIBE` subscribers receive `smessage` frames only from `SPUBLISH`; the driver has no `spublish` helper, so tests send `driver.execute_command("SPUBLISH", channel, payload)`.
- A client closing its socket drops its subscriptions, so a later `PUBLISH` does not write to a closed socket.
- `EVALSHA` for an unknown SHA replies `NOSCRIPT`, which drives the driver's `EVAL` fallback; a second `eval_script` with the same text hits the stored SHA.
- `UNSUBSCRIBE` with no names removes every `message` subscription of that connection and leaves `pmessage` and `smessage` entries in place.

## Public-Seam Test Blueprint

- **Fixture file**: `src/py/tests/unit/conftest.py`. Dependency category: local-substitutable network boundary.
- **Fixture parameter typing**: every retargeted test declares `resp3_server: Resp3FakeServer` and each module adds `from typing import TYPE_CHECKING` plus `if TYPE_CHECKING: from tests.unit.conftest import Resp3FakeServer` (`test_redis_valkey_driver.py` already has it from `driver-fail-loud:ch03:t02`). The four test files keep `from __future__ import annotations`, so the annotation stays unquoted: ruff `UP` (`UP037`) rejects quoted annotations in modules with deferred evaluation.
- **Retarget matrix**:

| Test (new name when renamed) | Concrete Change | Expected Output |
| --- | --- | --- |
| `test_redis_valkey_driver.py::test_sync_redis_driver_memory_store_operations` → `test_sync_redis_driver_store_operations(resp3_server)` | `SyncRedisDriver(RedisConfig(url=resp3_server.redis_url))` | `execute_pipeline([("SET","k2","v2"),("GET","k2"),("DEL","k1")]) == ["OK", b"v2", 1]`; `eval_script("return redis.call('get', KEYS[1])", keys=["k2"]) == b"v2"`; after `flushdb()`, `pttl("k2") == -2` |
| `test_redis_valkey_driver.py::test_async_redis_driver_memory_store_operations` → `test_async_redis_driver_store_operations(resp3_server)` | `AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url))` | `await driver.publish("news", "headline") == 0`; `await driver.eval_script("return ARGV[1]", args=["hello"]) == b"hello"` |
| `test_alchemy_cache_backend.py::test_alchemy_cache_clear_removes_only_its_prefix(resp3_server)` | both drivers on `resp3_server.redis_url` | `sync_driver.get("other:keep") == b"1"`; `await backend_b.get_async("user:9") == {"id": 9}` |
| `test_alchemy_cache_backend.py::test_alchemy_cache_sends_sub_second_ttl_as_milliseconds(resp3_server)` | fixture URL | `1 <= sync_driver.pttl("vtb:aa:ephemeral") <= 250` |
| new `test_alchemy_cache_backend.py::test_alchemy_cache_memory_url_stays_local` | `SQLAlchemyCacheBackend.from_url("memory://")` | `backend.sync_driver is None and backend.async_driver is None` |
| `test_framework_subdomains.py::test_key_value_store_redis_invalidation_read_through_and_ttl_renewal(resp3_server)` | `AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url))` | `5000 < await driver.pttl("vertebra:warm") <= 20000` |
| `test_starlette_plugin.py::test_cache_store_delete_all_removes_only_its_prefix(resp3_server)` | fixture URL | `await driver.get("other:keep") == b"x"` |

## Verification

- Baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/framework/test_framework_subdomains.py src/py/tests/unit/extensions/test_starlette_plugin.py -q` passes on `HEAD`.
- Coverage snapshot before edits: `uv run --no-sync coverage run --branch -m pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/framework/test_framework_subdomains.py src/py/tests/unit/extensions/test_starlette_plugin.py -q && uv run --no-sync coverage json -o /tmp/ch03-t03-before.json --include="src/py/vertebra/db/adapters/redis/*"`; repeat after edits into `/tmp/ch03-t03-after.json`. Lines that lose coverage lie only inside `SyncRedisDriver._publish_memory`, `_execute_memory_command`, `_eval_memory_script`, and `memory://` branches.
- Vacuity probe: temporarily make both `redis_url` and `valkey_url` return port `1`, rerun the six retargeted fixture tests, confirm they fail with `ConnectionRefusedError` / `OSError`, then restore.
- Green + lint: rerun the baseline command, then `uv run --no-sync ruff check src/py/tests/unit/conftest.py src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/framework/test_framework_subdomains.py src/py/tests/unit/extensions/test_starlette_plugin.py && uv run --no-sync ruff format --check src/py/tests/unit/conftest.py && uv run --no-sync mypy src/py/tests/unit/conftest.py`.
- Residue gate: `rg -n 'url="memory://"|from_valkey\("memory://"\)' src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/framework/test_framework_subdomains.py src/py/tests/unit/extensions/test_starlette_plugin.py` prints nothing.

## Acceptance Criteria

- [ ] No test in the four files constructs a `memory://` Redis or Valkey config (R7a).
- [ ] `SQLAlchemyCacheBackend.from_url("memory://")` is asserted local (preserved backend).
- [ ] Coverage diff over the five files loses only pub/sub, scripting, and remaining memory-emulation lines; vacuity probe over the six retargeted fixture tests goes red.
- [ ] Zero inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-08 00:00] `estimated_loc` is `125`: fixture additions about `90` lines (eight command cases, push encoder, script rule, subscription cleanup); retargets about `35`. The pub/sub surface also serves `test_redis_valkey_resp3.py::test_sync_multiplexed_pubsub_and_cluster_moved_ask`, which `ch04-redis-valkey-memory-removal` retargets.

[^redis-driver]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Redis sync and async drivers and their pub/sub and scripting wire commands.
[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/cache.py::SQLAlchemyCacheBackend.from_url` — Advanced Alchemy cache memory:// local branch.
