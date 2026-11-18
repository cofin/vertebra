"""Public-seam unit tests for the framework-neutral ``vertebra.cache`` package."""

from __future__ import annotations

import inspect
import subprocess
import sys
import uuid
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING

import pytest

from vertebra._vertebra import ShmCache
from vertebra.cache import AsyncCacheStore, SyncCacheStore
from vertebra.typing import MissingDependencyError, reset_dependency_cache

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@contextmanager
def _shm_segment(prefix: str = "vtb-test-cache") -> Iterator[str]:
    segment = f"{prefix}-{uuid.uuid4().hex[:10]}"
    try:
        yield segment
    finally:
        handle = ShmCache(segment, capacity_bytes=65536, max_entries=16)
        handle.close()
        handle.unlink()


def _build_backend_url(kind: str, tmp_path: Path, stack: ExitStack) -> tuple[str, str | None]:
    if kind == "memory":
        return "memory://", None
    if kind == "shm":
        seg = stack.enter_context(_shm_segment("vtb-test-shm"))
        return f"shm://{seg}", None
    if kind == "file":
        root = tmp_path / f"store-{uuid.uuid4().hex[:8]}"
        root.mkdir(parents=True, exist_ok=True)
        return f"file://{root}", None
    if kind == "memory+shm":
        seg = stack.enter_context(_shm_segment("vtb-test-l1"))
        return "memory://", f"shm://{seg}"
    msg = f"Unknown test backend kind: {kind!r}"
    raise AssertionError(msg)


def test_import_isolation_excludes_orm_and_extensions() -> None:
    """Importing ``vertebra.cache`` and constructing memory stores never loads ORM or extension modules."""
    script = (
        "import sys; "
        "from vertebra.cache import AsyncCacheStore, CacheStats, SyncCacheStore; "
        "s = SyncCacheStore('memory://'); "
        "a = AsyncCacheStore('memory://'); "
        "s.set('k', b'v'); "
        "assert s.get('k') == b'v'; "
        "forbidden = [m for m in sys.modules if m.startswith(('advanced_alchemy', 'sqlalchemy', 'vertebra.extensions'))]; "
        "assert not forbidden, forbidden; "
        "assert CacheStats().capacity == 0"
    )
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr


def test_sync_and_async_constructors_match_signature() -> None:
    """SyncCacheStore and AsyncCacheStore expose identical ``__init__`` parameter contracts."""
    sync_params = list(inspect.signature(SyncCacheStore.__init__).parameters.values())[1:]
    async_params = list(inspect.signature(AsyncCacheStore.__init__).parameters.values())[1:]
    assert [(p.name, p.kind, p.default) for p in sync_params] == [(p.name, p.kind, p.default) for p in async_params]


@pytest.mark.parametrize(
    ("url", "l1_url", "capacity", "key_prefix"),
    [
        ("unknown://localhost", None, 16, "vtb:cache:"),
        ("noscheme", None, 16, "vtb:cache:"),
        ("http://localhost", None, 16, "vtb:cache:"),
        ("memory://", "bad://l1", 16, "vtb:cache:"),
        ("memory://", None, 0, "vtb:cache:"),
        ("memory://", None, -5, "vtb:cache:"),
        ("memory://", None, 16, ""),
        ("memory://", None, 16, "   "),
        ("memory://", None, 16, "vtb:*"),
        ("memory://", None, 16, "vtb:?"),
        ("memory://", None, 16, "vtb:[a]"),
    ],
)
def test_invalid_configuration_raises_value_error(
    url: str,
    l1_url: str | None,
    capacity: int,
    key_prefix: str,
) -> None:
    """Unknown schemes, bad ``l1_url``, non-positive capacity, and invalid ``key_prefix`` raise ``ValueError``."""
    with pytest.raises(ValueError):
        SyncCacheStore(url, l1_url=l1_url, capacity=capacity, key_prefix=key_prefix)
    with pytest.raises(ValueError):
        AsyncCacheStore(url, l1_url=l1_url, capacity=capacity, key_prefix=key_prefix)


@pytest.mark.parametrize(
    ("url", "masked_module", "expected_extra"),
    [
        ("valkey://localhost:6379/0", "vertebra._valkey", "valkey"),
        ("valkeys://localhost:6379/0", "vertebra._valkey", "valkey"),
        ("redis://localhost:6379/0", "vertebra._redis", "redis"),
        ("rediss://localhost:6379/0", "vertebra._redis", "redis"),
        ("s3://bucket/prefix", "vertebra._storage", "storage"),
        ("gs://bucket/prefix", "vertebra._storage", "storage"),
        ("az://container/prefix", "vertebra._storage", "storage"),
        ("r2://bucket/prefix", "vertebra._storage", "storage"),
        ("oci://bucket/prefix", "vertebra._storage", "storage"),
        ("file:///tmp/vtb-missing-dep-test", "vertebra._storage", "storage"),
    ],
)
def test_missing_native_driver_raises_missing_dependency_error(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    masked_module: str,
    expected_extra: str,
) -> None:
    """Missing optional driver wheels raise ``MissingDependencyError`` for matching URL schemes."""
    monkeypatch.setitem(sys.modules, masked_module, None)
    reset_dependency_cache()
    try:
        with pytest.raises(MissingDependencyError) as sync_exc:
            SyncCacheStore(url)
        assert sync_exc.value.install_package == f"vertebra-core[{expected_extra}]"
        assert f"vertebra-core[{expected_extra}]" in sync_exc.value.hint

        with pytest.raises(MissingDependencyError) as async_exc:
            AsyncCacheStore(url)
        assert async_exc.value.install_package == f"vertebra-core[{expected_extra}]"
        assert f"vertebra-core[{expected_extra}]" in async_exc.value.hint
    finally:
        monkeypatch.undo()
        reset_dependency_cache()


@pytest.mark.parametrize("backend_kind", ["memory", "shm", "file", "memory+shm"])
def test_sync_crud_across_backends(backend_kind: str, tmp_path: Path) -> None:
    """SyncCacheStore supports full CRUD, conditional add, batch ops, and stats across backends."""
    with ExitStack() as stack:
        url, l1_url = _build_backend_url(backend_kind, tmp_path, stack)
        store = SyncCacheStore(url, l1_url=l1_url, capacity=64)
        try:
            assert store.get("missing") is None
            assert store.has("missing") is False
            assert store.delete("missing") is False

            store.set("alpha", b"one")
            assert store.has("alpha") is True
            assert store.get("alpha") == b"one"

            assert store.add("alpha", b"two") is False
            assert store.get("alpha") == b"one"
            assert store.add("beta", b"two") is True
            assert store.get("beta") == b"two"

            store.set_many({"gamma": b"three", "delta": b"four"})
            assert store.get_many(["alpha", "gamma", "missing"]) == {"alpha": b"one", "gamma": b"three"}

            assert store.delete("alpha") is True
            assert store.get("alpha") is None
            assert store.delete_many(["beta", "gamma", "beta", "missing"]) == 2
            assert store.has("beta") is False
            assert store.has("delta") is True

            snapshot = store.stats()
            assert snapshot.capacity == 64
            assert snapshot.hits >= 3
            assert snapshot.misses >= 1
        finally:
            store.close()


@pytest.mark.anyio
@pytest.mark.parametrize("backend_kind", ["memory", "shm", "file", "memory+shm"])
async def test_async_crud_across_backends(backend_kind: str, tmp_path: Path) -> None:
    """AsyncCacheStore supports full async CRUD, conditional add, batch ops, and stats across backends."""
    with ExitStack() as stack:
        url, l1_url = _build_backend_url(backend_kind, tmp_path, stack)
        store = AsyncCacheStore(url, l1_url=l1_url, capacity=64)
        try:
            assert await store.get("missing") is None
            assert await store.has("missing") is False
            assert await store.delete("missing") is False

            await store.set("alpha", b"one")
            assert await store.has("alpha") is True
            assert await store.get("alpha") == b"one"

            assert await store.add("alpha", b"two") is False
            assert await store.add("beta", b"two") is True

            await store.set_many({"gamma": b"three", "delta": b"four"})
            assert await store.get_many(["alpha", "gamma", "missing"]) == {"alpha": b"one", "gamma": b"three"}

            assert await store.delete("alpha") is True
            assert await store.delete_many(["beta", "gamma", "missing"]) == 2
            assert await store.has("delta") is True

            snapshot = await store.stats()
            assert snapshot.capacity == 64
            assert snapshot.hits >= 2
            assert snapshot.misses >= 1
        finally:
            await store.aclose()


@pytest.mark.anyio
@pytest.mark.parametrize("l1_mode", [False, True])
async def test_async_from_sync_shares_memory_state_and_survives_aclose(l1_mode: bool) -> None:
    """``AsyncCacheStore.from_sync`` shares memory state with sync store and ``aclose()`` keeps data intact."""
    with ExitStack() as stack:
        l1_url = f"shm://{stack.enter_context(_shm_segment('vtb-from-sync'))}" if l1_mode else None
        sync_store = SyncCacheStore("memory://", l1_url=l1_url, capacity=32)
        async_store = AsyncCacheStore.from_sync(sync_store)
        try:
            sync_store.set("shared", b"sync-value")
            assert await async_store.get("shared") == b"sync-value"

            await async_store.set("from-async", b"async-value")
            assert sync_store.get("from-async") == b"async-value"

            await async_store.aclose()
            assert sync_store.get("shared") == b"sync-value"
            assert sync_store.get("from-async") == b"async-value"
            assert sync_store.stats().hits >= 3
        finally:
            sync_store.close()


@pytest.mark.anyio
async def test_async_from_sync_shares_shm_segment() -> None:
    """``AsyncCacheStore.from_sync`` attaches to the same POSIX SHM segment without clobbering on ``aclose()``."""
    with _shm_segment("vtb-shm-from-sync") as seg:
        sync_store = SyncCacheStore(f"shm://{seg}", capacity=32)
        async_store = AsyncCacheStore.from_sync(sync_store)
        try:
            sync_store.set("item", b"payload")
            assert await async_store.get("item") == b"payload"
            await async_store.aclose()
            assert sync_store.get("item") == b"payload"
        finally:
            sync_store.close()


@pytest.mark.parametrize("backend_kind", ["memory", "shm", "file"])
def test_incr_initializes_default_ttl_and_rejects_non_integer_bytes(
    backend_kind: str,
    tmp_path: Path,
) -> None:
    """``incr`` initializes missing keys to ``delta`` and raises ``ValueError`` on non-integer bytes."""
    with ExitStack() as stack:
        url, l1_url = _build_backend_url(backend_kind, tmp_path, stack)
        now = 100.0
        clock = None if backend_kind == "shm" else (lambda: now)
        store = SyncCacheStore(url, l1_url=l1_url, default_ttl=5.0, _clock=clock)
        try:
            assert store.incr("counter", 3) == 3
            assert store.incr("counter", 4) == 7
            assert store.get("counter") == b"7"

            if clock is not None:
                now = 106.0
                assert store.get("counter") is None
                assert store.incr("counter", 2) == 2

            store.set("bad-text", b"not-an-int")
            with pytest.raises(ValueError):
                store.incr("bad-text")

            store.set("bad-underscore", b"1_0")
            with pytest.raises(ValueError):
                store.incr("bad-underscore")
        finally:
            store.close()


@pytest.mark.anyio
@pytest.mark.parametrize("backend_kind", ["memory", "shm", "file"])
async def test_async_incr_and_non_integer_rejection(backend_kind: str, tmp_path: Path) -> None:
    """Async ``incr`` initializes missing keys with ``default_ttl`` and rejects non-integer bytes."""
    with ExitStack() as stack:
        url, l1_url = _build_backend_url(backend_kind, tmp_path, stack)
        now = 200.0
        clock = None if backend_kind == "shm" else (lambda: now)
        store = AsyncCacheStore(url, l1_url=l1_url, default_ttl=5.0, _clock=clock)
        try:
            assert await store.incr("counter", 5) == 5
            assert await store.incr("counter", -2) == 3

            if clock is not None:
                now = 206.0
                assert await store.incr("counter", 9) == 9

            await store.set("bad", b"xyz")
            with pytest.raises(ValueError):
                await store.incr("bad")
        finally:
            await store.aclose()


@pytest.mark.anyio
@pytest.mark.parametrize("backend_kind", ["memory", "file"])
async def test_ttl_expiration_and_touch_with_injected_clock(backend_kind: str, tmp_path: Path) -> None:
    """TTL expiration, non-positive TTL immediate expiry, and ``touch`` honor an injected clock."""
    now = 1000.0

    def clock() -> float:
        return now

    with ExitStack() as stack:
        url, _ = _build_backend_url(backend_kind, tmp_path, stack)
        sync_store = SyncCacheStore(url, default_ttl=10.0, _clock=clock)
        async_store = AsyncCacheStore(url, default_ttl=10.0, _clock=clock, key_prefix="vtb:async:")
        try:
            sync_store.set("a", b"val-a")
            sync_store.set("zero", b"z", ttl=0.0)
            sync_store.set("neg", b"n", ttl=-1.0)
            assert sync_store.get("zero") is None
            assert sync_store.get("neg") is None

            now += 6.0
            assert sync_store.get("a") == b"val-a"
            assert sync_store.touch("a", ttl=10.0) is True
            assert sync_store.touch("missing", ttl=5.0) is False

            now += 6.0
            assert sync_store.get("a") == b"val-a"

            now += 10.0
            assert sync_store.get("a") is None
            assert sync_store.touch("a", ttl=5.0) is False

            await async_store.set("b", b"val-b", ttl=4.0)
            now += 3.0
            assert await async_store.touch("b", ttl=8.0) is True
            now += 6.0
            assert await async_store.get("b") == b"val-b"
            assert await async_store.touch("b", ttl=0.0) is True
            assert await async_store.get("b") is None
        finally:
            sync_store.close()
            await async_store.aclose()


def test_memory_lru_eviction_updates_stats_and_bounds_size() -> None:
    """LRU capacity eviction updates ``stats().evictions`` and bounds ``stats().size``."""
    now = 50.0
    store = SyncCacheStore("memory://", capacity=2, _clock=lambda: now)
    try:
        store.set("k1", b"v1")
        store.set("k2", b"v2")
        assert store.get("k1") == b"v1"
        store.set("k3", b"v3")

        assert store.get("k2") is None
        assert store.get("k1") == b"v1"
        assert store.get("k3") == b"v3"

        snapshot = store.stats()
        assert snapshot.evictions == 1
        assert snapshot.size == 2
        assert snapshot.capacity == 2

        store.set("k4", b"v4", ttl=1.0)
        now += 2.0
        store.set("k5", b"v5")
        after_expiry = store.stats()
        assert after_expiry.expirations == 1
        assert after_expiry.size == 2
    finally:
        store.close()


@pytest.mark.parametrize(
    ("backend_kind", "prefix_a", "prefix_b"),
    [
        ("shm", "svc-a:", "svc-b:"),
        ("shm", "long-namespace-" + ("x" * 70) + ":", "long-namespace-" + ("y" * 70) + ":"),
        ("file", "svc-a:", "svc-b:"),
    ],
)
def test_prefix_isolation_on_clear_and_delete_prefix(
    backend_kind: str,
    prefix_a: str,
    prefix_b: str,
    tmp_path: Path,
) -> None:
    """Distinct ``key_prefix`` stores sharing one backend never clear each other."""
    with ExitStack() as stack:
        url, _ = _build_backend_url(backend_kind, tmp_path, stack)
        store_a = SyncCacheStore(url, key_prefix=prefix_a)
        store_b = SyncCacheStore(url, key_prefix=prefix_b)
        try:
            store_a.set("users:1", b"a1")
            store_a.set("users:2", b"a2")
            store_a.set("other:1", b"a3")
            store_b.set("users:1", b"b1")
            store_b.set("other:1", b"b2")

            removed = store_a.delete_prefix("users:")
            assert removed == 2
            assert store_a.get("users:1") is None
            assert store_a.get("other:1") == b"a3"
            assert store_b.get("users:1") == b"b1"

            store_a.clear()
            assert store_a.get("other:1") is None
            assert store_b.get("users:1") == b"b1"
            assert store_b.get("other:1") == b"b2"
        finally:
            store_a.close()
            store_b.close()


def test_shm_long_keys_above_128_bytes_roundtrip_and_stay_isolated() -> None:
    """Keys exceeding 128 UTF-8 bytes hash via BLAKE2b without collisions and stay namespace-scoped."""
    with _shm_segment("vtb-shm-longkey") as seg:
        store_a = SyncCacheStore(f"shm://{seg}", key_prefix="ns-a:")
        store_b = SyncCacheStore(f"shm://{seg}", key_prefix="ns-b:")
        try:
            shared_stem = "segment-" + ("é" * 60) + "-"
            key_one = shared_stem + "alpha"
            key_two = shared_stem + "beta"
            assert len(key_one.encode("utf-8")) > 128

            store_a.set(key_one, b"v1")
            store_a.set(key_two, b"v2")
            store_b.set(key_one, b"b1")

            assert store_a.get(key_one) == b"v1"
            assert store_a.get(key_two) == b"v2"
            assert store_b.get(key_one) == b"b1"

            assert store_a.delete_prefix("segment-") == 2
            assert store_a.get(key_one) is None
            assert store_b.get(key_one) == b"b1"
        finally:
            store_a.close()
            store_b.close()


@pytest.mark.anyio
async def test_oversize_shm_payloads_evict_stale_l1_and_pure_shm() -> None:
    """Payloads above 856 bytes evict stale SHM entries in both pure SHM and L1+L2 stores."""
    with _shm_segment("vtb-shm-pure") as pure_seg, _shm_segment("vtb-shm-l1") as l1_seg:
        pure_shm = SyncCacheStore(f"shm://{pure_seg}")
        tiered_sync = SyncCacheStore("memory://", l1_url=f"shm://{l1_seg}")
        tiered_async = AsyncCacheStore.from_sync(tiered_sync)
        raw_l1 = ShmCache(l1_seg, capacity_bytes=65536, max_entries=2048)
        try:
            pure_shm.set("k", b"small")
            assert pure_shm.get("k") == b"small"
            pure_shm.set("k", b"x" * 900)
            assert pure_shm.get("k") is None

            tiered_sync.set("big", b"initial-small")
            assert tiered_sync.get("big") == b"initial-small"

            oversize = b"z" * 1024
            tiered_sync.set("big", oversize)
            assert raw_l1.get("vtb:cache:big") is None
            assert tiered_sync.get("big") == oversize
            assert await tiered_async.get("big") == oversize
            assert raw_l1.get("vtb:cache:big") is None
        finally:
            raw_l1.close()
            await tiered_async.aclose()
            tiered_sync.close()
            pure_shm.close()
