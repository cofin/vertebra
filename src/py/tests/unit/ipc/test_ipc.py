import pytest

from vertebra import _vertebra

pytestmark = pytest.mark.unit


def test_shm_region_roundtrip() -> None:
    region = _vertebra.ShmRegion(1024)
    region.write(0, b"hi")
    assert region.read(0, 2) == b"hi"


def test_buffer_pool_roundtrip() -> None:
    pool = _vertebra.BufferPool(32, 1)
    buf = pool.acquire()
    assert buf is not None
    buf.write(b"ok")
    assert buf.read()[:2] == b"ok"


def test_spsc_ring_roundtrip() -> None:
    ring = _vertebra.SpscRing(64, 8)
    ring.send(b"ping", 0)
    assert ring.recv(16) == b"ping"


def test_mpmc_ring_roundtrip() -> None:
    ring = _vertebra.MpmcRing(64, 8)
    ring.send(b"pong", 0)
    assert ring.recv(16) == b"pong"


def test_buffer_channel_roundtrip() -> None:
    channel = _vertebra.BufferChannel(64, 2, 8)
    buf = channel.acquire()
    assert buf is not None
    buf.write(b"data")
    channel.send(buf, 4, 9)
    received = channel.recv()
    recv_buf, length, flags = received
    assert length == 4
    assert flags == 9
    assert recv_buf.read()[:4] == b"data"


def test_context_managers() -> None:
    with _vertebra.ShmRegion(128) as region:
        region.write(0, b"x")
        assert region.read(0, 1) == b"x"
    pool = _vertebra.BufferPool(32, 1)
    buf = pool.acquire()
    assert buf is not None
    with buf as buffer:
        buffer.write(b"y")
        assert buffer.read()[:1] == b"y"
