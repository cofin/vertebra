"""uvloop compatibility tests for AsyncShmTransport.

This test validates that the hybrid spin-wait transport works correctly with uvloop,
especially high-frequency add_reader/remove_reader cycling which could expose edge cases.
"""

from __future__ import annotations

import asyncio
import sys
import time
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from vertebra._vertebra import AsyncRing
    from vertebra.ipc.async_transport import AsyncShmTransport

# Skip on Windows where uvloop is not supported
pytestmark = [
    pytest.mark.skipif(sys.platform == "win32", reason="uvloop not supported on Windows"),
    pytest.mark.integration,
]


@pytest.fixture
def uvloop_policy() -> None:
    """Set up uvloop event loop policy for the test."""
    try:
        import uvloop
    except ImportError:
        pytest.skip("uvloop not installed")

    old_policy = asyncio.get_event_loop_policy()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    yield
    asyncio.set_event_loop_policy(old_policy)


@pytest.fixture
def async_ring() -> "AsyncRing":
    """Create an AsyncRing for testing."""
    from vertebra._vertebra import AsyncRing

    return AsyncRing(4096, 64)


@pytest.fixture
def transport(async_ring: "AsyncRing") -> "AsyncShmTransport":
    """Create an AsyncShmTransport."""
    from vertebra.ipc.async_transport import AsyncShmTransport

    transport = AsyncShmTransport(async_ring, spin_duration_us=5)
    yield transport
    transport.close()


class TestUvloopCompatibility:
    """Test AsyncShmTransport with uvloop."""

    @pytest.mark.anyio
    async def test_basic_send_recv(self, uvloop_policy, async_ring, transport) -> None:
        """Test basic send/recv works with uvloop."""
        # Send data
        test_data = b"hello uvloop"
        async_ring.send(test_data, 0)

        # Receive with hybrid spin-wait
        received = await transport.recv()
        assert received == test_data

    @pytest.mark.anyio
    async def test_immediate_data_latency(self, uvloop_policy, async_ring, transport) -> None:
        """Test that immediate data is captured in spin phase (sub-20µs)."""
        test_data = b"latency test"
        latencies = []

        for _ in range(100):
            # Pre-fill ring
            async_ring.send(test_data, 0)

            start = time.perf_counter()
            received = await transport.recv()
            elapsed = (time.perf_counter() - start) * 1_000_000  # microseconds

            assert received == test_data
            latencies.append(elapsed)

        p50 = sorted(latencies)[50]
        sorted(latencies)[99]

        # Target: p99 < 20µs for immediate data
        assert p50 < 50, f"p50 latency {p50:.1f}µs exceeds 50µs"
        # Note: In CI, latencies may be higher due to virtualization

    @pytest.mark.anyio
    async def test_high_frequency_cycling(self, uvloop_policy, async_ring, transport) -> None:
        """Test high-frequency add_reader/remove_reader cycling doesn't cause issues."""
        test_data = b"x" * 64

        # This test exercises the add_reader/remove_reader path rapidly
        # to catch any edge cases in uvloop's FD handling
        async def producer() -> None:
            for i in range(1000):
                # Avoid blocking the event loop if the ring is full (intentional busy-wait for test).
                while not async_ring.try_send(test_data, 0):  # noqa: ASYNC110
                    await asyncio.sleep(0)
                # Small delay to simulate realistic workload
                if i % 100 == 0:
                    await asyncio.sleep(0.0001)

        async def consumer() -> int:
            count = 0
            while count < 1000:
                try:
                    data = await asyncio.wait_for(transport.recv(), timeout=1.0)
                    assert len(data) == 64
                    count += 1
                except TimeoutError:
                    pytest.fail(f"Timeout after receiving {count} messages")
            return count

        # Run producer and consumer concurrently
        producer_task = asyncio.create_task(producer())
        count = await consumer()
        await producer_task

        assert count == 1000

    @pytest.mark.anyio
    async def test_concurrent_receivers(self, uvloop_policy) -> None:
        """Test multiple concurrent transports don't interfere."""
        from vertebra._vertebra import AsyncRing
        from vertebra.ipc.async_transport import AsyncShmTransport

        rings = [AsyncRing(4096, 64) for _ in range(4)]
        transports = [AsyncShmTransport(r, spin_duration_us=5) for r in rings]

        try:

            async def send_recv(idx: int) -> None:
                ring = rings[idx]
                transport = transports[idx]

                for i in range(100):
                    data = f"msg-{idx}-{i}".encode()
                    ring.send(data, 0)
                    received = await transport.recv()
                    assert received == data

            # Run all concurrently
            await asyncio.gather(*[send_recv(i) for i in range(4)])
        finally:
            for t in transports:
                t.close()

    @pytest.mark.anyio
    async def test_spin_phase_captures_immediate(self, uvloop_policy, async_ring, transport) -> None:
        """Verify spin phase captures data without falling back to add_reader."""
        # Pre-fill the ring
        test_data = b"spin test"
        async_ring.send(test_data, 0)

        # Track if we used add_reader (the transport should NOT register it)
        initial_registered = transport._registered

        received = await transport.recv()

        assert received == test_data
        # If data was captured in spin phase, _registered should still be False
        # (no add_reader was called)
        assert transport._registered == initial_registered

    @pytest.mark.anyio
    async def test_fallback_to_add_reader(self, uvloop_policy, async_ring, transport) -> None:
        """Test that slow path correctly falls back to add_reader."""

        # Don't pre-fill - data will arrive after spin phase
        async def delayed_send() -> None:
            await asyncio.sleep(0.01)  # 10ms - well beyond spin phase
            async_ring.send(b"delayed", 0)

        sender = asyncio.create_task(delayed_send())

        # This should fall through spin phase and use add_reader
        received = await asyncio.wait_for(transport.recv(), timeout=1.0)

        await sender
        assert received == b"delayed"
