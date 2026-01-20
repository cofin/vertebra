"""Tests for multi-client concurrent access to the shared-memory broker.

These tests verify that multiple ring buffers can operate concurrently
without interference. Each ring is SPSC (single-producer, single-consumer).
"""

from __future__ import annotations

import asyncio

import pytest

from tests.helpers.shm import skip_if_shm_unavailable, tracked_shm_cleanup, unique_shm_name

pytestmark = [pytest.mark.anyio, pytest.mark.integration]


def _skip_if_async_ring_unavailable() -> None:
    """Skip test if AsyncRing (PyO3) is not available."""
    try:
        from vertebra._vertebra import AsyncRing  # noqa: F401
    except ImportError:
        pytest.skip("AsyncRing (PyO3) not available")
    skip_if_shm_unavailable(require_shm_lib=False)


async def test_multi_ring_isolation() -> None:
    """Test that separate rings are properly isolated."""
    _skip_if_async_ring_unavailable()

    from vertebra._vertebra import AsyncRing

    ring_name_a = f"/{unique_shm_name('isolation-a')}"
    ring_name_b = f"/{unique_shm_name('isolation-b')}"

    with tracked_shm_cleanup([ring_name_a, ring_name_b]):
        ring_a = AsyncRing.create_named(ring_name_a, slot_size=4096, slots=8)
        ring_b = AsyncRing.create_named(ring_name_b, slot_size=4096, slots=8)

        # Send different messages to each ring
        ring_a.send(b"message-for-ring-a", 0)
        ring_b.send(b"message-for-ring-b", 0)

        # Verify each ring only sees its own messages
        data_a = ring_a.try_recv()
        data_b = ring_b.try_recv()

        assert data_a == b"message-for-ring-a"
        assert data_b == b"message-for-ring-b"

        # Verify rings are now empty
        assert ring_a.try_recv() is None
        assert ring_b.try_recv() is None


async def test_concurrent_ring_operations() -> None:
    """Test that multiple rings can be used concurrently via asyncio tasks."""
    _skip_if_async_ring_unavailable()

    from vertebra._internal.async_transport import PollingTransport
    from vertebra._vertebra import AsyncRing

    num_rings = 4
    messages_per_ring = 10
    ring_names = [f"/{unique_shm_name(f'concurrent-{i}')}" for i in range(num_rings)]

    with tracked_shm_cleanup(ring_names):
        rings = [AsyncRing.create_named(name, slot_size=4096, slots=16) for name in ring_names]

        async def ring_test(ring_id: int, ring: AsyncRing) -> bool:
            """Test send/recv on a single ring."""
            transport = PollingTransport(ring, poll_interval=0.001)
            try:
                for i in range(messages_per_ring):
                    msg = f"ring-{ring_id}-msg-{i}".encode()
                    await asyncio.wait_for(transport.send(msg), timeout=2.0)
                    received = await asyncio.wait_for(transport.recv(), timeout=2.0)
                    if received != msg:
                        return False
                return True
            finally:
                transport.close()

        # Run all ring tests concurrently
        tasks = [asyncio.create_task(ring_test(i, ring)) for i, ring in enumerate(rings)]

        results = await asyncio.gather(*tasks)

        # Verify all rings worked correctly
        for i, success in enumerate(results):
            assert success, f"Ring {i} failed"


async def test_ring_capacity_limits() -> None:
    """Test that ring buffers correctly handle capacity limits."""
    _skip_if_async_ring_unavailable()

    from vertebra._vertebra import AsyncRing

    ring_name = f"/{unique_shm_name('capacity')}"

    with tracked_shm_cleanup([ring_name]):
        # Create a small ring with limited slots
        ring = AsyncRing.create_named(ring_name, slot_size=256, slots=4)

        # Fill the ring using try_send (non-blocking)
        messages_sent = 0
        for i in range(6):  # Try to send more than capacity
            sent = ring.try_send(f"msg-{i}".encode(), 0)
            if sent:
                messages_sent += 1

        # Should have sent up to ring capacity
        assert messages_sent > 0, "Should have sent at least one message"
        assert messages_sent <= 4, f"Should not exceed capacity, got {messages_sent}"

        # Drain the ring
        received = []
        while True:
            data = ring.try_recv()
            if data is None:
                break
            received.append(data)

        # Should receive all sent messages
        assert len(received) == messages_sent
        for i, data in enumerate(received):
            assert data == f"msg-{i}".encode()


async def test_named_ring_reopen() -> None:
    """Test that a named ring can be reopened by another handle."""
    _skip_if_async_ring_unavailable()

    from vertebra._vertebra import AsyncRing

    ring_name = f"/{unique_shm_name('reopen')}"

    with tracked_shm_cleanup([ring_name]):
        # Create the ring
        creator = AsyncRing.create_named(ring_name, slot_size=4096, slots=8)

        # Open the same ring from another handle
        opener = AsyncRing.open_named(ring_name, slot_size=4096, slots=8)

        # Send from creator
        creator.send(b"hello from creator", 0)

        # Receive from opener
        data = opener.try_recv()
        assert data == b"hello from creator"

        # Send from opener
        opener.send(b"hello from opener", 0)

        # Receive from creator
        data = creator.try_recv()
        assert data == b"hello from opener"


async def test_multiple_messages_ordering() -> None:
    """Test that messages are received in FIFO order."""
    _skip_if_async_ring_unavailable()

    from vertebra._internal.async_transport import PollingTransport
    from vertebra._vertebra import AsyncRing

    ring_name = f"/{unique_shm_name('ordering')}"

    with tracked_shm_cleanup([ring_name]):
        ring = AsyncRing.create_named(ring_name, slot_size=4096, slots=16)
        transport = PollingTransport(ring, poll_interval=0.001)

        try:
            # Send multiple messages
            messages = [f"msg-{i}".encode() for i in range(10)]
            for msg in messages:
                await asyncio.wait_for(transport.send(msg), timeout=2.0)

            # Receive and verify order
            received = []
            for _ in range(len(messages)):
                data = await asyncio.wait_for(transport.recv(), timeout=2.0)
                received.append(data)

            assert received == messages, "Messages should be received in FIFO order"
        finally:
            transport.close()


async def test_large_message_handling() -> None:
    """Test handling of larger messages up to slot size."""
    _skip_if_async_ring_unavailable()

    from vertebra._internal.async_transport import PollingTransport
    from vertebra._vertebra import AsyncRing

    ring_name = f"/{unique_shm_name('large-msg')}"
    slot_size = 8192  # 8KB slots

    with tracked_shm_cleanup([ring_name]):
        ring = AsyncRing.create_named(ring_name, slot_size=slot_size, slots=4)
        transport = PollingTransport(ring, poll_interval=0.001)

        try:
            # Create a message that uses most of the slot
            # Leave some room for header
            large_message = b"X" * (slot_size - 256)

            await asyncio.wait_for(transport.send(large_message), timeout=2.0)
            received = await asyncio.wait_for(transport.recv(), timeout=2.0)

            assert received == large_message
            assert len(received) == len(large_message)
        finally:
            transport.close()
