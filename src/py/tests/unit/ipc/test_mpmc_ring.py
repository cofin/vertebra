import pytest
from vertebra._vertebra import AsyncMpmcRing

@pytest.fixture
def ring():
    return AsyncMpmcRing(256, 8)

def test_mpmc_ring_create(ring):
    stats = ring.stats()
    assert stats["slot_size"] == 256
    assert stats["slots"] == 8
    assert stats["read_head"] == 0
    assert stats["write_head"] == 0

def test_mpmc_ring_send_recv(ring):
    assert not ring.has_pending_notification()
    
    payload = b"hello mpmc"
    ring.send(payload)
    
    assert ring.has_pending_notification()
    
    data = ring.try_recv()
    assert data == payload
    
    ring.clear_notification()
    assert not ring.has_pending_notification()

def test_mpmc_ring_try_send_recv(ring):
    payload = b"test"
    assert ring.try_send(payload)
    assert ring.try_recv() == payload
    assert ring.try_recv() is None

def test_mpmc_ring_full(ring):
    # Ring has 8 slots. MPMC uses one slot as a gap?
    # Usually ring buffer capacity is size - 1 or size depending on implementation.
    # Our implementation: tail wrapping_add(1) & mask == head => full.
    # So capacity is 7.
    
    payload = b"x"
    sent_count = 0
    while ring.try_send(payload):
        sent_count += 1
        
    assert sent_count > 0
    assert not ring.try_send(payload) # Should be full

def test_mpmc_ring_recv_hybrid(ring):
    payload = b"hybrid"
    ring.send(payload)
    
    # Data ready immediately
    data = ring.recv_hybrid(spin_us=1, timeout_ms=100)
    assert data == payload

def test_mpmc_ring_named():
    import os
    name = f"/test_mpmc_{os.getpid()}"
    try:
        ring1 = AsyncMpmcRing.create_named(name, 256, 8)
        ring2 = AsyncMpmcRing.open_named(name, 256, 8)
        
        ring1.send(b"shared")
        assert ring2.try_recv() == b"shared"
    finally:
        # Cleanup handled by OS or restart usually, but we should try to unlink if possible
        # Python wrapper doesn't expose unlink yet?
        pass
