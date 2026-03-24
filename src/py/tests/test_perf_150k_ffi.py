
import pytest
from vertebra._vertebra import VertebraResponse, submit_response, init_transport, get_batch
from vertebra.server._native import CallbackScheduler, _PartitionedBatch, _dispatch_batch
import time
import asyncio

def test_submit_response_exists():
    init_transport()
    resp = VertebraResponse(status=200, body=b"hello", request_id=123)
    submit_response(resp)

def test_non_blocking_dispatch():
    init_transport() # in-process mode
    scheduler = CallbackScheduler()
    
    async def slow_handler():
        await asyncio.sleep(0.1)
        return b"slow"
        
    class Req:
        def __init__(self, rid):
            self.request_id = rid
            self.handler_id = 1
            
    batch = [Req(1), Req(2)]
    partitioned = _PartitionedBatch()
    # Mocking what _partition_batch does
    partitioned.async_native = [(0, batch[0], slow_handler), (1, batch[1], slow_handler)]
    
    start = time.monotonic()
    # async_mode=True should return immediately
    _dispatch_batch(partitioned, {1: slow_handler}, [], VertebraResponse, scheduler, async_mode=True)
    elapsed = time.monotonic() - start
    
    assert elapsed < 0.05 # Should have returned immediately
    
    # Clean up
    scheduler.stop()
