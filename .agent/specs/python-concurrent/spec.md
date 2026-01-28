# Spec: python-concurrent

**Flow ID:** `python-concurrent`
**Epic:** `vertebra-k4k.4`
**Status:** Planning
**Parent PRD:** async-ipc-pipelining

## Summary

Enable concurrent request processing in Python workers using asyncio. Process multiple in-flight requests simultaneously rather than sequentially.

## Code Analysis Summary

### Current State

**`src/py/vertebra/core/worker.py`:**
The worker processes requests sequentially:
```python
async def run(self):
    while True:
        # Wait for request notification
        await self._wait_for_request()

        # Process ONE request at a time
        msg = self._req_rx.try_recv()
        if msg:
            await self._handle_request(msg)  # Blocks until ASGI app completes
```

### Problem

Even with Rust-side pipelining (Chapters 1-3), Python processes requests one at a time:
- Request 1 arrives, Python starts processing
- Requests 2, 3, 4 arrive, sit in ring buffer
- Request 1 completes, Python reads request 2
- etc.

This limits throughput to Python's single-request processing time.

### Target State

Python processes multiple requests concurrently via asyncio:
```python
async def run(self):
    while True:
        # Wait for any request
        await self._wait_for_request()

        # Spawn ALL available requests as concurrent tasks
        while (msg := self._req_rx.try_recv()) is not None:
            asyncio.create_task(self._handle_request(msg))
```

## Requirements

### R1: Concurrent Task Spawning
Spawn each request as an independent asyncio task:
```python
async def _on_requests_available(self):
    self._clear_request_notification()

    while (msg := self._req_rx.try_recv()) is not None:
        task = asyncio.create_task(self._handle_request(msg))
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
```

### R2: Request ID Tracking Per-Task
Each task needs its own request ID (from Chapter 2):
```python
async def _handle_request(self, msg: bytes):
    # Extract request ID from header
    req_id = struct.unpack('<Q', msg[:8])[0]

    # Process ASGI with this request ID
    writer = ResponseWriter(req_id=req_id, ...)
    await self._app(scope, receive, writer.send)
```

### R3: Response Ordering via Correlation
Responses can complete in any order - the Rust router handles correlation:
```python
# Response includes its request ID
def _send_response(self, req_id: int, payload: bytes):
    header = struct.pack('<QII', req_id, 0, len(payload))
    self._resp_tx.send(header + payload)
    self._notify_rust()
```

### R4: Concurrency Limit
Prevent unbounded task spawning:
```python
MAX_CONCURRENT_REQUESTS = 100

async def _on_requests_available(self):
    while len(self._active_tasks) < MAX_CONCURRENT_REQUESTS:
        msg = self._req_rx.try_recv()
        if msg is None:
            break
        # ... spawn task ...
```

### R5: Graceful Shutdown
Wait for in-flight tasks during shutdown:
```python
async def shutdown(self):
    # Stop accepting new requests
    self._shutting_down = True

    # Wait for active tasks with timeout
    if self._active_tasks:
        done, pending = await asyncio.wait(
            self._active_tasks,
            timeout=30.0
        )
        for task in pending:
            task.cancel()
```

### R6: Task-Local ASGI Context
Each concurrent request needs isolated ASGI context:
```python
class RequestContext:
    def __init__(self, req_id: int, scope: dict):
        self.req_id = req_id
        self.scope = scope
        self.response_started = False
        self.response_body = bytearray()
```

## Non-Goals

- Multi-threaded Python (GIL prevents true parallelism)
- Multiprocessing within single worker (that's what multiple workers are for)

## Dependencies

- Chapter 2 (`correlation-protocol`) - for request ID in headers

## Risks

1. **Memory growth** - Many concurrent requests = many in-memory contexts
2. **CPU starvation** - Too many CPU-bound tasks block the event loop
3. **ASGI app compatibility** - Some apps may not be truly async

## Acceptance Criteria

- [ ] Worker spawns asyncio tasks for each incoming request
- [ ] Multiple requests processed concurrently (verified via logging)
- [ ] Request IDs correctly tracked per-task
- [ ] Responses correctly correlated (no cross-talk)
- [ ] Concurrency limit prevents resource exhaustion
- [ ] Graceful shutdown waits for in-flight requests
- [ ] Benchmark shows improved throughput vs sequential processing
