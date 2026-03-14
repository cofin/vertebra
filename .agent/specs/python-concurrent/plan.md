# Plan: python-concurrent

**Flow ID:** `python-concurrent`
**Epic:** `vertebra-k4k.4`

## Phase 1: Request Context Isolation

### Task 1.1: Create RequestContext class
**File:** `src/py/vertebra/core/worker.py`

```python
@dataclass
class RequestContext:
    """Isolated context for a single ASGI request."""
    req_id: int
    scope: dict
    response_started: bool = False
    status: int = 200
    headers: list[tuple[bytes, bytes]] = field(default_factory=list)
    body: bytearray = field(default_factory=bytearray)
```

### Task 1.2: Update ResponseWriter to use context
**File:** `src/py/vertebra/core/worker.py`

```python
class ResponseWriter:
    def __init__(self, ctx: RequestContext, resp_tx, resp_notify_fd: int):
        self._ctx = ctx
        self._resp_tx = resp_tx
        self._resp_notify_fd = resp_notify_fd

    async def send(self, message: dict) -> None:
        msg_type = message["type"]
        if msg_type == "http.response.start":
            self._ctx.response_started = True
            self._ctx.status = message["status"]
            self._ctx.headers = message.get("headers", [])
        elif msg_type == "http.response.body":
            self._ctx.body.extend(message.get("body", b""))
            if not message.get("more_body", False):
                self._send_response()

    def _send_response(self):
        payload = self._build_msgpack_response()
        header = struct.pack('<QII', self._ctx.req_id, 0, len(payload))
        self._resp_tx.send(header + payload)
        os.write(self._resp_notify_fd, b'\x01\x00\x00\x00\x00\x00\x00\x00')
```

## Phase 2: Concurrent Task Spawning

### Task 2.1: Add active task tracking
**File:** `src/py/vertebra/core/worker.py`

```python
class Worker:
    def __init__(self, ...):
        # ... existing init ...
        self._active_tasks: set[asyncio.Task] = set()
        self._max_concurrent = int(os.environ.get("VERTEBRA_MAX_CONCURRENT", "100"))
```

### Task 2.2: Refactor main loop for concurrent dispatch
**File:** `src/py/vertebra/core/worker.py`

```python
async def run(self):
    loop = asyncio.get_event_loop()

    # Register for request notifications
    request_ready = asyncio.Event()

    def on_request_readable():
        os.read(self._req_notify_fd, 8)  # Clear notification
        request_ready.set()

    loop.add_reader(self._req_notify_fd, on_request_readable)

    try:
        while not self._shutting_down:
            await request_ready.wait()
            request_ready.clear()

            # Spawn tasks for all available requests (up to limit)
            self._spawn_pending_requests()
    finally:
        loop.remove_reader(self._req_notify_fd)
        await self._shutdown()

def _spawn_pending_requests(self):
    """Spawn tasks for all pending requests up to concurrency limit."""
    while len(self._active_tasks) < self._max_concurrent:
        msg = self._req_rx.try_recv()
        if msg is None:
            break

        # Parse request ID from header
        req_id = struct.unpack('<Q', msg[:8])[0]

        # Create and track task
        task = asyncio.create_task(
            self._handle_request(req_id, msg),
            name=f"request-{req_id}"
        )
        self._active_tasks.add(task)
        task.add_done_callback(self._on_task_complete)

def _on_task_complete(self, task: asyncio.Task):
    self._active_tasks.discard(task)
    if task.exception():
        # Log error but don't crash worker
        logger.error(f"Request failed: {task.exception()}")
```

### Task 2.3: Update _handle_request for concurrent context
**File:** `src/py/vertebra/core/worker.py`

```python
async def _handle_request(self, req_id: int, msg: bytes):
    """Handle a single request with isolated context."""
    try:
        # Parse header and payload
        _, flags, payload_len = struct.unpack('<QII', msg[:16])
        payload = msg[16:16 + payload_len]
        asgi_req = msgspec.msgpack.decode(payload, type=AsgiRequest)

        # Build ASGI scope
        scope = self._build_scope(asgi_req)

        # Create isolated context
        ctx = RequestContext(req_id=req_id, scope=scope)

        # Create writer with this context
        writer = ResponseWriter(ctx, self._resp_tx, self._resp_notify_fd)

        # Create receive callable
        async def receive():
            return {"type": "http.request", "body": asgi_req.body, "more_body": False}

        # Invoke ASGI app
        await self._app(scope, receive, writer.send)

    except Exception as e:
        logger.exception(f"Request {req_id} failed: {e}")
        # Send error response
        self._send_error_response(req_id, 500, str(e))
```

## Phase 3: Error Handling

### Task 3.1: Implement error response
**File:** `src/py/vertebra/core/worker.py`

```python
def _send_error_response(self, req_id: int, status: int, message: str):
    """Send an error response for a failed request."""
    response = AsgiResponse(
        status=status,
        headers=[("content-type", "text/plain")],
        body=message.encode("utf-8")
    )
    payload = msgspec.msgpack.encode(response)
    header = struct.pack('<QII', req_id, 0, len(payload))
    self._resp_tx.send(header + payload)
    os.write(self._resp_notify_fd, b'\x01\x00\x00\x00\x00\x00\x00\x00')
```

### Task 3.2: Handle ASGI app exceptions
**File:** `src/py/vertebra/core/worker.py`

Wrap ASGI call in try/except (already shown in Task 2.3).

## Phase 4: Graceful Shutdown

### Task 4.1: Implement shutdown sequence
**File:** `src/py/vertebra/core/worker.py`

```python
async def _shutdown(self):
    """Gracefully shutdown, waiting for in-flight requests."""
    self._shutting_down = True
    logger.info(f"Shutting down with {len(self._active_tasks)} active requests")

    if self._active_tasks:
        # Wait up to 30 seconds for tasks to complete
        done, pending = await asyncio.wait(
            self._active_tasks,
            timeout=30.0
        )

        if pending:
            logger.warning(f"Cancelling {len(pending)} pending tasks")
            for task in pending:
                task.cancel()

            # Wait for cancellation to complete
            await asyncio.gather(*pending, return_exceptions=True)

    logger.info("Shutdown complete")
```

### Task 4.2: Handle SIGTERM/SIGINT
**File:** `src/py/vertebra/core/worker.py`

```python
async def run(self):
    loop = asyncio.get_event_loop()

    # Register signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(self._shutdown()))

    # ... rest of run() ...
```

## Phase 5: Thread Safety for Response TX

### Task 5.1: Add lock for response sending
**File:** `src/py/vertebra/core/worker.py`

Since multiple tasks may complete simultaneously:
```python
class Worker:
    def __init__(self, ...):
        # ... existing ...
        self._resp_lock = asyncio.Lock()

class ResponseWriter:
    def _send_response(self):
        # Acquire lock for thread-safe send
        # Note: asyncio.Lock is reentrant-safe within single thread
        payload = self._build_msgpack_response()
        header = struct.pack('<QII', self._ctx.req_id, 0, len(payload))

        # Lock protects the ring buffer send
        with self._worker._resp_lock:
            self._resp_tx.send(header + payload)
            os.write(self._resp_notify_fd, b'\x01\x00\x00\x00\x00\x00\x00\x00')
```

Wait - `asyncio.Lock` is not a context manager for sync code. Need to use `threading.Lock`:
```python
self._resp_lock = threading.Lock()  # For sync send operation
```

## Task Dependencies

```
1.1 ─> 1.2 ─> 2.1 ─> 2.2 ─> 2.3 ─> 3.1 ─> 3.2 ─> 4.1 ─> 4.2 ─> 5.1
```

Also depends on Chapter 2 completion (correlation-protocol) for request ID handling.

## Estimated Complexity

- **Phase 1:** Low - Data class, writer refactor
- **Phase 2:** Medium - Main loop restructure, task spawning
- **Phase 3:** Low - Error handling
- **Phase 4:** Medium - Graceful shutdown with signal handling
- **Phase 5:** Low - Thread safety for sends

## Notes

- Python GIL means only one Python bytecode executes at a time
- But asyncio tasks can interleave during I/O waits
- This benefits apps that do async I/O (database, HTTP clients)
- CPU-bound apps won't see improvement (use more workers instead)
- Ring buffer send needs sync lock since SpscSender isn't thread-safe
