# Runtime & Shared-Memory IPC (`vertebra-core`)

`vertebra-core` implements the transport layer that connects the Rust HTTP server to Python workers in both single-process (`embedded`) and multi-process (`shm`) modes.

## Shared-Memory Layout (`ShmHeader` & Rings)

When running in `--topology shm`, the parent Rust process creates a shared-memory region (backed by `memfd_create`, `/dev/shm`, or POSIX `shm_open`) validated by a header, version, and generation counter (`ShmHeader`).

To prevent CPU cache contention between threads, producer and consumer pointers sit on separate **128-byte cache lines**:

- **`SpscRing`**: Single-producer/single-consumer ring buffer pairing each Python worker process with the Rust HTTP server.
- **`MpmcRing`**: Multi-producer/multi-consumer ring buffer used for work-stealing request queues.
- **`TieredRingSet`**: Size-tiered response slot pools (`tiny`, `small`, `medium`, `large`, plus `/tmp/vtb-*` overflow files for multi-megabyte payloads) so small JSON responses never wait behind large file downloads.

## Adaptive Worker Wakeups (`futex`, `ulock` & `eventfd`)

Polling shared memory non-stop wastes CPU when idle, while making a kernel syscall on every request adds latency under load. `vertebra-core` uses a two-step wakeup strategy:

1. **Brief Spin While Busy**: Workers spin briefly while request batches are arriving back-to-back.
2. **Sleep When Idle**: Before sleeping, a worker marks itself parked (`prepare_request_sleep`) and waits on Linux `futex`, macOS `ulock`, or an `eventfd` registered with Python's `asyncio` event loop (`get_request_notify_fd`). The Rust server only makes a wake syscall when a worker is actually asleep.

## In-Process `BatchBridge`

In `--topology embedded`, `vertebra-core` skips shared memory and passes `RequestBatch` pointers directly between Rust HTTP threads and Python worker threads in process memory via `BatchBridge`.
