# Runtime & Shared-Memory IPC

`vertebra-core` implements the transport layer between the Rust HTTP server and Python workers. It supports both single-process `embedded` mode and multi-process `shm` mode.

## Shared-Memory Layout

In `--topology shm`, the parent Rust process allocates a shared-memory region backed by `memfd_create`, `/dev/shm`, or POSIX `shm_open`. A shared header records the layout version and generation counter so workers can validate the mapping on startup.

Producer and consumer indices live on separate 128-byte cache lines to avoid false sharing across CPU cores:

- **`SpscRing`**: Single-producer, single-consumer ring buffer pairing each Python worker process with the Rust server.
- **`MpmcRing`**: Multi-producer, multi-consumer ring buffer for work-stealing request queues.
- **`TieredRingSet`**: Size-tiered response pools (`tiny`, `small`, `medium`, `large`, plus `/tmp/vtb-*` overflow files for multi-megabyte bodies) so small JSON responses do not queue behind large downloads.

## Adaptive Worker Wakeups

Continuous polling burns CPU when traffic is idle, while a syscall on every request adds latency under load. `vertebra-core` switches between spinning and sleeping:

1. **Brief Spin While Busy**: Workers spin briefly while request batches arrive back-to-back.
2. **Sleep When Idle**: Before sleeping, a worker marks itself parked and blocks on Linux `futex`, macOS `ulock`, or an `eventfd` registered with Python's `asyncio` event loop. The Rust server issues a wake syscall only when a worker is parked.

## In-Process Batch Bridge

In `--topology embedded`, `vertebra-core` skips shared memory and hands request batches directly between Rust HTTP threads and Python worker threads in process memory through `BatchBridge`.
