# Runtime & Shared-Memory IPC (`vertebra-core`)

`vertebra-core` implements the zero-copy transport primitives that connect the Rust HTTP ingress to Python workers in both single-process (`embedded`) and multi-process (`shm`) topologies.

## Shared-Memory Layout (`ShmHeader` & Rings)

When running in `--topology shm`, the parent Rust process allocates a shared-memory region (backed by `/dev/shm` on Linux or POSIX `shm_open`) validated by a magic header, version, and generation counter (`ShmHeader`).

To eliminate false sharing across CPU cores, producer head and consumer tail indices are aligned to **128-byte cache-line boundaries**:

- **`SpscRing`**: Lock-free single-producer/single-consumer ring buffer pairing each Python worker process with the Rust ingress dispatcher using `Acquire`/`Release` atomic ordering.
- **`MpmcRing`**: Bounded multi-producer/multi-consumer ring using Vyukov sequence numbers for work-stealing request queues.
- **`TieredRingSet`**: Size-segregated response slot pools (`tiny`, `small`, `medium`, `large`, plus `/tmp/vtb-*` overflow files for multi-megabyte payloads) so small JSON responses never contend with large file transfers.

## Adaptive `futex` / `ulock` & `eventfd` Signaling

Polling shared memory continuously wastes CPU when idle, while issuing a kernel syscall on every request adds microsecond latency under load. `vertebra-core` uses an adaptive two-phase wakeup protocol:

1. **Active Spin Window**: Consumers spin briefly with `core::hint::spin_loop()` while batches are arriving back-to-back.
2. **Parked Kernel Wait**: Before sleeping, a worker sets the atomic parked flag (`SeqCst` handshake via `prepare_request_sleep`) and waits on Linux `FUTEX_WAIT_BITSET`, macOS `__ulock_wait`, or an `eventfd` registered with the Python `asyncio` selector (`get_request_notify_fd`). Producers only invoke a kernel wake syscall when the consumer's parked flag is observed set.

## Embedded `BatchBridge`

In `--topology embedded` (the default), `vertebra-core` bypasses POSIX shared memory and transfers `RequestBatch` ownership directly between Tokio worker threads and Python worker threads in process memory via `BatchBridge`.
