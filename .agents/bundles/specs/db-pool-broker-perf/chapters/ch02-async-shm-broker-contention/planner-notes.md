# Planner Notes — ch02-async-shm-broker-contention

## Files Written

- `chapters/ch02-async-shm-broker-contention/spec.md`
- `chapters/ch02-async-shm-broker-contention/tasks/t04-rust-broker-single-wake-and-async-wait.md`
- `chapters/ch02-async-shm-broker-contention/tasks/t05-async-broker-local-handoff-and-leader.md`
- `chapters/ch02-async-shm-broker-contention/tasks/t06-async-broker-contention-tests.md`

## Task Summaries

- `t04`: Add `wake_one_waiter_seqcst` (`wake_one` when `partition_count <= 1`, else `wake_all`), `ShmLeaseTable::{wake_seq, wait_for_wake}`, and PyO3 `LeaseBroker::{wake_seq, wait_for_wake_async}` with `_vertebra.pyi` stubs.
- `t05`: Replace `SyncPool` waiting in `AsyncSHMBrokerTransport.acquire` with per-loop local handoff deques, single-leader `wait_for_wake_async` watching, and hot-path metadata caching.
- `t06`: Add `c=128` zero-`SyncPool`, leader cancellation, `release_sync`, cross-process SHM, and multi-partition fairness regression tests.

## Root Spec Issues

None

## Open Questions

None
