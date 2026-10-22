# Memory Ordering and Concurrency Analysis

This guide provides deep technical rules for reviewing atomic orderings, futex synchronization, and memory barriers in `vertebra-core`.

---

## Store-Load Ordering on Futex Wait Words

When synchronizing cross-process rings via Linux `futex` or macOS `__ulock_wait`:

1. **Publish Cursor**:
   - The producer publishes written data by advancing the head cursor with `Ordering::Release`.
2. **Unconditional SeqCst Wait Word Check**:
   - The producer immediately performs an unconditional `SeqCst` read-modify-write or swap on the wait word.
3. **SeqCst Waiter Load**:
   - The producer performs a `SeqCst` load of the `send_waiters` or `recv_waiters` counter.
   - If waiters > 0, it calls `futex_wake`.

Weakenings from `SeqCst` to `Acquire`/`Release` allow store-load reordering where the consumer checks the cursor before entering the futex, while the producer checks the waiter count before the consumer records its waiter flag, causing missed wakeups and deadlocks.
