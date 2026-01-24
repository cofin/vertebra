# Recovery Plan: Bun Broker Pool Manager

## Rollback Strategy
- If `PoolManager` introduces instability or deadlocks, revert `FairScheduler` to use direct connections (if that was the previous state) or the previous simpler pool implementation.
- This feature is additive; disabling the `PoolManager` (or setting `max_size` to infinite/pass-through) should return to baseline behavior.

## Known Risks
- **Deadlocks:** If `acquire()` waits for `release()` but `release()` is blocked or never called (leak), the system halts.
    - *Mitigation:* Strict `finally` blocks for release. `acquire` timeout is mandatory.
- **Thundering Herd:** Many waiters waking up for one connection.
    - *Mitigation:* `notify(1)` instead of `notify_all()` on release.
- **Zombie Connections:** Connections broken by DB but thought alive by Pool.
    - *Mitigation:* Aggressive validation on acquire or background pings.

## Debugging
- Enable `DEBUG` logs for `bun.broker.pool`.
- Monitor `pool.wait_count`. If high and stable, we have a leak or under-provisioning.
