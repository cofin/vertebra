# Implementation Tasks: Bun Broker Pool Manager

## Phase 1: Skeleton & Configuration
- [ ] Define `PoolConfig` data class (pydantic/dataclass). <!-- id: 1 -->
- [ ] Create `PoolManager` class stub. <!-- id: 2 -->
- [ ] Define `Connection` abstract interface (or mock) for testing. <!-- id: 3 -->
- [ ] Implement `start()` and `stop()` (basic state management). <!-- id: 4 -->

## Phase 2: Core Lifecycle (Single Threaded Logic)
- [ ] Implement `create_connection()` logic. <!-- id: 5 -->
- [ ] Implement `close_connection()` logic. <!-- id: 6 -->
- [ ] Maintain `idle` and `active` collections. <!-- id: 7 -->

## Phase 3: Async Concurrency & Pooling
- [ ] Implement `acquire()` with `asyncio.Condition` or `Semaphore` for waiting. <!-- id: 8 -->
- [ ] Implement `release()` with notification to waiters. <!-- id: 9 -->
- [ ] Add timeout logic to `acquire()`. <!-- id: 10 -->
- [ ] Verify `max_size` constraints are respected under load. <!-- id: 11 -->

## Phase 4: Health & Maintenance
- [ ] Implement `validate()` method (check if connection is open). <!-- id: 12 -->
- [ ] Add background task `_maintenance_loop` for `idle_timeout` eviction. <!-- id: 13 -->
- [ ] Add `max_lifetime` eviction. <!-- id: 14 -->

## Phase 5: Observability & Integration
- [ ] Expose `stats()` method. <!-- id: 15 -->
- [ ] Add structured logging for state changes. <!-- id: 16 -->
- [ ] Create integration test simulating `FairScheduler` load. <!-- id: 17 -->
