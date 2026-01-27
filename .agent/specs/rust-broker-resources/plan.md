# Plan: Rust Broker Resources

## Phase 1: Fairness Scheduler
- [ ] **Task: Port `FairScheduler` to Rust**
    - Create `src/rs/vertebra-broker/src/scheduler.rs`.
    - Implement DRR algorithm (Deficit Round Robin).
    - Ensure thread-safety (Arc/Mutex if needed, or lock-free).
    - Unit tests to verify fairness (port TS tests).

## Phase 2: Pool Architecture (Abstract Only)
- [ ] **Task: Define `PoolAdapter` Trait**
    - Create `src/rs/vertebra-broker/src/pool.rs`.
    - Define trait for `acquire`, `release`, `stats`.
- [ ] **Task: Implement `PoolManager`**
    - Manages a collection of named pools (`HashMap<String, Box<dyn PoolAdapter>>`).
    - Methods to register/unregister pools.

## Phase 3: Integration
- [ ] **Task: Wire into `vertebra-server`**
    - `VertebraServer` should initialize the `Scheduler` and `PoolManager`.
    - Incoming IPC requests for "resource.acquire" should be enqueued in the scheduler.