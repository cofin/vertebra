# Plan: Rust Broker Resources

## Phase 1: Fairness Scheduler
- [x] **Task: Port `FairScheduler` to Rust** 1dcb642
    - Create `src/rs/vertebra-broker/src/scheduler.rs`.
    - Implement DRR algorithm (Deficit Round Robin).
    - Ensure thread-safety (Arc/Mutex if needed, or lock-free).
    - Unit tests to verify fairness (port TS tests).

## Phase 2: Pool Architecture (Abstract Only)
- [x] **Task: Define `PoolAdapter` Trait** 6f2dbeb
    - Create `src/rs/vertebra-broker/src/pool.rs`.
    - Define trait for `acquire`, `release`, `stats`.
- [x] **Task: Implement `PoolManager`** 6f2dbeb
    - Manages a collection of named pools (`HashMap<String, Box<dyn PoolAdapter>>`).
    - Methods to register/unregister pools.

## Phase 3: Integration
- [x] **Task: Wire into `vertebra-server`** bc0c793
    - `VertebraServer` should initialize the `Scheduler` and `PoolManager`.
    - Incoming IPC requests for "resource.acquire" should be enqueued in the scheduler.

## Phase 4: Completion
- [x] **Task: Run Test Suite & Checkpoint**
    - `make test-all`
    - Create tag `checkpoint/rust-broker-resources/phase-4` (6cc6f7f)
- [x] **Task: Archive Flow**
    - Move to `.agent/archive/`
