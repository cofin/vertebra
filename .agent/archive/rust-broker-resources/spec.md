# Spec: Rust Broker Resources

## Code Analysis Summary
**Current State (TypeScript):**
- `base-pool.ts`: Abstract base class for pools (`acquire`, `release`, `stats`).
- `scheduler.ts`: `FairScheduler` implementing Deficit Round Robin (DRR) for fairness.
- **Problem:** These logic pieces are in TypeScript/Bun, which we are removing from the core.

**Target Architecture (Rust):**
- **`vertebra-broker` crate:** This will be the new home for resource management logic.
- **`ResourcePool` trait:** Similar to `BasePoolAdapter`, but in Rust.
- **`FairScheduler` struct:** Port of the TS implementation to Rust.
- **Scope Refinement:** NO native DB adapters implementation in this phase.
    - We will define the *interfaces* and *registration* mechanism.
    - Actual pools will be registered from Python/external processes (future) or basic placeholders.
    - The goal is the *Broker Infrastructure*, not the drivers themselves yet.

## Requirements

### 1. Resource Pool Trait (Rust)
- **Trait:** `PoolAdapter`
- **Methods:**
    - `acquire()`: Returns a resource handle (or token).
    - `release(resource)`: Returns resource to pool.
    - `stats()`: Returns pool statistics.
- **NO Native Drivers:** Do not implement `SqlPool`, `RedisPool` in Rust yet.

### 2. Fairness Scheduler (Rust)
- **Struct:** `FairScheduler<T>`
- **Algorithm:** Deficit Round Robin (DRR) to match TS implementation.
- **Features:**
    - `enqueue(client_id, item, weight)`
    - `next()`: Returns next item to process based on fairness.
    - `update_weight(client_id, weight)`

### 3. Integration with Broker
- The `BrokerServer` (in `vertebra-server`) will own the `FairScheduler` and `PoolManager`.
- IPC requests for resources will be routed through the scheduler.

## API Changes

### Rust (`vertebra-broker`)
```rust
pub trait PoolAdapter { ... }
pub struct FairScheduler<T> { ... }
```
