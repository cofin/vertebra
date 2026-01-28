# Rust Code Removal Candidates

## Summary

- **Total files scanned**: 22 Rust source files + 4 Cargo.toml
- **Dead code items found**: 8
- **Deprecated items found**: 0
- **TODO/FIXME comments**: 2
- **Clippy errors**: 58 (all in vertebra-broker C ABI unsafe pointer handling)
- **Clippy warnings**: 4

## Audit Methodology

1. `cargo clippy --all-targets --all-features -- -W dead_code -W unused -W deprecated`
2. Grep for `#[allow(dead_code)]`, `#[deprecated]`, `TODO|FIXME|HACK`
3. Manual code review for unused structs, unreachable code paths, and unused dependencies

---

## By Package

### vertebra-core

| Item | Type | Location | Reason | Action |
|------|------|----------|--------|--------|
| `region` field | Struct field | `async_ring.rs:44-45` | `#[allow(dead_code)]` - Arc<ShmRegion> kept alive but never accessed after creation | **Keep** - Intentional lifetime extension |
| `region` field | Struct field | `async_ring.rs:298` | `#[allow(dead_code)]` - Same pattern for AsyncMpmcRing | **Keep** - Intentional lifetime extension |
| `pub mod types {}` | Empty module | `lib.rs:25` | Placeholder for PRD-2+ type bridge utilities | **Remove** - Empty placeholder adds confusion |
| Collapsible if | Style | `channel.rs:1294-1305` | Nested if can be collapsed | **Fix** - `cargo clippy --fix` |
| Collapsible if | Style | `channel.rs:1316-1327` | Nested if can be collapsed | **Fix** - `cargo clippy --fix` |
| Missing safety doc | Documentation | `async_ring.rs:358` | `from_raw_fd` unsafe fn missing `# Safety` section | **Fix** - Add safety documentation |
| `#[allow(dead_code)]` | Annotation | `channel.rs:126` | SlotMeta marker_start field | **Review** - May be for future alignment |
| `#[allow(dead_code)]` | Annotation | `channel.rs:958` | MpmcSlotMeta marker_start | **Review** - May be for future alignment |
| `#[allow(dead_code)]` | Annotation | `channel.rs:1092` | MpmcRing epoch field | **Review** - May be for future use |

### vertebra-broker

| Item | Type | Location | Reason | Action |
|------|------|----------|--------|--------|
| `PoolAdapter` trait | Trait | `pool.rs:13-23` | Only used in test, no implementations | **Remove** - Dead abstraction, broker moved to TypeScript |
| `PoolManager` | Struct | `pool.rs:25-50` | Created in server but never used (no register/get/unregister calls) | **Remove** - Unused; broker functionality in TS |
| `FairScheduler<T>` | Struct | `scheduler.rs:21-90` | Created in server but never used (no enqueue/next calls outside tests) | **Remove** - Unused; scheduling handled elsewhere |
| C ABI functions | 58 functions | `lib.rs` | All `not_unsafe_ptr_arg_deref` errors | **Fix** - Mark functions `unsafe` or use safe wrappers |
| Missing Default impl | Trait impl | `pool.rs:30` | `PoolManager::new()` without Default | **Fix** - Add `impl Default` (if keeping PoolManager) |

### vertebra-py

| Item | Type | Location | Reason | Action |
|------|------|----------|--------|--------|
| `start_dev_server` TODO | Incomplete | `supervisor.rs:336-337` | "TODO: Track this process for cleanup in stop_internal" | **Fix** - Either implement or remove dev server tracking |
| `_child` handle | Lost handle | `supervisor.rs:356` | "TODO: Store child handle" - spawned process not tracked | **Fix** - Store handle or document limitation |
| `#[allow(deprecated)]` | Annotation | `async_ring.rs` | Used for `py.allow_threads()` | **Keep** - Required for PyO3 compatibility |

### vertebra-server

| Item | Type | Location | Reason | Action |
|------|------|----------|--------|--------|
| `RoundRobinDispatcher` | Struct | `dispatcher.rs:12-42` | Defined but never instantiated (only `LeastPendingDispatcher` used) | **Remove** - Dead code, superseded by LeastPendingDispatcher |
| `pool_manager` field | Field | `lib.rs:120` | ServerState holds PoolManager but never uses it | **Remove** - Along with PoolManager removal |
| `scheduler` field | Field | `lib.rs:121` | ServerState holds FairScheduler but never uses it | **Remove** - Along with FairScheduler removal |

---

## Dependency Analysis

### Potentially Unused Dependencies

| Package | Dependency | Status | Action |
|---------|-----------|--------|--------|
| vertebra-broker | `async-trait` | Used by PoolAdapter | **Remove** if PoolAdapter removed |
| workspace | `criterion` | Used in benches | **Keep** |
| workspace | `proptest` | Used in tests | **Keep** |
| workspace | `arrow` | Exported but not actively used in current IPC | **Review** - May be for future Arrow IPC |

### Feature Flags

No conditional feature flags found that are always on/off.

---

## Recommended Cleanup Priority

### High Priority (Release Blockers)

1. **Fix C ABI unsafe pointer handling** (58 clippy errors in vertebra-broker)
   - Either mark all C ABI functions as `unsafe extern "C"`
   - Or wrap pointer derefs in proper null checks before unsafe blocks

2. **Remove unused broker abstractions**
   - `PoolAdapter`, `PoolManager`, `FairScheduler`
   - These were designed for a different architecture; broker is now in TypeScript

3. **Remove `RoundRobinDispatcher`**
   - Dead code, `LeastPendingDispatcher` is the active implementation

### Medium Priority (Tech Debt)

4. **Fix supervisor TODOs**
   - Either implement dev server process tracking or document as known limitation
   - Affects graceful shutdown of `start_dev_server`

5. **Clean up ServerState**
   - Remove `pool_manager` and `scheduler` fields after their types are removed

6. **Fix clippy warnings**
   - Collapsible if statements in channel.rs
   - Add safety documentation for `from_raw_fd`
   - Add `Default` impl for types with `new()`

### Low Priority (Nice to Have)

7. **Remove empty `types` module** in vertebra-core
8. **Review `#[allow(dead_code)]` annotations** - some may hide actual dead code

---

## Code Size Impact

| Action | Estimated Lines Removed |
|--------|------------------------|
| Remove PoolAdapter/PoolManager | ~50 lines |
| Remove FairScheduler | ~120 lines |
| Remove RoundRobinDispatcher | ~30 lines |
| Remove ServerState unused fields | ~10 lines |
| **Total** | **~210 lines** |

---

## Test Coverage Notes

- `FairScheduler` has test coverage but the struct itself is unused in production
- `PoolAdapter` has no implementations, making its trait useless
- `RoundRobinDispatcher` has no tests (further evidence it's dead code)

---

## Related Beads Issues

See beads issues created for tracking this cleanup work:
- Dead broker abstractions removal
- C ABI safety fixes
- Supervisor TODO resolution
