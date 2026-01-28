# Rust Code Removal Candidates

## Summary

- **Total files scanned**: 22 Rust source files + 4 Cargo.toml
- **Dead code items found**: 8
- **Deprecated items found**: 0
- **TODO/FIXME comments**: 2 (now fixed)
- **Clippy errors**: 58 (all fixed - C ABI unsafe pointer handling)
- **Clippy warnings**: 4 (collapsible if fixed, safety docs added)

## Cleanup Status

All cleanup items have been addressed. See the "Completion Status" section at the end.

---

## By Package

### vertebra-core

| Item | Type | Location | Reason | Status |
|------|------|----------|--------|--------|
| `region` field | Struct field | `async_ring.rs:44-45` | `#[allow(dead_code)]` - Arc<ShmRegion> kept alive but never accessed after creation | **DONE** - Documented with comment |
| `region` field | Struct field | `async_ring.rs:298` | `#[allow(dead_code)]` - Same pattern for AsyncMpmcRing | **DONE** - Documented with comment |
| `pub mod types {}` | Empty module | `lib.rs:25` | Placeholder for PRD-2+ type bridge utilities | **N/A** - Already removed (not present) |
| Collapsible if | Style | `channel.rs:1377-1388` | Nested if can be collapsed | **DONE** - Collapsed |
| Collapsible if | Style | `channel.rs:1399-1410` | Nested if can be collapsed | **DONE** - Collapsed |
| Missing safety doc | Documentation | `async_ring.rs:117` | `from_raw_fd` unsafe fn missing `# Safety` section | **DONE** - Added safety docs |
| Missing safety doc | Documentation | `async_ring.rs:375` | `from_raw_fd` for AsyncMpmcRing | **DONE** - Added safety docs |
| `#[allow(dead_code)]` | Annotation | `channel.rs:126` | SpscRing region field | **DONE** - Documented |
| `#[allow(dead_code)]` | Annotation | `channel.rs:1043` | BufferSender pool field | **DONE** - Documented |
| `#[allow(dead_code)]` | Annotation | `channel.rs:1179` | MpmcRing region field | **DONE** - Documented |

### vertebra-broker

| Item | Type | Location | Reason | Status |
|------|------|----------|--------|--------|
| `PoolAdapter` trait | Trait | `pool.rs` | Dead abstraction | **N/A** - Already removed (file not present) |
| `PoolManager` | Struct | `pool.rs` | Unused | **N/A** - Already removed |
| `FairScheduler<T>` | Struct | `scheduler.rs` | Unused | **N/A** - Already removed |
| C ABI functions | 58 functions | `lib.rs` | All `not_unsafe_ptr_arg_deref` errors | **DONE** - All marked `unsafe extern "C"` with safety docs |
| Missing Default impl | Trait impl | `pool.rs` | `PoolManager::new()` without Default | **N/A** - PoolManager removed |

### vertebra-py

| Item | Type | Location | Reason | Status |
|------|------|----------|--------|--------|
| `start_dev_server` TODO | Incomplete | `supervisor.rs` | "TODO: Track this process for cleanup" | **DONE** - Added `dev_server` field and tracking |
| `_child` handle | Lost handle | `supervisor.rs` | "TODO: Store child handle" | **DONE** - Child handle now stored and cleaned up |
| `#[allow(deprecated)]` | Annotation | `async_ring.rs` | Used for `py.allow_threads()` | **Keep** - Required for PyO3 compatibility |

### vertebra-server

| Item | Type | Location | Reason | Status |
|------|------|----------|--------|--------|
| `RoundRobinDispatcher` | Struct | `dispatcher.rs` | Dead code | **N/A** - Already removed (not in dispatcher.rs) |
| `pool_manager` field | Field | `lib.rs` | ServerState field | **N/A** - Already removed (not in ServerState) |
| `scheduler` field | Field | `lib.rs` | ServerState field | **N/A** - Already removed |

---

## Dependency Analysis

### Potentially Unused Dependencies

| Package | Dependency | Status | Action |
|---------|-----------|--------|--------|
| vertebra-broker | `async-trait` | Not present | **N/A** - Already removed |
| workspace | `criterion` | Used in benches | **Keep** |
| workspace | `proptest` | Used in tests | **Keep** |
| workspace | `arrow` | Exported but not actively used in current IPC | **Review** - May be for future Arrow IPC |

### Feature Flags

No conditional feature flags found that are always on/off.

---

## Completion Status

### High Priority (Release Blockers) - ALL DONE

1. **Fix C ABI unsafe pointer handling** (58 clippy errors in vertebra-broker)
   - **DONE**: All C ABI functions now marked `unsafe extern "C"` with proper safety documentation
   - Functions include: `vtb_router_*`, `vtb_ring_*`, `vtb_region_*`, `vtb_mpmc_*`, `vtb_notifier_*`, `vtb_async_bridge_*`

2. **Remove unused broker abstractions**
   - **N/A**: `PoolAdapter`, `PoolManager`, `FairScheduler` were already removed in previous cleanup

3. **Remove `RoundRobinDispatcher`**
   - **N/A**: Already removed in previous cleanup

### Medium Priority (Tech Debt) - ALL DONE

4. **Fix supervisor TODOs**
   - **DONE**: Added `dev_server: Arc<Mutex<Option<Child>>>` field to `NativeSupervisor`
   - **DONE**: Dev server process now tracked and cleaned up in `stop_internal()`

5. **Clean up ServerState**
   - **N/A**: `pool_manager` and `scheduler` fields were already removed

6. **Fix clippy warnings**
   - **DONE**: Collapsible if statements in `channel.rs` collapsed
   - **DONE**: Safety documentation added for `from_raw_fd` in `async_ring.rs` (both SPSC and MPMC)
   - **DONE**: `#[allow(dead_code)]` annotations documented with explanatory comments

### Low Priority (Nice to Have) - ALL DONE

7. **Remove empty `types` module** in vertebra-core
   - **N/A**: Already removed (not present in `lib.rs`)

8. **Review `#[allow(dead_code)]` annotations**
   - **DONE**: All reviewed and documented with comments explaining why each is intentionally kept:
     - `region` fields in ring structs: Arc kept alive for memory lifetime
     - `pool` field in BufferSender: Arc kept for pool lifetime symmetry

---

## Verification

All changes verified with:
```bash
cd src/rs && cargo clippy --all-targets --all-features  # No errors, only minor warnings
cd src/rs && cargo test --all                           # 82 tests passed
```

Remaining minor clippy warnings (not in scope of this cleanup):
- `vertebra-server`: `PathBuf` vs `Path`, empty slice comparison, needless returns, missing Default for Router
- `vertebra-py`: Unnecessary borrow in `Signals::new()`, unnecessary `Ok()?` pattern

---

## Code Size Impact

| Action | Estimated Lines Changed |
|--------|------------------------|
| C ABI safety fixes | ~60 function signatures updated |
| Supervisor tracking | ~15 lines added |
| Safety docs added | ~20 lines added |
| Collapsible if fixed | ~4 lines simplified |
| Dead code comments | ~10 lines added |
| **Net change** | **~100 lines added** (mostly documentation) |

---

## Related Beads Issues

All issues resolved:
- Dead broker abstractions removal - N/A (already done)
- C ABI safety fixes - DONE
- Supervisor TODO resolution - DONE
