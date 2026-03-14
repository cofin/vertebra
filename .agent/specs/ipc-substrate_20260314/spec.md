# Flow Spec: Native IPC Substrate
## Goal
Implement high-performance shared-memory queues and arena allocators.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Shared Memory
- [ ] Task 0: Open GitHub issue for "Shared Memory Substrate and Ring Buffers".
- [ ] Task 1: Implement `src/rs/vertebra-core/src/shm.rs`. Add POSIX `shm_open` and `mmap` wrappers.
- [ ] Task 2: Implement cleanup logic to prevent `/dev/shm` leakage.

### Phase 2: Ring Buffers
- [ ] Task 3: Implement `src/rs/vertebra-core/src/ring.rs`. Add lock-free SPSC and MPMC rings.
- [ ] Task 4: Implement Futex-based notification signaling.

### Phase 3: Arenas & Proxies
- [ ] Task 5: Implement `src/rs/vertebra-core/src/batch.rs`. Add `BatchNode` with Arena allocator.
- [ ] Task 6: Implement `src/rs/vertebra-core/src/proxy.rs`. Define C-ABI `RequestInfo` and `ResponseInfo`.
