# Learnings - AsyncNotifierBridge

## Patterns Discovered

### 1. Self-Pipe Trick for Thread Shutdown
**Context:** Need to wake a thread blocked on poll() instantly without timeout polling.
**Pattern:** Use a dedicated eventfd as a second poll target. Write to it from stop() to wake the thread immediately.
**Benefit:** 0% CPU idle (infinite timeout) + instant shutdown (0ms vs 50ms polling).

### 2. FD_CLOEXEC for All Internal FDs
**Context:** File descriptors leaking to child processes can cause resource exhaustion.
**Pattern:** Set FD_CLOEXEC on all internally-created fds (pipes, eventfds) immediately after creation.
**Benefit:** Prevents fd leaks across fork/exec boundaries.

### 3. net.Socket for Raw FD Event Loop Integration
**Context:** Bun can't directly watch arbitrary file descriptors in its event loop.
**Pattern:** Wrap raw fd in `new net.Socket({ fd })` and listen for 'readable' events.
**Benefit:** Native event loop integration without polling overhead.

### 4. Notification Coalescing via Pipe
**Context:** Multiple rapid signals shouldn't require multiple wakeups.
**Pattern:** Write single byte to non-blocking pipe on each signal. Reader drains entire pipe on wake.
**Benefit:** Multiple signals coalesce into single event loop callback.

### 5. C ABI Pattern: Box::into_raw / Box::from_raw
**Context:** Exposing Rust objects to C/FFI requires stable pointers.
**Pattern:**
- Create: `Box::into_raw(Box::new(wrapper))` to transfer ownership out
- Destroy: `Box::from_raw(handle)` to reclaim and drop
- Access: `&*handle` or `&mut *handle` for borrowing
**Benefit:** Safe memory management across FFI boundary.

## Technical Insights

### Why poll() over epoll for 2 FDs
- epoll has setup overhead (epoll_create, epoll_ctl)
- For 1-2 fds, poll() is simpler and equally efficient
- Kernel overhead dominates at this scale

### Why Separate Bridge Thread
- Bun is single-threaded; can't block main thread
- eventfd is Linux-specific; pipe is universal
- Thread isolation means no GIL/JS runtime concerns

## What Worked Well
- Incremental testing: Rust tests first, then TypeScript
- Following existing patterns (ShmNotifier → ShmAsyncBridge)
- Clear separation: Rust core → C ABI → TypeScript FFI → Application

## What Could Be Improved
- Could add metrics for bridge latency
- Could support multiple source fds per bridge (future optimization)
