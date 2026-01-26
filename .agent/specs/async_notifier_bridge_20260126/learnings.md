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

### 3. ⚠️ Bun Cannot Watch Arbitrary File Descriptors
**Context:** Tried to integrate pipe fds with Bun's event loop for zero-polling response notification.
**Discovery:** None of these approaches work in Bun for non-socket fds:
- `new net.Socket({ fd })` - `readable` events never fire
- `Bun.file(fd).stream()` - doesn't work for pipes
- `fs.createReadStream("", { fd })` - events don't fire
**Root Cause:** Bun's Node.js compatibility layer is an emulation that doesn't properly register arbitrary fds with libuv/the underlying event loop.
**Workaround:** Use hybrid polling loop (tight spin + periodic yield) instead of event-driven.
**Impact:** ~5,700 RPS with polling vs theoretical higher throughput with true event-driven.

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

## Critical Discovery: Bun FD Watching Limitations

### The Problem
The original plan assumed Bun could watch pipe file descriptors via `net.Socket({ fd })`. This assumption was based on Node.js behavior where this pattern works reliably.

### What We Tried
1. **net.Socket({ fd: pipeFd })** - Events never fired
2. **Bun.file(pipeFd).stream().getReader()** - Stream never yielded data
3. **fs.createReadStream("", { fd: pipeFd })** - Data events never fired

### Why It Failed
Bun's `node:net` module is a compatibility shim, not a full implementation. It doesn't properly integrate arbitrary file descriptors with Bun's event loop. Only actual network sockets work correctly.

### The Fix
Replaced event-driven approach with hybrid polling:
```typescript
private async runResponseLoop(): Promise<void> {
  let spinCount = 0;
  const maxSpins = 100;

  while (this.responseLoopRunning && this.pendingRequests.size > 0) {
    const hadResponses = this.drainResponses();
    if (hadResponses) {
      spinCount = 0;  // Reset on success
    } else {
      spinCount++;
      if (spinCount >= maxSpins) {
        spinCount = 0;
        await yieldToEventLoop();  // Prevent blocking HTTP
      }
    }
  }
}
```

### Performance Results
| Approach | RPS | Success Rate |
|----------|-----|--------------|
| Event-driven (broken) | 21 | 0% |
| Hybrid polling | 5,724 | 100% |

### Future Considerations
- Monitor Bun releases for proper fd watching support
- Consider WebSocket or Unix socket alternatives (Bun handles these correctly)
- The AsyncNotifierBridge Rust code is preserved for when Bun gains this capability
