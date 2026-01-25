import { dlopen, ptr, read, toArrayBuffer } from "bun:ffi";
import { existsSync } from "node:fs";
import { join, resolve } from "node:path";

/**
 * Create a null-terminated C string as Uint8Array.
 * This avoids TypeScript type issues with Buffer.from().
 */
function cString(s: string): Uint8Array {
  const encoder = new TextEncoder();
  const encoded = encoder.encode(s);
  const result = new Uint8Array(encoded.length + 1);
  result.set(encoded);
  result[encoded.length] = 0; // null terminator
  return result;
}

export type ShmLibrary = {
  vtb_last_error_message: () => number;
  vtb_ring_capacity: (handle: number) => bigint;
  vtb_ring_create: (name: number, slotSize: number, slots: number, outHandle: number) => number;
  vtb_ring_open: (name: number, slotSize: number, slots: number, outHandle: number) => number;
  vtb_ring_send: (
    handle: number,
    data: Uint8Array,
    len: number,
    flags: number,
    timeoutMs: number,
  ) => number;
  vtb_ring_recv: (
    handle: number,
    out: Uint8Array,
    capacity: number,
    outLen: BigUint64Array,
    outFlags: Uint32Array,
    timeoutMs: number,
  ) => number;
  vtb_ring_close: (handle: number) => void;
  vtb_region_create: (name: number, size: number, outHandle: number) => number;
  vtb_region_open: (name: number, size: number, outHandle: number) => number;
  vtb_region_ptr: (handle: number) => number;
  vtb_region_len: (handle: number) => bigint;
  vtb_region_close: (handle: number) => void;
  vtb_router_new: () => number;
  vtb_router_insert: (handle: number, path: Uint8Array, id: bigint) => number;
  vtb_router_match: (
    handle: number,
    path: Uint8Array,
    outId: BigUint64Array,
    outParamsJson: Uint8Array,
    outParamsCap: number,
    outParamsLen: BigUint64Array,
  ) => number;
  vtb_router_close: (handle: number) => void;
  vtb_mpmc_create: (name: number, slotSize: number, slots: number, outHandle: number) => number;
  vtb_mpmc_open: (name: number, slotSize: number, slots: number, outHandle: number) => number;
  vtb_mpmc_send: (
    handle: number,
    data: Uint8Array,
    len: number,
    flags: number,
    timeoutMs: number,
  ) => number;
  vtb_mpmc_recv: (
    handle: number,
    out: Uint8Array,
    capacity: number,
    outLen: BigUint64Array,
    outFlags: Uint32Array,
    timeoutMs: number,
  ) => number;
  vtb_mpmc_capacity: (handle: number) => bigint;
  vtb_mpmc_close: (handle: number) => void;
  vtb_shm_unlink: (name: number) => number;
  // Notifier functions for event-driven IPC
  vtb_notifier_new: (outHandle: number) => number;
  vtb_notifier_fd: (handle: number) => number;
  vtb_notifier_write_fd: (handle: number) => number;
  vtb_notifier_notify: (handle: number) => number;
  vtb_notifier_clear: (handle: number) => number;
  vtb_notifier_has_pending: (handle: number) => number;
  vtb_notifier_wait: (handle: number, timeoutMs: number) => number;
  vtb_notifier_from_fds: (readFd: number, writeFd: number, outHandle: number) => number;
  vtb_notifier_close: (handle: number) => void;
};

let cached: ShmLibrary | null = null;

const STATUS_OK = 0;
const STATUS_TIMEOUT = 3;
const STATUS_EMPTY = 5;

function readLastError(lib: ShmLibrary): string | null {
  const errPtr = lib.vtb_last_error_message();
  if (!errPtr) {
    return null;
  }
  const max = 4096;
  const buffer = new Uint8Array(toArrayBuffer(errPtr, 0, max));
  const end = buffer.indexOf(0);
  const slice = end === -1 ? buffer : buffer.subarray(0, end);
  const message = new TextDecoder().decode(slice);
  return message.length ? message : null;
}

function formatStatusError(status: number, lib: ShmLibrary): string {
  const message = readLastError(lib);
  if (message) {
    return `${status}: ${message}`;
  }
  return String(status);
}

function libName(): string {
  if (process.platform === "darwin") {
    return "libvertebra_broker.dylib";
  }
  if (process.platform === "win32") {
    return "vertebra_broker.dll";
  }
  return "libvertebra_broker.so";
}

function resolveLibraryPath(): string {
  const env = process.env.VERTEBRA_SHM_LIB;
  if (env && existsSync(env)) {
    return env;
  }
  const name = libName();
  const candidates = [
    resolve(process.cwd(), "src/rs/target/debug", name),
    resolve(process.cwd(), "src/rs/target/release", name),
    resolve(process.cwd(), "target/debug", name),
    resolve(process.cwd(), "target/release", name),
    // Relative if running from src/ts
    resolve(process.cwd(), "../rs/target/debug", name),
    resolve(process.cwd(), "../rs/target/release", name),
    // Relative to this file
    join(import.meta.dir, "..", "..", "..", "..", "src", "rs", "target", "debug", name),
    join(import.meta.dir, "..", "..", "..", "..", "src", "rs", "target", "release", name),
  ];
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  throw new Error("Unable to locate shared memory C ABI library; set VERTEBRA_SHM_LIB");
}

export function loadShmLib(): ShmLibrary {
  if (cached) {
    return cached;
  }
  const path = resolveLibraryPath();
  const lib = dlopen(path, {
    vtb_last_error_message: {
      args: [],
      returns: "ptr",
    },
    vtb_ring_capacity: {
      args: ["ptr"],
      returns: "usize",
    },
    vtb_ring_create: {
      args: ["ptr", "usize", "usize", "ptr"],
      returns: "i32",
    },
    vtb_ring_open: {
      args: ["ptr", "usize", "usize", "ptr"],
      returns: "i32",
    },
    vtb_ring_send: {
      args: ["ptr", "ptr", "usize", "u32", "u32"],
      returns: "i32",
    },
    vtb_ring_recv: {
      args: ["ptr", "ptr", "usize", "ptr", "ptr", "u32"],
      returns: "i32",
    },
    vtb_ring_close: {
      args: ["ptr"],
      returns: "void",
    },
    vtb_region_create: {
      args: ["ptr", "usize", "ptr"],
      returns: "i32",
    },
    vtb_region_open: {
      args: ["ptr", "usize", "ptr"],
      returns: "i32",
    },
    vtb_region_ptr: {
      args: ["ptr"],
      returns: "ptr",
    },
    vtb_region_len: {
      args: ["ptr"],
      returns: "usize",
    },
    vtb_region_close: {
      args: ["ptr"],
      returns: "void",
    },
    vtb_router_new: {
      args: [],
      returns: "ptr",
    },
    vtb_router_insert: {
      args: ["ptr", "ptr", "u64"],
      returns: "i32",
    },
    vtb_router_match: {
      args: ["ptr", "ptr", "ptr", "ptr", "usize", "ptr"],
      returns: "i32",
    },
    vtb_router_close: {
      args: ["ptr"],
      returns: "void",
    },
    vtb_mpmc_create: {
      args: ["ptr", "usize", "usize", "ptr"],
      returns: "i32",
    },
    vtb_mpmc_open: {
      args: ["ptr", "usize", "usize", "ptr"],
      returns: "i32",
    },
    vtb_mpmc_send: {
      args: ["ptr", "ptr", "usize", "u32", "u32"],
      returns: "i32",
    },
    vtb_mpmc_recv: {
      args: ["ptr", "ptr", "usize", "ptr", "ptr", "u32"],
      returns: "i32",
    },
    vtb_mpmc_capacity: {
      args: ["ptr"],
      returns: "usize",
    },
    vtb_mpmc_close: {
      args: ["ptr"],
      returns: "void",
    },
    vtb_shm_unlink: {
      args: ["ptr"],
      returns: "i32",
    },
    // Notifier functions for event-driven IPC
    vtb_notifier_new: {
      args: ["ptr"],
      returns: "i32",
    },
    vtb_notifier_fd: {
      args: ["ptr"],
      returns: "i32",
    },
    vtb_notifier_write_fd: {
      args: ["ptr"],
      returns: "i32",
    },
    vtb_notifier_notify: {
      args: ["ptr"],
      returns: "i32",
    },
    vtb_notifier_clear: {
      args: ["ptr"],
      returns: "i32",
    },
    vtb_notifier_has_pending: {
      args: ["ptr"],
      returns: "i32",
    },
    vtb_notifier_wait: {
      args: ["ptr", "i32"],
      returns: "i32",
    },
    vtb_notifier_from_fds: {
      args: ["i32", "i32", "ptr"],
      returns: "i32",
    },
    vtb_notifier_close: {
      args: ["ptr"],
      returns: "void",
    },
  });
  cached = lib.symbols as unknown as ShmLibrary;
  return cached;
}

export class ShmRing {
  private handle: number;
  private capacity: number;
  private lib: ShmLibrary;

  private constructor(handle: number, lib: ShmLibrary) {
    this.handle = handle;
    this.lib = lib;
    this.capacity = Number(lib.vtb_ring_capacity(handle));
  }

  static create(name: string, slotSize: number, slots: number): ShmRing {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const namePtr = ptr(cString(name));
    const status = lib.vtb_ring_create(namePtr, slotSize, slots, ptr(out));
    if (status !== STATUS_OK) {
      throw new Error(`shm ring create failed: ${formatStatusError(status, lib)}`);
    }
    const handle = read.ptr(ptr(out));
    return new ShmRing(handle, lib);
  }

  static open(name: string, slotSize: number, slots: number): ShmRing {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const namePtr = ptr(cString(name));
    const status = lib.vtb_ring_open(namePtr, slotSize, slots, ptr(out));
    if (status !== STATUS_OK) {
      throw new Error(`shm ring open failed: ${formatStatusError(status, lib)}`);
    }
    const handle = read.ptr(ptr(out));
    return new ShmRing(handle, lib);
  }

  send(payload: Uint8Array, flags = 0, timeoutMs = 0): void {
    const status = this.lib.vtb_ring_send(this.handle, payload, payload.length, flags, timeoutMs);
    if (status !== STATUS_OK) {
      throw new Error(`shm ring send failed: ${formatStatusError(status, this.lib)}`);
    }
  }

  /**
   * Receive a message from the ring buffer.
   *
   * @param timeoutMs Timeout in milliseconds (0 = non-blocking)
   * @param pool Optional buffer pool for zero-allocation receives
   * @returns Message data and flags, or null if empty/timeout
   *
   * When a BufferPool is provided, the returned buffer comes from the pool
   * and MUST be released back via pool.release(result.data) after use.
   * This eliminates per-message allocations in hot paths.
   */
  recv(timeoutMs = 0, pool?: BufferPool): { data: Uint8Array; flags: number } | null {
    // Use pooled buffer if available, otherwise allocate
    const buffer = pool?.acquire() ?? new Uint8Array(this.capacity);
    const outLen = new BigUint64Array(1);
    const outFlags = new Uint32Array(1);
    const status = this.lib.vtb_ring_recv(
      this.handle,
      buffer,
      buffer.length,
      outLen,
      outFlags,
      timeoutMs,
    );
    if (status === STATUS_TIMEOUT || status === STATUS_EMPTY) {
      // Release buffer back to pool if we acquired from pool
      if (pool) {
        pool.release(buffer);
      }
      return null;
    }
    if (status !== STATUS_OK) {
      // Release buffer back to pool on error
      if (pool) {
        pool.release(buffer);
      }
      throw new Error(`shm ring recv failed: ${formatStatusError(status, this.lib)}`);
    }
    const len = Number(outLen[0]);
    return { data: buffer.subarray(0, len), flags: outFlags[0] };
  }

  close(): void {
    this.lib.vtb_ring_close(this.handle);
  }

  /**
   * Unlink (remove) a named shared memory region.
   *
   * This removes the name from /dev/shm so new processes cannot open it.
   * Existing handles remain valid until closed.
   *
   * @param name The SHM name (e.g., "/vertebra-asgi.0.control")
   */
  static unlink(name: string): void {
    const lib = loadShmLib();
    const namePtr = ptr(cString(name));
    // Ignore errors (e.g. ENOENT) - region may not exist
    lib.vtb_shm_unlink(namePtr);
  }
}

export class ShmRegion {
  private handle: number;
  private length: number;
  private lib: ShmLibrary;
  private view?: Uint8Array;

  private constructor(handle: number, lib: ShmLibrary) {
    this.handle = handle;
    this.lib = lib;
    this.length = Number(lib.vtb_region_len(handle));
  }

  static create(name: string, size: number): ShmRegion {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const namePtr = ptr(cString(name));
    const status = lib.vtb_region_create(namePtr, size, ptr(out));
    if (status !== STATUS_OK) {
      throw new Error(`shm region create failed: ${formatStatusError(status, lib)}`);
    }
    const handle = read.ptr(ptr(out));
    return new ShmRegion(handle, lib);
  }

  static open(name: string, size: number): ShmRegion {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const namePtr = ptr(cString(name));
    const status = lib.vtb_region_open(namePtr, size, ptr(out));
    if (status !== STATUS_OK) {
      throw new Error(`shm region open failed: ${formatStatusError(status, lib)}`);
    }
    const handle = read.ptr(ptr(out));
    return new ShmRegion(handle, lib);
  }

  buffer(): Uint8Array {
    if (!this.view) {
      const ptr = this.lib.vtb_region_ptr(this.handle);
      this.view = new Uint8Array(toArrayBuffer(ptr, 0, this.length));
    }
    return this.view;
  }

  write(payload: Uint8Array): void {
    if (payload.length > this.length) {
      throw new Error(`shm region size ${this.length} < payload ${payload.length}`);
    }
    this.buffer().set(payload);
  }

  close(): void {
    this.lib.vtb_region_close(this.handle);
  }
}

export class ShmRouter {
  private handle: number;
  private lib: ShmLibrary;

  constructor() {
    this.lib = loadShmLib();
    this.handle = this.lib.vtb_router_new();
  }

  insert(path: string, id: number): void {
    const pathPtr = cString(path);
    const status = this.lib.vtb_router_insert(this.handle, pathPtr as any, BigInt(id));
    if (status !== STATUS_OK) {
      throw new Error(`shm router insert failed: ${formatStatusError(status, this.lib)}`);
    }
  }

  matchPath(path: string): { id: number; params: Record<string, string> } | null {
    const pathPtr = cString(path);
    const outId = new BigUint64Array(1);
    const paramsCap = 4096;
    const outParamsJson = new Uint8Array(paramsCap);
    const outParamsLen = new BigUint64Array(1);

    const status = this.lib.vtb_router_match(
      this.handle,
      pathPtr as any,
      outId,
      outParamsJson,
      paramsCap,
      outParamsLen,
    );
    if (status === STATUS_EMPTY) {
      return null;
    }
    if (status !== STATUS_OK) {
      throw new Error(`shm router match failed: ${formatStatusError(status, this.lib)}`);
    }

    const len = Number(outParamsLen[0]);
    let params = {};
    if (len > 0) {
      const jsonStr = new TextDecoder().decode(outParamsJson.subarray(0, len));
      try {
        params = JSON.parse(jsonStr);
      } catch {
        // ignore parse error
      }
    }

    return { id: Number(outId[0]), params };
  }

  close(): void {
    this.lib.vtb_router_close(this.handle);
  }
}

/**
 * Pre-allocated buffer pool for zero-allocation recv() operations.
 *
 * The pool pre-allocates a fixed number of Uint8Array buffers at construction.
 * Instead of allocating a new buffer on every recv(), callers acquire() from
 * the pool and release() when done processing the data.
 *
 * This eliminates GC pressure and provides consistent latency in hot paths.
 *
 * @example
 * ```typescript
 * const pool = new BufferPool(64, 65536); // 64 buffers of 64KB each
 *
 * // In recv loop:
 * const result = ring.recv(0, pool);
 * if (result) {
 *   processData(result.data);
 *   pool.release(result.data);
 * }
 * ```
 */
export class BufferPool {
  private readonly buffers: Uint8Array[];
  private readonly available: Uint8Array[];
  private readonly capacity: number;
  private readonly poolSize: number;

  /**
   * Create a new buffer pool.
   *
   * @param poolSize Number of buffers to pre-allocate
   * @param bufferCapacity Size of each buffer in bytes
   */
  constructor(poolSize: number, bufferCapacity: number) {
    this.poolSize = poolSize;
    this.capacity = bufferCapacity;
    this.buffers = [];
    this.available = [];

    // Pre-allocate all buffers at construction time
    for (let i = 0; i < poolSize; i++) {
      const buf = new Uint8Array(bufferCapacity);
      this.buffers.push(buf);
      this.available.push(buf);
    }
  }

  /**
   * Acquire a buffer from the pool.
   *
   * Returns null if the pool is exhausted. Callers should either:
   * - Wait and retry
   * - Fall back to allocation (not recommended for hot paths)
   * - Treat as backpressure signal
   */
  acquire(): Uint8Array | null {
    const buf = this.available.pop();
    return buf ?? null;
  }

  /**
   * Release a buffer back to the pool.
   *
   * The buffer must have been acquired from this pool.
   * Releasing a buffer that wasn't acquired from this pool is a logic error.
   */
  release(buffer: Uint8Array): void {
    // Only accept buffers that belong to this pool
    if (this.buffers.includes(buffer)) {
      this.available.push(buffer);
    }
  }

  /**
   * Get pool statistics.
   */
  stats(): { total: number; available: number; inUse: number; capacity: number } {
    return {
      total: this.poolSize,
      available: this.available.length,
      inUse: this.poolSize - this.available.length,
      capacity: this.capacity,
    };
  }
}

export class ShmMpmcRing {
  private handle: number;
  private capacity: number;
  private lib: ShmLibrary;

  private constructor(handle: number, lib: ShmLibrary) {
    this.handle = handle;
    this.lib = lib;
    this.capacity = Number(lib.vtb_mpmc_capacity(handle));
  }

  static create(name: string, slotSize: number, slots: number): ShmMpmcRing {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const namePtr = ptr(cString(name));
    const status = lib.vtb_mpmc_create(namePtr, slotSize, slots, ptr(out));
    if (status !== STATUS_OK) {
      throw new Error(`shm mpmc create failed: ${formatStatusError(status, lib)}`);
    }
    const handle = read.ptr(ptr(out));
    return new ShmMpmcRing(handle, lib);
  }

  static open(name: string, slotSize: number, slots: number): ShmMpmcRing {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const namePtr = ptr(cString(name));
    const status = lib.vtb_mpmc_open(namePtr, slotSize, slots, ptr(out));
    if (status !== STATUS_OK) {
      throw new Error(`shm mpmc open failed: ${formatStatusError(status, lib)}`);
    }
    const handle = read.ptr(ptr(out));
    return new ShmMpmcRing(handle, lib);
  }

  send(payload: Uint8Array, flags = 0, timeoutMs = 0): void {
    const status = this.lib.vtb_mpmc_send(this.handle, payload, payload.length, flags, timeoutMs);
    if (status !== STATUS_OK) {
      throw new Error(`shm mpmc send failed: ${formatStatusError(status, this.lib)}`);
    }
  }

  /**
   * Receive a message from the MPMC ring buffer.
   *
   * @param timeoutMs Timeout in milliseconds (0 = non-blocking)
   * @param pool Optional buffer pool for zero-allocation receives
   * @returns Message data and flags, or null if empty/timeout
   */
  recv(timeoutMs = 0, pool?: BufferPool): { data: Uint8Array; flags: number } | null {
    const buffer = pool?.acquire() ?? new Uint8Array(this.capacity);
    const outLen = new BigUint64Array(1);
    const outFlags = new Uint32Array(1);
    const status = this.lib.vtb_mpmc_recv(
      this.handle,
      buffer,
      buffer.length,
      outLen,
      outFlags,
      timeoutMs,
    );
    if (status === STATUS_TIMEOUT || status === STATUS_EMPTY) {
      if (pool) {
        pool.release(buffer);
      }
      return null;
    }
    if (status !== STATUS_OK) {
      if (pool) {
        pool.release(buffer);
      }
      throw new Error(`shm mpmc recv failed: ${formatStatusError(status, this.lib)}`);
    }
    const len = Number(outLen[0]);
    return { data: buffer.subarray(0, len), flags: outFlags[0] };
  }

  close(): void {
    this.lib.vtb_mpmc_close(this.handle);
  }

  static unlink(name: string): void {
    const lib = loadShmLib();
    const namePtr = ptr(cString(name));
    // We ignore errors (e.g. ENOENT) for unlink
    lib.vtb_shm_unlink(namePtr);
  }
}

/**
 * Cross-platform notification primitive for event-driven IPC.
 *
 * Uses eventfd on Linux (3-10µs latency) or pipe on macOS/Windows.
 * Register the `fd` with your event loop to be notified when data arrives.
 *
 * @example
 * ```typescript
 * const notifier = new ShmNotifier();
 * const fd = notifier.fd;
 *
 * // Producer signals data available
 * notifier.notify();
 *
 * // Consumer waits for notification (can use poll/epoll/kqueue)
 * notifier.wait(1000); // 1 second timeout
 * notifier.clear();
 * // Now try_recv() from the ring
 * ```
 */
export class ShmNotifier {
  private handle: number;
  private lib: ShmLibrary;

  constructor() {
    this.lib = loadShmLib();
    const out = new BigUint64Array(1);
    const status = this.lib.vtb_notifier_new(ptr(out));
    if (status !== STATUS_OK) {
      throw new Error(`notifier create failed: ${formatStatusError(status, this.lib)}`);
    }
    this.handle = read.ptr(ptr(out));
  }

  /**
   * Create a notifier from inherited file descriptors.
   *
   * Used by child processes that inherit FDs from their parent.
   * On Linux, only readFd is used (eventfd). For portability, pass both.
   */
  static fromFds(readFd: number, writeFd: number): ShmNotifier {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const status = lib.vtb_notifier_from_fds(readFd, writeFd, ptr(out));
    if (status !== STATUS_OK) {
      throw new Error(`notifier from_fds failed: ${formatStatusError(status, lib)}`);
    }
    const handle = read.ptr(ptr(out));
    const notifier = Object.create(ShmNotifier.prototype) as ShmNotifier;
    notifier.handle = handle;
    notifier.lib = lib;
    return notifier;
  }

  /** Get the notification FD for event loop registration. */
  get fd(): number {
    return this.lib.vtb_notifier_fd(this.handle);
  }

  /** Get the write FD (same as fd on Linux, different on macOS). */
  get writeFd(): number {
    return this.lib.vtb_notifier_write_fd(this.handle);
  }

  /** Signal that data is available (non-blocking). */
  notify(): void {
    const status = this.lib.vtb_notifier_notify(this.handle);
    if (status !== STATUS_OK) {
      throw new Error(`notifier notify failed: ${formatStatusError(status, this.lib)}`);
    }
  }

  /** Clear pending notifications. Call after handling data. */
  clear(): void {
    const status = this.lib.vtb_notifier_clear(this.handle);
    if (status !== STATUS_OK) {
      throw new Error(`notifier clear failed: ${formatStatusError(status, this.lib)}`);
    }
  }

  /** Check if there are pending notifications (non-blocking). */
  hasPending(): boolean {
    return this.lib.vtb_notifier_has_pending(this.handle) !== 0;
  }

  /**
   * Wait for a notification with timeout.
   *
   * @param timeoutMs Timeout in milliseconds (-1 for infinite)
   * @returns true if notification received, false on timeout
   */
  wait(timeoutMs: number): boolean {
    const status = this.lib.vtb_notifier_wait(this.handle, timeoutMs);
    if (status === STATUS_OK) {
      return true;
    }
    if (status === STATUS_TIMEOUT) {
      return false;
    }
    throw new Error(`notifier wait failed: ${formatStatusError(status, this.lib)}`);
  }

  /** Close the notifier. */
  close(): void {
    this.lib.vtb_notifier_close(this.handle);
  }
}
