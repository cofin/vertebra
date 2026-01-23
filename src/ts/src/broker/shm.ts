import { dlopen, toArrayBuffer } from "bun:ffi";
import { existsSync } from "node:fs";
import { join, resolve } from "node:path";

export type ShmLibrary = {
  vtb_last_error_message: () => bigint;
  vtb_ring_capacity: (handle: bigint) => bigint;
  vtb_ring_create: (
    name: string,
    slotSize: number,
    slots: number,
    outHandle: BigUint64Array,
  ) => number;
  vtb_ring_open: (
    name: string,
    slotSize: number,
    slots: number,
    outHandle: BigUint64Array,
  ) => number;
  vtb_ring_send: (
    handle: bigint,
    data: Uint8Array,
    len: number,
    flags: number,
    timeoutMs: number,
  ) => number;
  vtb_ring_recv: (
    handle: bigint,
    out: Uint8Array,
    capacity: number,
    outLen: BigUint64Array,
    outFlags: Uint32Array,
    timeoutMs: number,
  ) => number;
  vtb_ring_close: (handle: bigint) => void;
  vtb_region_create: (name: string, size: number, outHandle: BigUint64Array) => number;
  vtb_region_open: (name: string, size: number, outHandle: BigUint64Array) => number;
  vtb_region_ptr: (handle: bigint) => bigint;
  vtb_region_len: (handle: bigint) => bigint;
  vtb_region_close: (handle: bigint) => void;
  vtb_router_new: () => bigint;
  vtb_router_insert: (handle: bigint, path: string, id: bigint) => number;
  vtb_router_match: (handle: bigint, path: string, outId: BigUint64Array) => number;
  vtb_router_close: (handle: bigint) => void;
};

let cached: ShmLibrary | null = null;

const STATUS_OK = 0;
const STATUS_TIMEOUT = 3;
const STATUS_EMPTY = 5;

function readLastError(lib: ShmLibrary): string | null {
  const ptr = lib.vtb_last_error_message();
  if (ptr === 0n) {
    return null;
  }
  const max = 4096;
  const buffer = new Uint8Array(toArrayBuffer(ptr, 0, max));
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
    join(import.meta.dir, "..", "..", name),
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
      args: ["cstring", "usize", "usize", "ptr"],
      returns: "i32",
    },
    vtb_ring_open: {
      args: ["cstring", "usize", "usize", "ptr"],
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
      args: ["cstring", "usize", "ptr"],
      returns: "i32",
    },
    vtb_region_open: {
      args: ["cstring", "usize", "ptr"],
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
      args: ["ptr", "ptr", "ptr"],
      returns: "i32",
    },
    vtb_router_close: {
      args: ["ptr"],
      returns: "void",
    },
  });
  cached = lib.symbols as unknown as ShmLibrary;
  return cached;
}

export class ShmRing {
  private handle: bigint;
  private capacity: number;
  private lib: ShmLibrary;

  private constructor(handle: bigint, lib: ShmLibrary) {
    this.handle = handle;
    this.lib = lib;
    this.capacity = Number(lib.vtb_ring_capacity(handle));
  }

  static create(name: string, slotSize: number, slots: number): ShmRing {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const status = lib.vtb_ring_create(name, slotSize, slots, out);
    if (status !== STATUS_OK) {
      throw new Error(`shm ring create failed: ${formatStatusError(status, lib)}`);
    }
    return new ShmRing(out[0], lib);
  }

  static open(name: string, slotSize: number, slots: number): ShmRing {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const status = lib.vtb_ring_open(name, slotSize, slots, out);
    if (status !== STATUS_OK) {
      throw new Error(`shm ring open failed: ${formatStatusError(status, lib)}`);
    }
    return new ShmRing(out[0], lib);
  }

  send(payload: Uint8Array, flags = 0, timeoutMs = 0): void {
    const status = this.lib.vtb_ring_send(this.handle, payload, payload.length, flags, timeoutMs);
    if (status !== STATUS_OK) {
      throw new Error(`shm ring send failed: ${formatStatusError(status, this.lib)}`);
    }
  }

  recv(timeoutMs = 0): { data: Uint8Array; flags: number } | null {
    const buffer = new Uint8Array(this.capacity);
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
      return null;
    }
    if (status !== STATUS_OK) {
      throw new Error(`shm ring recv failed: ${formatStatusError(status, this.lib)}`);
    }
    const len = Number(outLen[0]);
    return { data: buffer.subarray(0, len), flags: outFlags[0] };
  }

  close(): void {
    this.lib.vtb_ring_close(this.handle);
  }
}

export class ShmRegion {
  private handle: bigint;
  private length: number;
  private lib: ShmLibrary;
  private view?: Uint8Array;

  private constructor(handle: bigint, lib: ShmLibrary) {
    this.handle = handle;
    this.lib = lib;
    this.length = Number(lib.vtb_region_len(handle));
  }

  static create(name: string, size: number): ShmRegion {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const status = lib.vtb_region_create(name, size, out);
    if (status !== STATUS_OK) {
      throw new Error(`shm region create failed: ${formatStatusError(status, lib)}`);
    }
    return new ShmRegion(out[0], lib);
  }

  static open(name: string, size: number): ShmRegion {
    const lib = loadShmLib();
    const out = new BigUint64Array(1);
    const status = lib.vtb_region_open(name, size, out);
    if (status !== STATUS_OK) {
      throw new Error(`shm region open failed: ${formatStatusError(status, lib)}`);
    }
    return new ShmRegion(out[0], lib);
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
  private handle: bigint;
  private lib: ShmLibrary;

  constructor() {
    this.lib = loadShmLib();
    this.handle = this.lib.vtb_router_new();
  }

  insert(path: string, id: number): void {
    const pathPtr = Buffer.from(`${path}\0`);
    const status = this.lib.vtb_router_insert(this.handle, pathPtr as any, BigInt(id));
    if (status !== STATUS_OK) {
      throw new Error(`shm router insert failed: ${formatStatusError(status, this.lib)}`);
    }
  }

  matchPath(path: string): number | null {
    const pathPtr = Buffer.from(`${path}\0`);
    const out = new BigUint64Array(1);
    const status = this.lib.vtb_router_match(this.handle, pathPtr as any, out);
    if (status === STATUS_EMPTY) {
      return null;
    }
    if (status !== STATUS_OK) {
      throw new Error(`shm router match failed: ${formatStatusError(status, this.lib)}`);
    }
    return Number(out[0]);
  }

  close(): void {
    this.lib.vtb_router_close(this.handle);
  }
}
