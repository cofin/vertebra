/**
 * Protocol definitions for IPC control plane.
 *
 * This module provides zero-parse-cost message headers for IPC communication.
 * Headers use a fixed 16-byte format that can be read directly from shared memory
 * without parsing overhead.
 *
 * Wire Format:
 * ┌─────────────────────┬─────────────────────┐
 * │  Header (16 bytes)  │  MessagePack Payload │
 * └─────────────────────┴─────────────────────┘
 *
 * The header contains:
 * - id: 64-bit correlation ID for multiplexing
 * - flags: 32-bit flags (priority, compression, etc.)
 * - payload_len: 32-bit length of the MessagePack payload
 */

/** Header size in bytes (u64 + u32 + u32 = 16 bytes) */
export const HEADER_SIZE = 16;

/**
 * Request flags for IPC messages.
 */
export const Flags = {
  /** No special flags */
  NONE: 0,
  /** Request requires immediate processing (skip queue) */
  PRIORITY: 1 << 0,
  /** Payload is compressed */
  COMPRESSED: 1 << 1,
  /** Request expects no response */
  FIRE_AND_FORGET: 1 << 2,
  /** Request is part of a batch */
  BATCH: 1 << 3,
  /** Response indicates success */
  OK: 1 << 4,
  /** Response contains an error */
  ERROR: 1 << 5,
} as const;

/**
 * Response flags for IPC messages.
 */
export const ResponseFlags = {
  NONE: Flags.NONE,
  OK: Flags.OK,
  ERROR: Flags.ERROR,
  COMPRESSED: Flags.COMPRESSED,
} as const;

/**
 * Fixed-size request header (16 bytes).
 *
 * This header is designed for zero-copy reads directly from shared memory.
 * All fields are little-endian for consistent cross-platform behavior.
 */
export class RequestHeader {
  /** Correlation ID for request/response matching in multiplexed scenarios */
  readonly id: bigint;
  /** Request flags (see Flags constants) */
  readonly flags: number;
  /** Length of the MessagePack payload following this header */
  readonly payloadLen: number;

  constructor(id: bigint, flags: number, payloadLen: number) {
    this.id = id;
    this.flags = flags;
    this.payloadLen = payloadLen;
  }

  /**
   * Parse header from a Uint8Array (zero-copy when aligned).
   *
   * @param data - At least 16 bytes of header data
   * @param offset - Byte offset to read from (default 0)
   * @returns Parsed header
   * @throws Error if data is too short
   */
  static fromBytes(data: Uint8Array, offset = 0): RequestHeader {
    if (data.length - offset < HEADER_SIZE) {
      throw new Error(`Header requires ${HEADER_SIZE} bytes, got ${data.length - offset}`);
    }
    const view = new DataView(data.buffer, data.byteOffset + offset, HEADER_SIZE);
    const id = view.getBigUint64(0, true); // little-endian
    const flags = view.getUint32(8, true);
    const payloadLen = view.getUint32(12, true);
    return new RequestHeader(id, flags, payloadLen);
  }

  /**
   * Serialize header to bytes.
   *
   * @returns 16-byte Uint8Array
   */
  toBytes(): Uint8Array {
    const buffer = new ArrayBuffer(HEADER_SIZE);
    const view = new DataView(buffer);
    view.setBigUint64(0, this.id, true);
    view.setUint32(8, this.flags, true);
    view.setUint32(12, this.payloadLen, true);
    return new Uint8Array(buffer);
  }

  /**
   * Write header to buffer at offset (zero-copy).
   *
   * @param buffer - Writable buffer
   * @param offset - Byte offset to write at
   */
  writeTo(buffer: Uint8Array, offset = 0): void {
    const view = new DataView(buffer.buffer, buffer.byteOffset + offset, HEADER_SIZE);
    view.setBigUint64(0, this.id, true);
    view.setUint32(8, this.flags, true);
    view.setUint32(12, this.payloadLen, true);
  }
}

/**
 * Fixed-size response header (16 bytes).
 */
export class ResponseHeader {
  /** Correlation ID (matches the request) */
  readonly id: bigint;
  /** Response flags */
  readonly flags: number;
  /** Length of the MessagePack payload following this header */
  readonly payloadLen: number;

  constructor(id: bigint, flags: number, payloadLen: number) {
    this.id = id;
    this.flags = flags;
    this.payloadLen = payloadLen;
  }

  /**
   * Parse header from a Uint8Array.
   */
  static fromBytes(data: Uint8Array, offset = 0): ResponseHeader {
    if (data.length - offset < HEADER_SIZE) {
      throw new Error(`Header requires ${HEADER_SIZE} bytes, got ${data.length - offset}`);
    }
    const view = new DataView(data.buffer, data.byteOffset + offset, HEADER_SIZE);
    const id = view.getBigUint64(0, true);
    const flags = view.getUint32(8, true);
    const payloadLen = view.getUint32(12, true);
    return new ResponseHeader(id, flags, payloadLen);
  }

  /**
   * Serialize header to bytes.
   */
  toBytes(): Uint8Array {
    const buffer = new ArrayBuffer(HEADER_SIZE);
    const view = new DataView(buffer);
    view.setBigUint64(0, this.id, true);
    view.setUint32(8, this.flags, true);
    view.setUint32(12, this.payloadLen, true);
    return new Uint8Array(buffer);
  }

  /**
   * Write header to buffer at offset.
   */
  writeTo(buffer: Uint8Array, offset = 0): void {
    const view = new DataView(buffer.buffer, buffer.byteOffset + offset, HEADER_SIZE);
    view.setBigUint64(0, this.id, true);
    view.setUint32(8, this.flags, true);
    view.setUint32(12, this.payloadLen, true);
  }

  /** Check if response indicates success */
  get isOk(): boolean {
    return (this.flags & ResponseFlags.OK) !== 0;
  }

  /** Check if response contains an error */
  get isError(): boolean {
    return (this.flags & ResponseFlags.ERROR) !== 0;
  }
}

/**
 * Control message types for broker coordination.
 */
export const ControlMessageKind = {
  CONNECT: "connect",
  DISCONNECT: "disconnect",
  HEARTBEAT: "heartbeat",
  SHUTDOWN: "shutdown",
} as const;

export type ControlMessageKindType = (typeof ControlMessageKind)[keyof typeof ControlMessageKind];

/**
 * Check if a string is a valid control message kind.
 */
export function isValidControlMessageKind(kind: string): kind is ControlMessageKindType {
  return Object.values(ControlMessageKind).includes(kind as ControlMessageKindType);
}
