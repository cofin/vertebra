import { describe, test, expect } from "bun:test";
import {
  HEADER_SIZE,
  Flags,
  ResponseFlags,
  RequestHeader,
  ResponseHeader,
  ControlMessageKind,
  isValidControlMessageKind,
} from "../../../src/protocol/headers";

describe("RequestHeader", () => {
  test("HEADER_SIZE should be 16 bytes", () => {
    expect(HEADER_SIZE).toBe(16);
  });

  test("roundtrip serialization", () => {
    const header = new RequestHeader(
      42n,
      Flags.PRIORITY | Flags.COMPRESSED,
      1024
    );
    const bytes = header.toBytes();
    expect(bytes.length).toBe(16);

    const restored = RequestHeader.fromBytes(bytes);
    expect(restored.id).toBe(42n);
    expect(restored.flags).toBe(Flags.PRIORITY | Flags.COMPRESSED);
    expect(restored.payloadLen).toBe(1024);
  });

  test("parse from offset", () => {
    const header = new RequestHeader(123n, 0, 512);
    const buffer = new Uint8Array(32);
    buffer.set(header.toBytes(), 8);

    const restored = RequestHeader.fromBytes(buffer, 8);
    expect(restored.id).toBe(123n);
    expect(restored.payloadLen).toBe(512);
  });

  test("writeTo buffer at offset", () => {
    const header = new RequestHeader(99n, Flags.BATCH, 256);
    const buffer = new Uint8Array(32);

    header.writeTo(buffer, 8);

    // First 8 bytes should be unchanged
    expect(buffer.slice(0, 8)).toEqual(new Uint8Array(8));

    // Header should be at offset 8
    const restored = RequestHeader.fromBytes(buffer, 8);
    expect(restored.id).toBe(99n);
    expect(restored.flags).toBe(Flags.BATCH);
    expect(restored.payloadLen).toBe(256);
  });

  test("throws on too short data", () => {
    expect(() => RequestHeader.fromBytes(new Uint8Array(15))).toThrow(
      "Header requires 16 bytes"
    );
  });
});

describe("ResponseHeader", () => {
  test("roundtrip serialization", () => {
    const header = new ResponseHeader(99n, ResponseFlags.OK, 512);
    const bytes = header.toBytes();
    const restored = ResponseHeader.fromBytes(bytes);

    expect(restored.id).toBe(99n);
    expect(restored.flags).toBe(ResponseFlags.OK);
    expect(restored.payloadLen).toBe(512);
  });

  test("isOk and isError flags", () => {
    const okHeader = new ResponseHeader(1n, ResponseFlags.OK, 0);
    const errHeader = new ResponseHeader(2n, ResponseFlags.ERROR, 0);
    const noneHeader = new ResponseHeader(3n, ResponseFlags.NONE, 0);

    expect(okHeader.isOk).toBe(true);
    expect(okHeader.isError).toBe(false);

    expect(errHeader.isOk).toBe(false);
    expect(errHeader.isError).toBe(true);

    expect(noneHeader.isOk).toBe(false);
    expect(noneHeader.isError).toBe(false);
  });
});

describe("Flags", () => {
  test("flag values", () => {
    expect(Flags.NONE).toBe(0);
    expect(Flags.PRIORITY).toBe(1);
    expect(Flags.COMPRESSED).toBe(2);
    expect(Flags.FIRE_AND_FORGET).toBe(4);
    expect(Flags.BATCH).toBe(8);
    expect(Flags.OK).toBe(16);
    expect(Flags.ERROR).toBe(32);
  });

  test("flag combination", () => {
    const combined = Flags.PRIORITY | Flags.COMPRESSED;
    expect(combined & Flags.PRIORITY).toBeTruthy();
    expect(combined & Flags.COMPRESSED).toBeTruthy();
    expect(combined & Flags.BATCH).toBeFalsy();
  });
});

describe("ControlMessageKind", () => {
  test("valid kinds", () => {
    expect(isValidControlMessageKind("connect")).toBe(true);
    expect(isValidControlMessageKind("disconnect")).toBe(true);
    expect(isValidControlMessageKind("heartbeat")).toBe(true);
    expect(isValidControlMessageKind("shutdown")).toBe(true);
  });

  test("invalid kinds", () => {
    expect(isValidControlMessageKind("invalid")).toBe(false);
    expect(isValidControlMessageKind("")).toBe(false);
    expect(isValidControlMessageKind("CONNECT")).toBe(false); // Case sensitive
  });
});

describe("Cross-language compatibility", () => {
  test("header bytes match expected layout", () => {
    // Create header with known values
    const header = new RequestHeader(
      0x0102030405060708n, // id
      0x11121314, // flags
      0x21222324 // payloadLen
    );
    const bytes = header.toBytes();

    // Verify little-endian layout
    // id: 0x0102030405060708 -> 08 07 06 05 04 03 02 01
    expect(bytes[0]).toBe(0x08);
    expect(bytes[1]).toBe(0x07);
    expect(bytes[2]).toBe(0x06);
    expect(bytes[3]).toBe(0x05);
    expect(bytes[4]).toBe(0x04);
    expect(bytes[5]).toBe(0x03);
    expect(bytes[6]).toBe(0x02);
    expect(bytes[7]).toBe(0x01);

    // flags: 0x11121314 -> 14 13 12 11
    expect(bytes[8]).toBe(0x14);
    expect(bytes[9]).toBe(0x13);
    expect(bytes[10]).toBe(0x12);
    expect(bytes[11]).toBe(0x11);

    // payloadLen: 0x21222324 -> 24 23 22 21
    expect(bytes[12]).toBe(0x24);
    expect(bytes[13]).toBe(0x23);
    expect(bytes[14]).toBe(0x22);
    expect(bytes[15]).toBe(0x21);
  });
});
