import { describe, test, expect } from "bun:test";
import {
  encodeRequest,
  decodeRequest,
  encodeResponse,
  decodeResponse,
  encodeMessage,
  decodeMessage,
} from "../../../src/protocol/serialization";
import { HEADER_SIZE, Flags, ResponseFlags } from "../../../src/protocol/headers";

describe("encodeMessage/decodeMessage", () => {
  test("roundtrip simple object", () => {
    const original = { hello: "world", count: 42 };
    const encoded = encodeMessage(original);
    const decoded = decodeMessage<typeof original>(encoded);
    expect(decoded).toEqual(original);
  });

  test("roundtrip array", () => {
    const original = [1, 2, 3, "test"];
    const encoded = encodeMessage(original);
    const decoded = decodeMessage<typeof original>(encoded);
    expect(decoded).toEqual(original);
  });
});

describe("encodeRequest/decodeRequest", () => {
  test("roundtrip with defaults", () => {
    const payload = { method: "greet", params: { name: "Alice" } };
    const encoded = encodeRequest(payload);

    expect(encoded.length).toBeGreaterThan(HEADER_SIZE);

    const decoded = decodeRequest<typeof payload>(encoded);
    expect(decoded.correlationId).toBe(0n);
    expect(decoded.flags).toBe(Flags.NONE);
    expect(decoded.payload).toEqual(payload);
  });

  test("roundtrip with correlation ID", () => {
    const payload = { action: "test" };
    const correlationId = 12345n;
    const encoded = encodeRequest(payload, correlationId);

    const decoded = decodeRequest<typeof payload>(encoded);
    expect(decoded.correlationId).toBe(correlationId);
    expect(decoded.payload).toEqual(payload);
  });

  test("roundtrip with flags", () => {
    const payload = { data: [1, 2, 3] };
    const flags = Flags.PRIORITY | Flags.COMPRESSED;
    const encoded = encodeRequest(payload, 99n, flags);

    const decoded = decodeRequest<typeof payload>(encoded);
    expect(decoded.correlationId).toBe(99n);
    expect(decoded.flags).toBe(flags);
    expect(decoded.payload).toEqual(payload);
  });

  test("throws on too small data", () => {
    expect(() => decodeRequest(new Uint8Array(15))).toThrow("Request too small");
  });
});

describe("encodeResponse/decodeResponse", () => {
  test("roundtrip with defaults", () => {
    const payload = { result: "success", value: 42 };
    const encoded = encodeResponse(payload);

    expect(encoded.length).toBeGreaterThan(HEADER_SIZE);

    const decoded = decodeResponse<typeof payload>(encoded);
    expect(decoded.correlationId).toBe(0n);
    expect(decoded.flags).toBe(ResponseFlags.OK);
    expect(decoded.isOk).toBe(true);
    expect(decoded.isError).toBe(false);
    expect(decoded.payload).toEqual(payload);
  });

  test("roundtrip with correlation ID", () => {
    const payload = { status: "done" };
    const correlationId = 54321n;
    const encoded = encodeResponse(payload, correlationId);

    const decoded = decodeResponse<typeof payload>(encoded);
    expect(decoded.correlationId).toBe(correlationId);
    expect(decoded.isOk).toBe(true);
  });

  test("error response", () => {
    const payload = { error: "Not found", code: 404 };
    const encoded = encodeResponse(payload, 100n, ResponseFlags.ERROR);

    const decoded = decodeResponse<typeof payload>(encoded);
    expect(decoded.correlationId).toBe(100n);
    expect(decoded.isOk).toBe(false);
    expect(decoded.isError).toBe(true);
    expect(decoded.payload).toEqual(payload);
  });

  test("throws on too small data", () => {
    expect(() => decodeResponse(new Uint8Array(10))).toThrow("Response too small");
  });
});

describe("Cross-language compatibility", () => {
  test("request wire format matches Python", () => {
    // Encode a known payload and verify header layout
    const payload = { test: true };
    const correlationId = 0x0102030405060708n;
    const flags = 0x11121314;
    const encoded = encodeRequest(payload, correlationId, flags);

    // Verify little-endian layout of correlation ID
    expect(encoded[0]).toBe(0x08);
    expect(encoded[1]).toBe(0x07);
    expect(encoded[2]).toBe(0x06);
    expect(encoded[3]).toBe(0x05);
    expect(encoded[4]).toBe(0x04);
    expect(encoded[5]).toBe(0x03);
    expect(encoded[6]).toBe(0x02);
    expect(encoded[7]).toBe(0x01);

    // Verify little-endian layout of flags
    expect(encoded[8]).toBe(0x14);
    expect(encoded[9]).toBe(0x13);
    expect(encoded[10]).toBe(0x12);
    expect(encoded[11]).toBe(0x11);

    // payload_len is at bytes 12-15 (varies with payload size)
    // Just verify we can decode it back
    const decoded = decodeRequest(encoded);
    expect(decoded.correlationId).toBe(correlationId);
    expect(decoded.flags).toBe(flags);
  });
});
