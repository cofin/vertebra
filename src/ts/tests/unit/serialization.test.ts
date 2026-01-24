import { describe, expect, test } from "bun:test";
import {
  decodeMessage,
  encodeMessage,
  encodeRequest,
  decodeRequest,
  encodeResponse,
  decodeResponse,
} from "../../src/protocol/serialization";
import { Flags, ResponseFlags } from "../../src/protocol/headers";

const payload = { id: "abc", ok: true, values: [1, 2, 3] };

describe("protocol serialization", () => {
  test("roundtrip msgpack", () => {
    const encoded = encodeMessage(payload);
    const decoded = decodeMessage<typeof payload>(encoded);
    expect(decoded).toEqual(payload);
  });

  test("request/response roundtrip", () => {
    const correlationId = 42n;
    const requestEncoded = encodeRequest(payload, correlationId, Flags.NONE);
    const requestDecoded = decodeRequest<typeof payload>(requestEncoded);
    expect(requestDecoded.correlationId).toBe(correlationId);
    expect(requestDecoded.payload).toEqual(payload);

    const responseEncoded = encodeResponse(payload, correlationId, ResponseFlags.OK);
    const responseDecoded = decodeResponse<typeof payload>(responseEncoded);
    expect(responseDecoded.correlationId).toBe(correlationId);
    expect(responseDecoded.isOk).toBe(true);
    expect(responseDecoded.payload).toEqual(payload);
  });
});
