import { decodeMessage, deframeMessage, encodeMessage, frameMessage } from "./serialization";

const payload = { id: "abc", ok: true, values: [1, 2, 3] };

describe("protocol serialization", () => {
  test("roundtrip msgpack", () => {
    const encoded = encodeMessage(payload);
    const decoded = decodeMessage<typeof payload>(encoded);
    expect(decoded).toEqual(payload);
  });

  test("frame and deframe", () => {
    const encoded = encodeMessage(payload);
    const framed = frameMessage(encoded);
    const body = deframeMessage(framed);
    expect(body).toEqual(encoded);
  });
});
