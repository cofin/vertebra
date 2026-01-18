import { decode, encode } from "@msgpack/msgpack";

export type MessagePayload = Uint8Array | ArrayBuffer;

export function encodeMessage(value: unknown): Uint8Array {
  return encode(value);
}

export function decodeMessage<T = unknown>(payload: MessagePayload): T {
  const buffer = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
  return decode(buffer) as T;
}

export function frameMessage(payload: Uint8Array): Uint8Array {
  const header = new Uint8Array(4);
  const view = new DataView(header.buffer);
  view.setUint32(0, payload.length, false);
  const framed = new Uint8Array(4 + payload.length);
  framed.set(header, 0);
  framed.set(payload, 4);
  return framed;
}

export function deframeMessage(payload: Uint8Array): Uint8Array {
  if (payload.length < 4) {
    throw new Error("Frame too small");
  }
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const size = view.getUint32(0, false);
  const body = payload.subarray(4);
  if (body.length !== size) {
    throw new Error("Frame size mismatch");
  }
  return body;
}
