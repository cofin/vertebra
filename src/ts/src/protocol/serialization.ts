import { decode, encode } from "@msgpack/msgpack";
import { Flags, HEADER_SIZE, RequestHeader, ResponseFlags, ResponseHeader } from "./headers";

export type MessagePayload = Uint8Array | ArrayBuffer;

export function encodeMessage(value: unknown): Uint8Array {
  return encode(value);
}

export function decodeMessage<T = unknown>(payload: MessagePayload): T {
  const buffer = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
  return decode(buffer) as T;
}

/**
 * Encode a request message with protocol header.
 *
 * Wire format: [RequestHeader 16 bytes][MessagePack payload]
 */
export function encodeRequest(
  value: unknown,
  correlationId: bigint = 0n,
  flags: number = Flags.NONE,
): Uint8Array {
  const payload = encode(value);
  const header = new RequestHeader(correlationId, flags, payload.length);
  const message = new Uint8Array(HEADER_SIZE + payload.length);
  header.writeTo(message, 0);
  message.set(payload, HEADER_SIZE);
  return message;
}

/**
 * Decoded request result.
 */
export type DecodedRequest<T = unknown> = {
  correlationId: bigint;
  flags: number;
  payload: T;
};

/**
 * Decode a request message with protocol header.
 *
 * @param data Raw message bytes
 * @returns Decoded request with correlation ID, flags, and payload
 */
export function decodeRequest<T = unknown>(data: Uint8Array): DecodedRequest<T> {
  if (data.length < HEADER_SIZE) {
    throw new Error(`Request too small: got ${data.length} bytes, need ${HEADER_SIZE}`);
  }
  const header = RequestHeader.fromBytes(data, 0);
  const payloadBytes = data.subarray(HEADER_SIZE, HEADER_SIZE + header.payloadLen);
  const payload = decode(payloadBytes) as T;
  return {
    correlationId: header.id,
    flags: header.flags,
    payload,
  };
}

/**
 * Encode a response message with protocol header.
 *
 * Wire format: [ResponseHeader 16 bytes][MessagePack payload]
 */
export function encodeResponse(
  value: unknown,
  correlationId: bigint = 0n,
  flags: number = ResponseFlags.OK,
): Uint8Array {
  const payload = encode(value);
  const header = new ResponseHeader(correlationId, flags, payload.length);
  const message = new Uint8Array(HEADER_SIZE + payload.length);
  header.writeTo(message, 0);
  message.set(payload, HEADER_SIZE);
  return message;
}

/**
 * Decoded response result.
 */
export type DecodedResponse<T = unknown> = {
  correlationId: bigint;
  flags: number;
  isOk: boolean;
  isError: boolean;
  payload: T;
};

/**
 * Decode a response message with protocol header.
 *
 * @param data Raw message bytes
 * @returns Decoded response with correlation ID, flags, status, and payload
 */
export function decodeResponse<T = unknown>(data: Uint8Array): DecodedResponse<T> {
  if (data.length < HEADER_SIZE) {
    throw new Error(`Response too small: got ${data.length} bytes, need ${HEADER_SIZE}`);
  }
  const header = ResponseHeader.fromBytes(data, 0);
  const payloadBytes = data.subarray(HEADER_SIZE, HEADER_SIZE + header.payloadLen);
  const payload = decode(payloadBytes) as T;
  return {
    correlationId: header.id,
    flags: header.flags,
    isOk: header.isOk,
    isError: header.isError,
    payload,
  };
}
