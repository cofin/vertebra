import { decodeMessage, encodeMessage, frameMessage } from "../protocol/serialization";
import type { Socket } from "bun";

export type IpcTransport = "tcp" | "uds";

export type IpcClientConfig = {
  transport: IpcTransport;
  host?: string;
  port?: number;
  path?: string;
  timeoutMs?: number;
};

type SocketState = {
  buffer: Uint8Array;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  done: boolean;
};

function concatBuffers(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length + b.length);
  result.set(a, 0);
  result.set(b, a.length);
  return result;
}

function parseFrame(buffer: Uint8Array): { frame: Uint8Array | null; rest: Uint8Array } {
  if (buffer.length < 4) {
    return { frame: null, rest: buffer };
  }
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  const size = view.getUint32(0, false);
  if (buffer.length - 4 < size) {
    return { frame: null, rest: buffer };
  }
  const frame = buffer.subarray(4, 4 + size);
  const rest = buffer.subarray(4 + size);
  return { frame, rest };
}

export class IpcClient {
  constructor(private config: IpcClientConfig) {}

  async request<T = unknown>(payload: unknown): Promise<T> {
    const timeoutMs = this.config.timeoutMs ?? 5000;
    const framed = frameMessage(encodeMessage(payload));
    return new Promise<T>((resolve, reject) => {
      let timer: number | null = null;
      const state: SocketState = {
        buffer: new Uint8Array(),
        resolve: (value) => {
          if (state.done) return;
          state.done = true;
          if (timer !== null) {
            clearTimeout(timer);
          }
          resolve(value as T);
        },
        reject: (reason) => {
          if (state.done) return;
          state.done = true;
          if (timer !== null) {
            clearTimeout(timer);
          }
          reject(reason);
        },
        done: false,
      };

      timer = setTimeout(() => {
        state.reject(new Error("IPC request timed out"));
      }, timeoutMs) as unknown as number;

      const connectOptions: Record<string, unknown> = {
        socket: {
          open: (socket: Socket) => {
            socket.write(framed);
          },
          data: (socket: Socket, data: Uint8Array) => {
            state.buffer = concatBuffers(state.buffer, data);
            const parsed = parseFrame(state.buffer);
            state.buffer = parsed.rest;
            if (parsed.frame) {
              const decoded = decodeMessage<T>(parsed.frame);
              state.resolve(decoded);
              socket.end();
            }
          },
          error: (_socket: Socket, error: Error) => {
            state.reject(error);
          },
          close: () => {
            if (!state.done) {
              state.reject(new Error("IPC socket closed"));
            }
          },
        },
      };

      if (this.config.transport === "uds") {
        connectOptions.unix = this.config.path ?? "";
      } else {
        connectOptions.hostname = this.config.host ?? "127.0.0.1";
        connectOptions.port = this.config.port ?? 7001;
      }

      Bun.connect(connectOptions).catch((error) => state.reject(error));
    });
  }
}
