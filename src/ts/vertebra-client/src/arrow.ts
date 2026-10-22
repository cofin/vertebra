/**
 * Zero-copy Apache Arrow IPC stream client (`@vertebra/client/arrow`).
 */

export const ARROW_STREAM_MIME = "application/vnd.apache.arrow.stream";
export const ARROW_FILE_MAGIC = new Uint8Array([0x41, 0x52, 0x52, 0x4f, 0x57, 0x31, 0x00, 0x00]);
export const ARROW_CONTINUATION_MARKER = 0xffffffff;

export interface ArrowStreamOptions {
  readonly endpoint: string;
  readonly headers?: Record<string, string>;
}

export class ArrowStreamClient {
  private readonly endpoint: string;
  private readonly baseHeaders: Record<string, string>;

  public constructor(options: ArrowStreamOptions) {
    this.endpoint = options.endpoint;
    this.baseHeaders = {
      Accept: ARROW_STREAM_MIME,
      ...(options.headers ?? {}),
    };
  }

  public static hasIpcHeader(bytes: Uint8Array): boolean {
    if (bytes.byteLength < 8) {
      return false;
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    if (view.getUint32(0, true) === ARROW_CONTINUATION_MARKER) {
      return true;
    }
    for (let i = 0; i < 6; i += 1) {
      if (bytes[i] !== ARROW_FILE_MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  public get url(): string {
    return this.endpoint;
  }

  public get headers(): Record<string, string> {
    return { ...this.baseHeaders };
  }

  public async fetchBytes(init?: RequestInit): Promise<Uint8Array> {
    const response = await fetch(this.endpoint, {
      ...init,
      headers: {
        ...this.baseHeaders,
        ...((init?.headers as Record<string, string> | undefined) ?? {}),
      },
    });
    if (!response.ok) {
      throw new Error(`Failed to fetch Arrow IPC stream: HTTP ${response.status}`);
    }
    const buffer = await response.arrayBuffer();
    const bytes = new Uint8Array(buffer);
    if (!ArrowStreamClient.hasIpcHeader(bytes)) {
      throw new Error("Invalid Arrow IPC payload");
    }
    return bytes;
  }
}
