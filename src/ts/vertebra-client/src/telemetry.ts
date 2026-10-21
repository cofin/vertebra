const HEX_32_RE = /^[0-9a-f]{32}$/i;
const HEX_16_RE = /^[0-9a-f]{16}$/i;
const HEX_2_RE = /^[0-9a-f]{2}$/i;

function randomHex(byteLength: number): string {
  const bytes = new Uint8Array(byteLength);
  if (typeof globalThis.crypto?.getRandomValues === "function") {
    globalThis.crypto.getRandomValues(bytes);
  } else {
    for (let i = 0; i < byteLength; i += 1) {
      bytes[i] = Math.floor(Math.random() * 256);
    }
  }
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

export class TraceContext {
  readonly version: string;
  readonly traceId: string;
  readonly spanId: string;
  readonly traceFlags: number;

  constructor(version: string | number, traceId: string, spanId: string, traceFlags = 1) {
    this.version =
      typeof version === "number"
        ? (version & 0xff).toString(16).padStart(2, "0")
        : version.toLowerCase().padStart(2, "0").slice(0, 2);
    this.traceId = traceId.toLowerCase();
    this.spanId = spanId.toLowerCase();
    this.traceFlags = traceFlags;
  }

  static parse(header: string): TraceContext | null {
    const parts = header.trim().split("-");
    if (parts.length < 4) {
      return null;
    }
    const [verHex, traceId, spanId, flagsHex] = parts;
    if (
      !verHex ||
      !traceId ||
      !spanId ||
      !flagsHex ||
      !HEX_2_RE.test(verHex) ||
      !HEX_32_RE.test(traceId) ||
      !HEX_16_RE.test(spanId) ||
      !HEX_2_RE.test(flagsHex)
    ) {
      return null;
    }
    const version = Number.parseInt(verHex, 16);
    const traceFlags = Number.parseInt(flagsHex, 16);
    if (version === 0xff || /^0+$/.test(traceId) || /^0+$/.test(spanId)) {
      return null;
    }
    return new TraceContext(verHex.toLowerCase(), traceId.toLowerCase(), spanId.toLowerCase(), traceFlags);
  }

  format(): string {
    const ver = this.version.toLowerCase().padStart(2, "0").slice(0, 2);
    const flags = (this.traceFlags & 0xff).toString(16).padStart(2, "0");
    return `${ver}-${this.traceId.toLowerCase()}-${this.spanId.toLowerCase()}-${flags}`;
  }

  static format(ctx: TraceContext): string {
    return ctx.format();
  }

  static injectHeader(
    headers: Record<string, string> | Headers = {},
    ctx?: TraceContext | null,
  ): Record<string, string> {
    const normalized: Record<string, string> = {};
    if (typeof Headers !== "undefined" && headers instanceof Headers) {
      headers.forEach((value, key) => {
        normalized[key] = value;
      });
    } else {
      Object.assign(normalized, headers);
    }
    const child = ctx
      ? new TraceContext(ctx.version, ctx.traceId, randomHex(8), ctx.traceFlags)
      : new TraceContext("00", randomHex(16), randomHex(8), 1);
    return {
      ...normalized,
      traceparent: child.format(),
    };
  }
}
