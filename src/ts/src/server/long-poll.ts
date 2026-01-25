import { Buffer } from "node:buffer";

type LongPollEvent = {
  id: number;
  payload: Uint8Array;
};

export type LongPollOptions = {
  path?: string;
  timeoutMs?: number;
  historySize?: number;
};

export class StateLongPollServer {
  private readonly history: LongPollEvent[] = [];
  private readonly waiters = new Map<string, (payload: Uint8Array | null) => void>();
  private readonly path: string;
  private readonly timeoutMs: number;
  private readonly historySize: number;
  private nextId = 1;

  constructor(options: LongPollOptions = {}) {
    this.path = options.path ?? "/state/poll";
    this.timeoutMs = options.timeoutMs ?? 15000;
    this.historySize = options.historySize ?? 256;
  }

  async handle(request: Request): Promise<Response | undefined> {
    const url = new URL(request.url);
    if (url.pathname !== this.path) {
      return;
    }

    const sinceParam = url.searchParams.get("since");
    const since = sinceParam ? Number.parseInt(sinceParam, 10) : 0;

    const pending = this.history.filter(event => event.id > since);
    if (pending.length > 0) {
      return this.buildResponse(pending[pending.length - 1]);
    }

    const waiterId = crypto.randomUUID();
    const payload = await new Promise<Uint8Array | null>(resolve => {
      this.waiters.set(waiterId, resolve);
      setTimeout(() => {
        this.waiters.delete(waiterId);
        resolve(null);
      }, this.timeoutMs);
    });

    if (payload) {
      return this.buildResponse({ id: since + 1, payload });
    }

    return new Response(null, { status: 204 });
  }

  publish(payload: Uint8Array): number {
    const event: LongPollEvent = { id: this.nextId++, payload };
    this.history.push(event);
    if (this.history.length > this.historySize) {
      this.history.shift();
    }

    for (const resolve of this.waiters.values()) {
      resolve(payload);
    }
    this.waiters.clear();
    return event.id;
  }

  private buildResponse(event: LongPollEvent): Response {
    const body = JSON.stringify({
      id: event.id,
      payload: Buffer.from(event.payload).toString("base64"),
    });
    return new Response(body, {
      headers: { "content-type": "application/json", "cache-control": "no-cache" },
    });
  }
}
