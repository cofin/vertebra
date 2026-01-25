import { Buffer } from "node:buffer";

type SseEvent = {
  id: number;
  payload: Uint8Array;
};

export type StateSseOptions = {
  historySize?: number;
  keepAliveMs?: number;
};

export class StateSseServer {
  private readonly sessions = new Map<string, ReadableStreamDefaultController<Uint8Array>>();
  private readonly keepAliveTimers = new Map<string, Timer>();
  private readonly history: SseEvent[] = [];
  private readonly historySize: number;
  private readonly keepAliveMs: number;
  private nextId = 1;

  constructor(options: StateSseOptions = {}) {
    this.historySize = options.historySize ?? 256;
    this.keepAliveMs = options.keepAliveMs ?? 30000;
  }

  handle(request: Request): Response {
    const lastEventIdHeader = request.headers.get("Last-Event-ID");
    const lastEventId = lastEventIdHeader ? Number.parseInt(lastEventIdHeader, 10) : 0;
    const sessionId = crypto.randomUUID();

    const stream = new ReadableStream<Uint8Array>({
      start: (controller) => {
        this.sessions.set(sessionId, controller);

        if (!Number.isNaN(lastEventId) && lastEventId > 0) {
          for (const event of this.history) {
            if (event.id > lastEventId) {
              controller.enqueue(this.formatEvent(event));
            }
          }
        }

        if (this.keepAliveMs > 0) {
          const timer = setInterval(() => {
            try {
              controller.enqueue(new TextEncoder().encode(": keep-alive\n\n"));
            } catch {
              this.cleanupSession(sessionId);
            }
          }, this.keepAliveMs);
          this.keepAliveTimers.set(sessionId, timer);
        }
      },
      cancel: () => {
        this.cleanupSession(sessionId);
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      },
    });
  }

  publish(payload: Uint8Array): number {
    const event: SseEvent = { id: this.nextId++, payload };
    if (this.historySize > 0) {
      this.history.push(event);
      if (this.history.length > this.historySize) {
        this.history.shift();
      }
    }

    const frame = this.formatEvent(event);
    for (const controller of this.sessions.values()) {
      controller.enqueue(frame);
    }
    return event.id;
  }

  close(): void {
    for (const timer of this.keepAliveTimers.values()) {
      clearInterval(timer);
    }
    this.keepAliveTimers.clear();
    for (const controller of this.sessions.values()) {
      try {
        controller.close();
      } catch {}
    }
    this.sessions.clear();
  }

  private cleanupSession(sessionId: string): void {
    const timer = this.keepAliveTimers.get(sessionId);
    if (timer) {
      clearInterval(timer);
      this.keepAliveTimers.delete(sessionId);
    }
    this.sessions.delete(sessionId);
  }

  private formatEvent(event: SseEvent): Uint8Array {
    const payload = Buffer.from(event.payload).toString("base64");
    const text = `id: ${event.id}\nevent: state\ndata: ${payload}\n\n`;
    return new TextEncoder().encode(text);
  }
}
