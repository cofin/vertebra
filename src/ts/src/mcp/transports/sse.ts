import type { Transport } from "./stdio";

export class SSETransport implements Transport {
  private sessions = new Map<string, ReadableStreamDefaultController>();
  private keepAliveIntervals = new Map<string, Timer>();

  onmessage?: (message: any, sessionId?: string) => void;
  onerror?: (error: Error) => void;

  constructor(private keepAliveIntervalMs: number = 30000) {}

  async start(): Promise<void> {}

  async close(): Promise<void> {
    for (const timer of this.keepAliveIntervals.values()) {
      clearInterval(timer);
    }
    this.keepAliveIntervals.clear();

    for (const controller of this.sessions.values()) {
      try {
        controller.close();
      } catch (e) {}
    }
    this.sessions.clear();
  }

  async send(message: any, sessionId?: string): Promise<void> {
    if (!sessionId) {
      throw new Error("sessionId is required for SSE transport");
    }
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new Error(`Session ${sessionId} not found`);
    }

    const data = `event: message\ndata: ${JSON.stringify(message)}\n\n`;
    session.enqueue(new TextEncoder().encode(data));
  }

  async handleConnect(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const sessionId = url.searchParams.get("session") || crypto.randomUUID();

    const stream = new ReadableStream({
      start: (controller) => {
        this.sessions.set(sessionId, controller);

        const endpoint = `${url.pathname}?session=${sessionId}`;
        const data = `event: endpoint\ndata: ${endpoint}\n\n`;
        controller.enqueue(new TextEncoder().encode(data));

        // Start keep-alive
        const timer = setInterval(() => {
          try {
            // SSE comment starting with colon is ignored by clients but keeps connection open
            controller.enqueue(new TextEncoder().encode(": keep-alive\n\n"));
          } catch (e) {
            this.cleanupSession(sessionId);
          }
        }, this.keepAliveIntervalMs);
        this.keepAliveIntervals.set(sessionId, timer);
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

  private cleanupSession(sessionId: string) {
    const timer = this.keepAliveIntervals.get(sessionId);
    if (timer) {
      clearInterval(timer);
      this.keepAliveIntervals.delete(sessionId);
    }
    this.sessions.delete(sessionId);
  }

  async handleMessage(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const sessionId = url.searchParams.get("session");
    if (!sessionId || !this.sessions.has(sessionId)) {
      return new Response("Session not found", { status: 404 });
    }

    try {
      const body = await request.json();
      this.onmessage?.(body, sessionId);
      return new Response(null, { status: 202 });
    } catch (error) {
      return new Response("Invalid JSON", { status: 400 });
    }
  }
}
