import { Buffer } from "node:buffer";

import type { Server, ServerWebSocket, WebSocketHandler } from "bun";

type StateWebSocketData = {
  sessionId: string;
  lastEventId?: number;
};

type StateEvent = {
  id: number;
  payload: Uint8Array;
};

export type StateWebSocketOptions = {
  path?: string;
  historySize?: number;
  heartbeatMs?: number;
};

type SyncMessage = {
  type: "sync";
  lastEventId?: number;
};

type DeltaMessage = {
  type: "delta";
  payload: string;
};

type PingMessage = {
  type: "ping";
  ts?: number;
};

type IncomingMessage = SyncMessage | DeltaMessage | PingMessage;

export class StateWebSocketServer {
  private readonly sockets = new Map<string, ServerWebSocket<StateWebSocketData>>();
  private readonly heartbeats = new Map<string, Timer>();
  private readonly history: StateEvent[] = [];
  private readonly historySize: number;
  private readonly heartbeatMs: number;
  private readonly path: string;
  private nextId = 1;

  onDelta?: (payload: Uint8Array, sessionId: string) => void;

  constructor(options: StateWebSocketOptions = {}) {
    this.historySize = options.historySize ?? 256;
    this.heartbeatMs = options.heartbeatMs ?? 30000;
    this.path = options.path ?? "/state/ws";
  }

  handleUpgrade(request: Request, server: Server<unknown>): boolean {
    const url = new URL(request.url);
    if (url.pathname !== this.path) {
      return false;
    }

    const lastEventId = url.searchParams.get("lastEventId");
    const data: StateWebSocketData = {
      sessionId: crypto.randomUUID(),
      lastEventId: lastEventId ? Number.parseInt(lastEventId, 10) : undefined,
    };

    return server.upgrade(request, { data });
  }

  handler(): WebSocketHandler<StateWebSocketData> {
    return {
      open: (ws) => this.handleOpen(ws),
      close: (ws) => this.handleClose(ws),
      message: (ws, message) => this.handleMessage(ws, message),
    };
  }

  handleOpen(ws: ServerWebSocket<StateWebSocketData>): void {
    const sessionId = ws.data.sessionId;
    this.sockets.set(sessionId, ws);

    if (ws.data.lastEventId && ws.data.lastEventId > 0) {
      this.replayHistory(ws, ws.data.lastEventId);
    }

    if (this.heartbeatMs > 0) {
      const timer = setInterval(() => {
        try {
          ws.send(JSON.stringify({ type: "ping", ts: Date.now() }));
        } catch {
          this.cleanupSession(sessionId);
        }
      }, this.heartbeatMs);
      this.heartbeats.set(sessionId, timer);
    }
  }

  handleClose(ws: ServerWebSocket<StateWebSocketData>): void {
    this.cleanupSession(ws.data.sessionId);
  }

  handleMessage(ws: ServerWebSocket<StateWebSocketData>, message: string | Buffer | Uint8Array): void {
    const text = typeof message === "string" ? message : Buffer.from(message).toString("utf8");
    let payload: IncomingMessage;
    try {
      payload = JSON.parse(text) as IncomingMessage;
    } catch {
      return;
    }

    if (payload.type === "sync") {
      const lastEventId = payload.lastEventId ?? 0;
      this.replayHistory(ws, lastEventId);
      return;
    }

    if (payload.type === "delta") {
      const bytes = Buffer.from(payload.payload, "base64");
      this.onDelta?.(new Uint8Array(bytes), ws.data.sessionId);
      return;
    }
  }

  publish(payload: Uint8Array): number {
    const event: StateEvent = { id: this.nextId++, payload };
    if (this.historySize > 0) {
      this.history.push(event);
      if (this.history.length > this.historySize) {
        this.history.shift();
      }
    }

    const frame = JSON.stringify({
      type: "delta",
      id: event.id,
      payload: Buffer.from(event.payload).toString("base64"),
    });

    for (const socket of this.sockets.values()) {
      socket.send(frame);
    }
    return event.id;
  }

  close(): void {
    for (const timer of this.heartbeats.values()) {
      clearInterval(timer);
    }
    this.heartbeats.clear();
    for (const socket of this.sockets.values()) {
      socket.close();
    }
    this.sockets.clear();
  }

  private replayHistory(ws: ServerWebSocket<StateWebSocketData>, lastEventId: number): void {
    for (const event of this.history) {
      if (event.id > lastEventId) {
        const frame = JSON.stringify({
          type: "delta",
          id: event.id,
          payload: Buffer.from(event.payload).toString("base64"),
        });
        ws.send(frame);
      }
    }
  }

  private cleanupSession(sessionId: string): void {
    const timer = this.heartbeats.get(sessionId);
    if (timer) {
      clearInterval(timer);
      this.heartbeats.delete(sessionId);
    }
    this.sockets.delete(sessionId);
  }
}
