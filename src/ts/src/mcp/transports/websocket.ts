import type { ServerWebSocket } from "bun";
import type { Transport } from "./stdio";

export type WebSocketData = {
  sessionId: string;
};

export class WebSocketTransport implements Transport {
  private sockets = new Map<string, ServerWebSocket<WebSocketData>>();

  onmessage?: (message: any, sessionId?: string) => void;
  onerror?: (error: Error) => void;

  async start(): Promise<void> {}

  async close(): Promise<void> {
    for (const socket of this.sockets.values()) {
      socket.close();
    }
    this.sockets.clear();
  }

  async send(message: any, sessionId?: string): Promise<void> {
    if (!sessionId) {
      throw new Error("sessionId is required for WebSocket transport");
    }
    const socket = this.sockets.get(sessionId);
    if (!socket) {
      throw new Error(`Socket session ${sessionId} not found`);
    }

    socket.send(JSON.stringify(message));
  }

  handleOpen(ws: ServerWebSocket<WebSocketData>) {
    const sessionId = ws.data.sessionId;
    this.sockets.set(sessionId, ws);
  }

  handleClose(ws: ServerWebSocket<WebSocketData>) {
    this.sockets.delete(ws.data.sessionId);
  }

  handleMessage(ws: ServerWebSocket<WebSocketData>, message: string | Buffer) {
    try {
      const data = typeof message === "string" ? message : message.toString();
      const body = JSON.parse(data);
      this.onmessage?.(body, ws.data.sessionId);
    } catch (error) {
      this.onerror?.(new Error(`Invalid JSON: ${error}`));
    }
  }
}
