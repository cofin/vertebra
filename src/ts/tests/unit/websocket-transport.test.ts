import { describe, expect, it, mock } from "bun:test";
import { WebSocketTransport } from "../../src/mcp/transports/websocket";

describe("WebSocketTransport", () => {
  it("should handle incoming messages", async () => {
    const transport = new WebSocketTransport();
    const messages: any[] = [];
    transport.onmessage = (msg, sid) => messages.push({ msg, sid });

    // Mock WebSocket object
    const ws = {
      send: mock(() => {}),
      data: { sessionId: "ws1" }
    };

    transport.handleMessage(ws as any, JSON.stringify({ jsonrpc: "2.0", method: "test" }));

    expect(messages).toHaveLength(1);
    expect(messages[0].msg).toEqual({ jsonrpc: "2.0", method: "test" });
    expect(messages[0].sid).toBe("ws1");
  });

  it("should send outgoing messages", async () => {
    const transport = new WebSocketTransport();
    const ws = {
      send: mock(() => {}),
      data: { sessionId: "ws1" }
    };

    transport.handleOpen(ws as any);
    await transport.send({ id: 1 }, "ws1");

    expect(ws.send).toHaveBeenCalledWith(JSON.stringify({ id: 1 }));
  });
});
