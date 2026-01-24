import { describe, expect, it, mock } from "bun:test";
import { McpServer } from "../../src/mcp/server";
import { SSETransport } from "../../src/mcp/transports/sse";
import { WebSocketTransport, type WebSocketData } from "../../src/mcp/transports/websocket";

describe("MCP Transports Integration", () => {
  it("should handle SSE flow", async () => {
    const server = new McpServer();
    const transport = new SSETransport(100);

    transport.onmessage = async (msg, sid) => {
      const res = await server.handle(msg);
      await transport.send(res, sid);
    };

    const res = await transport.handleConnect(new Request("http://localhost/mcp?session=s1"));
    const reader = res.body?.getReader();

    const { value: v1 } = await reader?.read() || {};
    expect(new TextDecoder().decode(v1)).toContain("event: endpoint");

    await transport.handleMessage(new Request("http://localhost/mcp?session=s1", {
      method: "POST",
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize" })
    }));

    const { value: v2 } = await reader?.read() || {};
    const text2 = new TextDecoder().decode(v2);
    expect(text2).toContain("event: message");
    expect(text2).toContain('"id":1');

    await transport.close();
  });

  it("should handle WebSocket flow", async () => {
    const server = new McpServer();
    const transport = new WebSocketTransport();

    transport.onmessage = async (msg, sid) => {
      const res = await server.handle(msg);
      await transport.send(res, sid);
    };

    const mockWs = {
      send: mock((data: string) => {}),
      close: mock(() => {}),
      data: { sessionId: "w1" }
    };

    transport.handleOpen(mockWs as any);
    transport.handleMessage(mockWs as any, JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize"
    }));

    // Response is async
    await new Promise(resolve => setTimeout(resolve, 10));

    expect(mockWs.send).toHaveBeenCalled();
    const lastCall = (mockWs.send as any).mock.calls[0][0];
    const res = JSON.parse(lastCall);
    expect(res.id).toBe(1);

    await transport.close();
  });
});
