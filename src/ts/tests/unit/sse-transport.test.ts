import { describe, expect, it, mock } from "bun:test";
import { SSETransport } from "../../src/mcp/transports/sse";

describe("SSETransport", () => {
  it("should return SSE stream on connect", async () => {
    const transport = new SSETransport();
    const connectReq = new Request("http://localhost/mcp?session=123", { method: "GET" });
    const response = await transport.handleConnect(connectReq);
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/event-stream");
    const reader = response.body?.getReader();
    const { value } = await reader?.read() || {};
    const text = new TextDecoder().decode(value);
    expect(text).toContain("event: endpoint");
    expect(text).toContain("data: /mcp?session=123");
    transport.close();
  });

  it("should receive messages via POST and pass sessionId", async () => {
    const transport = new SSETransport();
    const messages: any[] = [];
    transport.onmessage = (msg, sid) => messages.push({ msg, sid });
    await transport.handleConnect(new Request("http://localhost/mcp?session=123"));
    const postReq = new Request("http://localhost/mcp?session=123", {
      method: "POST",
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "test" })
    });
    const response = await transport.handleMessage(postReq);
    expect(response.status).toBe(202);
    expect(messages).toHaveLength(1);
    expect(messages[0]).toEqual({
      msg: { jsonrpc: "2.0", id: 1, method: "test" },
      sid: "123"
    });
    transport.close();
  });

  it("should send responses to specific session", async () => {
    const transport = new SSETransport();
    
    const res1 = await transport.handleConnect(new Request("http://localhost/mcp?session=c1"));
    const reader1 = res1.body?.getReader();
    await reader1?.read();

    const res2 = await transport.handleConnect(new Request("http://localhost/mcp?session=c2"));
    const reader2 = res2.body?.getReader();
    await reader2?.read();

    await transport.send({ id: 1 }, "c1");
    const { value: v1 } = await reader1?.read() || {};
    expect(new TextDecoder().decode(v1)).toContain('{"id":1}');

    await transport.send({ id: 2 }, "c2");
    const { value: v2 } = await reader2?.read() || {};
    expect(new TextDecoder().decode(v2)).toContain('{"id":2}');
    transport.close();
  });

  it("should send keep-alive messages", async () => {
    const transport = new SSETransport(10); // 10ms interval
    const connectReq = new Request("http://localhost/mcp?session=123");
    const response = await transport.handleConnect(connectReq);
    const reader = response.body?.getReader();

    await reader?.read(); // endpoint event

    // Wait for keep-alive
    const { value } = await reader?.read() || {};
    const text = new TextDecoder().decode(value);
    expect(text).toBe(": keep-alive\n\n");
    
    transport.close();
  });
});