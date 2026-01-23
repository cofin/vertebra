import { expect, test, afterAll } from "bun:test";
import { McpServer } from "../src/mcp/server";
import { SSETransport } from "../src/mcp/transports/sse";
import { WebSocketTransport } from "../src/mcp/transports/websocket";

const server = new McpServer();
const sse = new SSETransport();
const wsTransport = new WebSocketTransport();

sse.onmessage = async (message, sessionId) => {
  const response = await server.handle(message, sessionId);
  if (response) {
    await sse.send(response, sessionId);
  }
};

wsTransport.onmessage = async (message, sessionId) => {
  const response = await server.handle(message, sessionId);
  if (response) {
    await wsTransport.send(response, sessionId);
  }
};

// Start a test server
const app = Bun.serve({
  port: 0, // Random port
  fetch(req, server) {
    const url = new URL(req.url);
    if (url.pathname === "/sse") return sse.handleConnect(req);
    if (url.pathname === "/mcp/message") return sse.handleMessage(req);
    if (url.pathname === "/ws") {
      const sessionId = crypto.randomUUID();
      if (server.upgrade(req, { data: { sessionId } })) return undefined;
      return new Response("Upgrade failed", { status: 500 });
    }
    return new Response("Not found", { status: 404 });
  },
  websocket: {
    open(ws) {
      wsTransport.handleOpen(ws);
    },
    message(ws, message) {
      wsTransport.handleMessage(ws, message);
    },
    close(ws) {
      wsTransport.handleClose(ws);
    },
  },
});

test("SSE Transport Flow", async () => {
  // 1. Connect to SSE
  const resp = await fetch(`http://localhost:${app.port}/sse`);
  expect(resp.status).toBe(200);
  const reader = resp.body?.getReader();
  expect(reader).toBeDefined();

  // Read first event (endpoint)
  const { value } = await reader!.read();
  const text = new TextDecoder().decode(value);

  expect(text).toContain("event: endpoint");

  // Extract sessionId
  const match = text.match(/session=([a-f0-9-]+)/);
  expect(match).not.toBeNull();
  const sessionId = match![1];

  // 2. Send Message
  const postResp = await fetch(`http://localhost:${app.port}/mcp/message?session=${sessionId}`, {
    method: "POST",
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {},
    }),
  });
  expect(postResp.status).toBe(202);

  // Read response from SSE stream
  const { value: val2 } = await reader!.read();
  const text2 = new TextDecoder().decode(val2);
  expect(text2).toContain("event: message");
  expect(text2).toContain("vertebra-mcp");

  reader!.cancel();
});

test("WebSocket Transport Flow", async () => {
  // Bun test doesn't have a native WebSocket client yet for tests?
  // Using standard WebSocket if available or skipped if not easy in test env.
  // Assuming Bun environment has WebSocket global.

  const ws = new WebSocket(`ws://localhost:${app.port}/ws`);

  await new Promise<void>((resolve) => {
    ws.onopen = () => resolve();
  });

  const msgPromise = new Promise<string>((resolve) => {
    ws.onmessage = (event) => resolve(event.data.toString());
  });

  ws.send(
    JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {},
    })
  );

  const response = await msgPromise;
  const json = JSON.parse(response);

  expect(json.result).toBeDefined();
  expect(json.result.serverInfo.name).toBe("vertebra-mcp");

  ws.close();
});

afterAll(() => {
  app.stop();
});
