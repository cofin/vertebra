import { expect, test } from "bun:test";
import { McpServer, type JsonRpcRequest } from "../src/mcp/server";

test("McpServer initialization", async () => {
  const server = new McpServer();
  const req: JsonRpcRequest = {
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: {
        protocolVersion: "2024-11-05",
        capabilities: {},
        clientInfo: { name: "test-client", version: "1.0" }
    },
  };

  const res = await server.handle(req);
  expect(res.error).toBeUndefined();
  expect(res.result).toMatchObject({
    serverInfo: { name: "vertebra-mcp" },
  });
});

test("McpServer tools flow", async () => {
  const server = new McpServer();
  
  // Register a dummy tool
  server.registerTool({
    name: "hello",
    description: "Say hello",
    inputSchema: { type: "object", properties: { name: { type: "string" } } },
    handler: async (args: any) => `Hello, ${args.name}!`,
  });

  // Initialize
  await server.handle({ jsonrpc: "2.0", id: 1, method: "initialize" });

  // List tools
  const listReq: JsonRpcRequest = { jsonrpc: "2.0", id: 2, method: "tools/list" };
  const listRes = await server.handle(listReq);
  
  expect(listRes.error).toBeUndefined();
  expect((listRes.result as any).tools).toHaveLength(1);
  expect((listRes.result as any).tools[0].name).toBe("hello");

  // Call tool
  const callReq: JsonRpcRequest = {
    jsonrpc: "2.0",
    id: 3,
    method: "tools/call",
    params: { name: "hello", arguments: { name: "World" } },
  };
  const callRes = await server.handle(callReq);

  expect(callRes.error).toBeUndefined();
  // MCP expects content array
  const content = (callRes.result as any).content;
  expect(content).toBeDefined();
  expect(content[0].text).toContain("Hello, World!");
});
