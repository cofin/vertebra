import { describe, expect, it, mock } from "bun:test";
import { McpServer, type McpTool } from "../../src/mcp/server";
import { SchemaRegistry } from "../../src/mcp/schema-registry";
import { MCPIPCBridge } from "../../src/mcp/ipc-bridge";

describe("McpServer", () => {
  it("should handle initialize request", async () => {
    const server = new McpServer();
    const response = await server.handle({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { protocolVersion: "2024-11-05" }
    });

    expect(response.error).toBeUndefined();
    expect(response.result).toBeDefined();
    const result = response.result as any;
    expect(result.protocolVersion).toBeDefined();
    expect(result.capabilities).toBeDefined();
    expect(result.capabilities.tools).toBeDefined();
  });

  it("should reject methods before initialization", async () => {
    const server = new McpServer();
    const response = await server.handle({
      jsonrpc: "2.0",
      id: 1,
      method: "tools/list"
    });

    expect(response.error).toBeDefined();
    expect(response.error?.code).toBe(-32002);
  });

  it("should list registered tools", async () => {
    const server = new McpServer();
    await server.handle({ jsonrpc: "2.0", id: 1, method: "initialize" });

    const tool: McpTool = {
      name: "test_tool",
      description: "A test tool",
      inputSchema: { type: "object" },
      handler: async () => ({ status: "ok" })
    };
    server.registerTool(tool);

    const response = await server.handle({
      jsonrpc: "2.0",
      id: 2,
      method: "tools/list"
    });

    expect(response.result).toBeDefined();
    const result = response.result as any;
    expect(result.tools).toHaveLength(1);
    expect(result.tools[0].name).toBe("test_tool");
  });

  it("should call a tool successfully", async () => {
    const server = new McpServer();
    await server.handle({ jsonrpc: "2.0", id: 1, method: "initialize" });

    const handler = mock(async (args: any) => ({ result: args.value * 2 }));
    const tool: McpTool = {
      name: "double",
      handler
    };
    server.registerTool(tool);

    const response = await server.handle({
      jsonrpc: "2.0",
      id: 2,
      method: "tools/call",
      params: {
        name: "double",
        arguments: { value: 21 }
      }
    });

    expect(handler).toHaveBeenCalled();
    expect(handler.mock.calls[0][0]).toEqual({ value: 21 });
    expect(response.result).toBeDefined();
    const result = response.result as any;
    expect(result.content[0].text).toContain("42");
  });

  it("should return error for unknown tool", async () => {
    const server = new McpServer();
    await server.handle({ jsonrpc: "2.0", id: 1, method: "initialize" });

    const response = await server.handle({
      jsonrpc: "2.0",
      id: 2,
      method: "tools/call",
      params: {
        name: "unknown_tool",
        arguments: {}
      }
    });

    expect(response.error).toBeDefined();
    expect(response.error?.code).toBe(-32601);
  });

  it("should return error for invalid params", async () => {
    const server = new McpServer();
    await server.handle({ jsonrpc: "2.0", id: 1, method: "initialize" });

    const response = await server.handle({
      jsonrpc: "2.0",
      id: 2,
      method: "tools/call",
      params: {} // Missing name
    });

    expect(response.error).toBeDefined();
    expect(response.error?.code).toBe(-32602);
  });

  it("should return error when tool execution fails", async () => {
    const server = new McpServer();
    await server.handle({ jsonrpc: "2.0", id: 1, method: "initialize" });

    const tool: McpTool = {
      name: "failing_tool",
      handler: async () => { throw new Error("Boom"); }
    };
    server.registerTool(tool);

    const response = await server.handle({
      jsonrpc: "2.0",
      id: 2,
      method: "tools/call",
      params: { name: "failing_tool" }
    });

    expect(response.error).toBeDefined();
    expect(response.error?.code).toBe(-32000);
    expect(response.error?.message).toContain("Tool execution failed");
  });

  it("should sync tools from registry", async () => {
    // Mock registry
    const registry = {
        getTools: () => [{ name: "remote_tool", description: "Remote", inputSchema: {} }],
        getResources: () => []
    } as any;
    
    // Mock bridge
    const bridge = {
        callTool: mock(async () => "remote_result")
    } as any;

    const server = new McpServer(registry, bridge);
    await server.handle({ jsonrpc: "2.0", id: 1, method: "initialize" });
    
    server.syncFromRegistry();
    
    // Check list
    const listRes = await server.handle({ jsonrpc: "2.0", id: 2, method: "tools/list" });
    const tools = (listRes.result as any).tools;
    expect(tools).toHaveLength(1);
    expect(tools[0].name).toBe("remote_tool");
    
    // Check call
    const callRes = await server.handle({
        jsonrpc: "2.0", id: 3, method: "tools/call",
        params: { name: "remote_tool", arguments: { foo: "bar" } }
    });
    
    expect(bridge.callTool).toHaveBeenCalled();
    expect(bridge.callTool.mock.calls[0]).toEqual(["remote_tool", { foo: "bar" }]);
    expect((callRes.result as any).content[0].text).toContain("remote_result");
  });
});