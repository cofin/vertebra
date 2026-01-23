import { describe, expect, it } from "bun:test";
import { McpServer } from "../../src/mcp/server";
import { StdioTransport } from "../../src/mcp/transports/stdio";
import { Readable, Writable } from "node:stream";

describe("MCP Integration", () => {
  it("should handle full request/response cycle via StdioTransport", async () => {
    // Setup
    const input = new Readable({ read() {} });
    const outputBuffer: string[] = [];
    const output = new Writable({
      write(chunk, encoding, callback) {
        outputBuffer.push(chunk.toString());
        callback();
      }
    });

    const server = new McpServer();
    const transport = new StdioTransport(input, output);

    // Wire up: This logic would usually be in the entry point
    transport.onmessage = async (msg) => {
      const response = await server.handle(msg);
      await transport.send(response);
    };
    
    await transport.start();

    // 1. Initialize
    input.push(JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { protocolVersion: "2024-11-05" }
    }) + "\n");

    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 10));

    expect(outputBuffer.length).toBeGreaterThan(0);
    const initRes = JSON.parse(outputBuffer.shift()!); // The ! is fine here
    expect(initRes.id).toBe(1);
    expect(initRes.result.protocolVersion).toBeDefined();

    // 2. Register Tool
    server.registerTool({
      name: "echo",
      description: "Echoes back the arguments",
      inputSchema: { type: "object" },
      handler: async (args) => args
    });

    // 3. Call Tool
    input.push(JSON.stringify({
      jsonrpc: "2.0",
      id: 2,
      method: "tools/call",
      params: {
        name: "echo",
        arguments: { hello: "world" }
      }
    }) + "\n");

    await new Promise(resolve => setTimeout(resolve, 10));

    expect(outputBuffer.length).toBeGreaterThan(0);
    const callRes = JSON.parse(outputBuffer.shift()!); // The ! is fine here
    expect(callRes.id).toBe(2);
    expect(callRes.result.content[0].text).toContain("world");

    await transport.close();
  });
});
