import { describe, expect, it, mock } from "bun:test";
import { StdioTransport } from "../../src/mcp/transports/stdio";
import { Readable, Writable } from "node:stream";

describe("StdioTransport", () => {
  it("should read JSON-RPC messages from input stream", async () => {
    const input = new Readable({
      read() {}
    });
    const output = new Writable({
      write(chunk, encoding, callback) {
        callback();
      }
    });

    const transport = new StdioTransport(input, output);
    const messages: any[] = [];

    transport.onmessage = (msg) => {
      messages.push(msg);
    };

    await transport.start();

    // Push a complete JSON message
    input.push(JSON.stringify({ jsonrpc: "2.0", method: "test" }) + "\n");
    
    // Wait for processing (microtask)
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(messages).toHaveLength(1);
    expect(messages[0]).toEqual({ jsonrpc: "2.0", method: "test" });
    
    await transport.close();
  });

  it("should write JSON-RPC messages to output stream", async () => {
    const input = new Readable({ read() {} });
    let outputData = "";
    const output = new Writable({
      write(chunk, encoding, callback) {
        outputData += chunk.toString();
        callback();
      }
    });

    const transport = new StdioTransport(input, output);
    await transport.start();

    await transport.send({ jsonrpc: "2.0", result: "success", id: 1 });

    expect(outputData).toContain(JSON.stringify({ jsonrpc: "2.0", result: "success", id: 1 }));
    expect(outputData.endsWith("\n")).toBe(true);

    await transport.close();
  });

  it("should handle malformed JSON gracefully", async () => {
    const input = new Readable({ read() {} });
    const output = new Writable({ write(c, e, cb) { cb(); } });
    
    const transport = new StdioTransport(input, output);
    const errors: any[] = [];
    
    transport.onerror = (err) => errors.push(err);
    
    await transport.start();
    
    // Push invalid JSON
    input.push("{ invalid json }\n");
    
    await new Promise(resolve => setTimeout(resolve, 0));
    
    expect(errors).toHaveLength(1);
    expect(errors[0].message).toContain("Parse error");
    
    await transport.close();
  });
});
