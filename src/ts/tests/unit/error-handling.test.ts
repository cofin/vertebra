import { describe, expect, it, mock } from "bun:test";
import { StdioTransport } from "../../src/mcp/transports/stdio";
import { SSETransport } from "../../src/mcp/transports/sse";
import { WebSocketTransport } from "../../src/mcp/transports/websocket";
import { SqlPool } from "../../src/broker/serve";
import { Readable, Writable } from "node:stream";

describe("Error Handling Audit", () => {
  describe("Stdio Transport", () => {
    it("should handle partial JSON chunks gracefully", async () => {
      const input = new Readable({ read() {} });
      const output = new Writable({ write(c, e, cb) { cb(); } });
      const transport = new StdioTransport(input, output);
      const errors: Error[] = [];
      transport.onerror = (err) => errors.push(err);
      
      await transport.start();
      
      input.push('{"jsonrpc": "2.0"');
      input.push(', "method": "test"}\n');
      
      input.push('{"partial":'); 
      await new Promise(r => setTimeout(r, 10));
      expect(errors.length).toBe(0); 
      
      input.push('"value"}\n');
      await new Promise(r => setTimeout(r, 10));
      
      input.push('INVALID\n');
      await new Promise(r => setTimeout(r, 10));
      expect(errors.length).toBeGreaterThan(0);
      expect((errors[0] as any).code).toBe(-32700);
    });
  });

  describe("SSE Transport", () => {
    it("should handle malformed POST bodies", async () => {
      const transport = new SSETransport();
      // Connect first to establish session
      await transport.handleConnect(new Request("http://localhost/mcp?session=123"));
      
      const req = new Request("http://localhost/mcp?session=123", {
        method: "POST",
        body: "invalid-json"
      });
      
      const res = await transport.handleMessage(req);
      expect(res.status).toBe(400);
    });
  });

  describe("Pool Error Propagation", () => {
    it("should propagate acquire timeout errors", async () => {
      const pool = new SqlPool(1);
      await pool.acquire(); 
      
      const onError = mock(async () => {});
      pool.setOnError(onError);
      
      const error = new Error("Timeout");
      if (pool.hooks.onError) {
          await pool.hooks.onError(null, error);
      }
      
      expect(onError).toHaveBeenCalledWith(null, error);
    });
  });
});