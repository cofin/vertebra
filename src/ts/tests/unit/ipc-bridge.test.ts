import { describe, expect, it, mock } from "bun:test";
import { MCPIPCBridge } from "../../src/mcp/ipc-bridge";
import { encode, decode } from "@msgpack/msgpack";

describe("MCPIPCBridge", () => {
  it("should send tool call and receive result", async () => {
    // Mock transport
    const mockTransport = {
      send: mock(async (payload: Uint8Array) => {
        const req = decode(payload) as any;
        expect(req.type).toBe("tool_call");
        expect(req.name).toBe("test_tool");

        return encode({ result: "success" });
      })
    };

    const bridge = new MCPIPCBridge(mockTransport);
    const result = await bridge.callTool("test_tool", { arg: 1 });

    expect(mockTransport.send).toHaveBeenCalled();
    expect(result).toEqual("success");
  });

  it("should handle transport errors", async () => {
    const mockTransport = {
      send: mock(async () => {
        throw new Error("Transport failed");
      })
    };

    const bridge = new MCPIPCBridge(mockTransport);

    let error;
    try {
        await bridge.callTool("test_tool", {});
    } catch (e) {
        error = e;
    }
    expect(error).toBeDefined();
    expect(error.message).toBe("Transport failed");
  });

  it("should handle remote errors", async () => {
    const mockTransport = {
      send: mock(async () => {
        return encode({ error: "Remote execution failed" });
      })
    };

    const bridge = new MCPIPCBridge(mockTransport);

    let error;
    try {
        await bridge.callTool("test_tool", {});
    } catch (e) {
        error = e;
    }
    expect(error).toBeDefined();
    expect(error.message).toBe("Remote execution failed");
  });
});
