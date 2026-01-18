export type JsonRpcRequest = {
  jsonrpc: "2.0";
  id: string | number | null;
  method: string;
  params?: unknown;
};

export type JsonRpcResponse = {
  jsonrpc: "2.0";
  id: string | number | null;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
};

export type ToolHandler = (params: unknown) => Promise<unknown> | unknown;

export interface McpTool {
  name: string;
  description?: string;
  inputSchema?: unknown;
  outputSchema?: unknown;
  handler: ToolHandler;
}

export class McpServer {
  private tools = new Map<string, McpTool>();

  registerTool(tool: McpTool): void {
    this.tools.set(tool.name, tool);
  }

  async handle(request: JsonRpcRequest): Promise<JsonRpcResponse> {
    const tool = this.tools.get(request.method);
    if (!tool) {
      return {
        jsonrpc: "2.0",
        id: request.id ?? null,
        error: { code: -32601, message: "Method not found" },
      };
    }
    try {
      const result = await tool.handler(request.params);
      return { jsonrpc: "2.0", id: request.id ?? null, result };
    } catch (error) {
      return {
        jsonrpc: "2.0",
        id: request.id ?? null,
        error: { code: -32000, message: "Tool execution failed", data: String(error) },
      };
    }
  }
}
