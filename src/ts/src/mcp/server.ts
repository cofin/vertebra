export type JsonRpcRequest = {
  jsonrpc: "2.0";
  id: string | number | null;
  method: string;
  params?: any;
};

export type JsonRpcResponse = {
  jsonrpc: "2.0";
  id: string | number | null;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
};

export type ToolHandler = (args: any) => Promise<unknown> | unknown;

export interface McpTool {
  name: string;
  description?: string;
  inputSchema?: unknown;
  handler: ToolHandler;
}

export class McpServer {
  private tools = new Map<string, McpTool>();
  private initialized = false;

  registerTool(tool: McpTool): void {
    this.tools.set(tool.name, tool);
  }

  async handle(request: JsonRpcRequest): Promise<JsonRpcResponse> {
    try {
      if (request.method === "initialize") {
        return this.handleInitialize(request);
      }

      if (!this.initialized) {
        return this.error(request.id, -32002, "Server not initialized");
      }

      switch (request.method) {
        case "tools/list":
          return this.handleToolsList(request);
        case "tools/call":
          return await this.handleToolsCall(request);
        case "ping":
          return this.success(request.id, {});
        default:
          return this.error(request.id, -32601, `Method not found: ${request.method}`);
      }
    } catch (error) {
      return this.error(request.id, -32000, "Internal error", String(error));
    }
  }

  private handleInitialize(request: JsonRpcRequest): JsonRpcResponse {
    this.initialized = true;
    return this.success(request.id, {
      protocolVersion: "2024-11-05", // Example version
      capabilities: {
        tools: {},
      },
      serverInfo: {
        name: "vertebra-mcp",
        version: "0.1.0",
      },
    });
  }

  private handleToolsList(request: JsonRpcRequest): JsonRpcResponse {
    const toolsList = Array.from(this.tools.values()).map((t) => ({
      name: t.name,
      description: t.description,
      inputSchema: t.inputSchema,
    }));

    return this.success(request.id, { tools: toolsList });
  }

  private async handleToolsCall(request: JsonRpcRequest): Promise<JsonRpcResponse> {
    const params = request.params as { name: string; arguments?: any };
    if (!params || !params.name) {
      return this.error(request.id, -32602, "Invalid params: 'name' is required");
    }

    const tool = this.tools.get(params.name);
    if (!tool) {
      return this.error(request.id, -32601, `Tool not found: ${params.name}`);
    }

    try {
      const result = await tool.handler(params.arguments || {});
      return this.success(request.id, {
        content: [{ type: "text", text: JSON.stringify(result) }],
      });
    } catch (error) {
      return this.error(request.id, -32000, "Tool execution failed", String(error));
    }
  }

  private success(id: string | number | null, result: unknown): JsonRpcResponse {
    return { jsonrpc: "2.0", id, result };
  }

  private error(
    id: string | number | null,
    code: number,
    message: string,
    data?: unknown,
  ): JsonRpcResponse {
    return {
      jsonrpc: "2.0",
      id,
      error: { code, message, data },
    };
  }
}
