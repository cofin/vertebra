import { encodeMcpHeaderValue, MCP_PROTOCOL_VERSION } from "./agent/index";

export interface WebMCPToolDescriptor {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
  endpoint?: string;
  requiresConfirmation?: boolean;
  annotations?: Record<string, unknown>;
}

export interface WebMCPManifest {
  protocolVersion: string;
  mcpEndpoint: string;
  tools: WebMCPToolDescriptor[];
}

export interface BrowserModelContextTool {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
  annotations?: Record<string, unknown>;
  execute: (args: Record<string, unknown>) => Promise<unknown>;
}

export interface BrowserModelContext {
  registerTool?: (tool: BrowserModelContextTool) => void | Promise<void>;
  unregisterTool?: (name: string) => void | Promise<void>;
  provideContext?: (context: { tools: BrowserModelContextTool[] }) => void | Promise<void>;
}

export interface RegisterWebMCPOptions {
  manifestUrl?: string;
  fetchImpl?: typeof fetch;
  modelContext?: BrowserModelContext | null;
  headers?: Record<string, string>;
}

function getBrowserModelContext(explicit?: BrowserModelContext | null): BrowserModelContext | null {
  if (explicit !== undefined) {
    return explicit;
  }
  if (typeof navigator !== "undefined" && "modelContext" in navigator) {
    return (navigator as unknown as { modelContext?: BrowserModelContext }).modelContext ?? null;
  }
  return null;
}

function isWebMCPManifest(value: WebMCPManifest | RegisterWebMCPOptions | undefined): value is WebMCPManifest {
  return value !== undefined && "tools" in value && Array.isArray((value as WebMCPManifest).tools);
}

/**
 * Register server-exposed MCP tools (`/.well-known/webmcp.json` or an explicit `WebMCPManifest`)
 * with the browser's W3C WebMCP `navigator.modelContext` API.
 */
export async function registerWebMCPTools(
  manifestOrOptions?: WebMCPManifest | RegisterWebMCPOptions,
  options?: RegisterWebMCPOptions,
): Promise<BrowserModelContextTool[]> {
  const hasManifest = isWebMCPManifest(manifestOrOptions);
  const resolvedOptions: RegisterWebMCPOptions =
    (hasManifest ? options : (manifestOrOptions as RegisterWebMCPOptions | undefined)) ?? {};
  const fetchFn = resolvedOptions.fetchImpl ?? globalThis.fetch;

  let manifest: WebMCPManifest;
  if (hasManifest) {
    manifest = manifestOrOptions;
  } else {
    const url = resolvedOptions.manifestUrl ?? "/.well-known/webmcp.json";
    const resp = await fetchFn(url, {
      method: "GET",
      headers: { Accept: "application/json", ...(resolvedOptions.headers ?? {}) },
    });
    if (!resp.ok) {
      throw new Error(`Failed to fetch WebMCP manifest from ${url}: HTTP ${resp.status}`);
    }
    manifest = (await resp.json()) as WebMCPManifest;
  }

  const defaultEndpoint = manifest.mcpEndpoint || "/mcp";
  const registeredTools: BrowserModelContextTool[] = manifest.tools.map((descriptor) => {
    const endpoint = descriptor.endpoint || defaultEndpoint;
    return {
      name: descriptor.name,
      description: descriptor.description,
      inputSchema: descriptor.inputSchema,
      ...(descriptor.annotations ? { annotations: descriptor.annotations } : {}),
      execute: async (args: Record<string, unknown>): Promise<unknown> => {
        const rpcId = `webmcp-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const resp = await fetchFn(endpoint, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
            "Mcp-Method": "tools/call",
            "Mcp-Name": encodeMcpHeaderValue(descriptor.name),
            ...(resolvedOptions.headers ?? {}),
          },
          body: JSON.stringify({
            jsonrpc: "2.0",
            id: rpcId,
            method: "tools/call",
            params: {
              name: descriptor.name,
              arguments: args ?? {},
            },
          }),
        });
        if (!resp.ok) {
          throw new Error(`WebMCP tool call '${descriptor.name}' failed with HTTP ${resp.status}`);
        }
        const payload = (await resp.json()) as {
          result?: unknown;
          error?: { code: number; message: string };
        };
        if (payload.error) {
          throw new Error(`WebMCP tool error (${payload.error.code}): ${payload.error.message}`);
        }
        return payload.result;
      },
    };
  });

  const ctx = getBrowserModelContext(resolvedOptions.modelContext);
  if (ctx) {
    if (typeof ctx.registerTool === "function") {
      for (const tool of registeredTools) {
        await ctx.registerTool(tool);
      }
    } else if (typeof ctx.provideContext === "function") {
      await ctx.provideContext({ tools: registeredTools });
    }
  }

  return registeredTools;
}
