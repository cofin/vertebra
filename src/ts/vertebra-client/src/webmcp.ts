import { encodeMcpHeaderValue, MCP_PROTOCOL_VERSION, parseMcpJsonRpcResponse } from "./agent/index";

export interface WebMCPToolDescriptor {
  name: string;
  title?: string;
  description: string;
  inputSchema: Record<string, unknown>;
  outputSchema?: Record<string, unknown>;
  endpoint?: string;
  requiresConfirmation?: boolean;
  annotations?: Record<string, unknown>;
}

export interface WebMCPManifest {
  protocolVersion: string;
  mcpEndpoint: string;
  tools: WebMCPToolDescriptor[];
}

export interface WebMCPConfirmationRequest {
  readonly tool: WebMCPToolDescriptor;
  readonly args: Record<string, unknown>;
}

export type WebMCPConfirmationHandler = (request: WebMCPConfirmationRequest) => boolean | Promise<boolean>;

export interface BrowserModelContextExecuteOptions {
  signal?: AbortSignal;
  requestUserInteraction?: (callback: () => Promise<boolean> | boolean) => Promise<boolean>;
}

export interface BrowserModelContextTool {
  name: string;
  title?: string;
  description: string;
  inputSchema: Record<string, unknown>;
  outputSchema?: Record<string, unknown>;
  annotations?: Record<string, unknown>;
  execute: (args: Record<string, unknown>, options?: BrowserModelContextExecuteOptions) => Promise<unknown>;
}

export interface BrowserModelContextRegisterOptions {
  signal?: AbortSignal;
}

export interface BrowserModelContext {
  registerTool?: (tool: BrowserModelContextTool, options?: BrowserModelContextRegisterOptions) => void | Promise<void>;
  unregisterTool?: (name: string) => void | Promise<void>;
}

export interface RegisterWebMCPOptions {
  manifestUrl?: string;
  fetchImpl?: typeof fetch;
  modelContext?: BrowserModelContext | null;
  headers?: Record<string, string>;
  signal?: AbortSignal;
  onConfirm?: WebMCPConfirmationHandler;
}

export interface WebMCPRegistrationError {
  name: string;
  error: unknown;
}

export interface WebMCPRegistrationHandle {
  tools: BrowserModelContextTool[];
  errors: WebMCPRegistrationError[];
  dispose: () => void;
}

const activeContextRegistrations = new WeakMap<BrowserModelContext, Map<string, AbortController>>();

function getBrowserModelContext(explicit?: BrowserModelContext | null): BrowserModelContext | null {
  if (explicit !== undefined) {
    return explicit;
  }
  const globalRecord = globalThis as Record<string, unknown>;
  const doc = globalRecord.document as { modelContext?: BrowserModelContext } | undefined;
  if (doc !== undefined && doc !== null && typeof doc === "object" && "modelContext" in doc && doc.modelContext) {
    return doc.modelContext;
  }
  if (typeof navigator !== "undefined" && "modelContext" in navigator) {
    return (navigator as unknown as { modelContext?: BrowserModelContext }).modelContext ?? null;
  }
  return null;
}

function isWebMCPManifest(value: WebMCPManifest | RegisterWebMCPOptions | undefined): value is WebMCPManifest {
  return value !== undefined && "tools" in value && Array.isArray((value as WebMCPManifest).tools);
}

async function resolveConfirmation(
  descriptor: WebMCPToolDescriptor,
  args: Record<string, unknown>,
  onConfirm?: WebMCPConfirmationHandler,
  execOptions?: BrowserModelContextExecuteOptions,
): Promise<boolean> {
  if (onConfirm) {
    return Boolean(await onConfirm({ tool: descriptor, args }));
  }
  const confirmFn = typeof globalThis.confirm === "function" ? globalThis.confirm : null;
  if (!confirmFn) {
    return false;
  }
  const promptMessage = `Allow tool '${descriptor.title ?? descriptor.name}' to execute?`;
  if (typeof execOptions?.requestUserInteraction === "function") {
    return Boolean(await execOptions.requestUserInteraction(() => confirmFn(promptMessage)));
  }
  return Boolean(confirmFn(promptMessage));
}

/**
 * Register server-exposed MCP tools (`/.well-known/webmcp.json` or an explicit `WebMCPManifest`)
 * with the browser's W3C WebMCP `document.modelContext` or `navigator.modelContext` API.
 */
export async function registerWebMCPTools(
  manifestOrOptions?: WebMCPManifest | RegisterWebMCPOptions,
  options?: RegisterWebMCPOptions,
): Promise<WebMCPRegistrationHandle> {
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
      ...(resolvedOptions.signal ? { signal: resolvedOptions.signal } : {}),
    });
    if (!resp.ok) {
      throw new Error(`Failed to fetch WebMCP manifest from ${url}: HTTP ${resp.status}`);
    }
    manifest = (await resp.json()) as WebMCPManifest;
  }

  const handleController = new AbortController();
  if (resolvedOptions.signal) {
    if (resolvedOptions.signal.aborted) {
      handleController.abort(resolvedOptions.signal.reason);
    } else {
      resolvedOptions.signal.addEventListener(
        "abort",
        () => {
          handleController.abort(resolvedOptions.signal?.reason);
        },
        { once: true },
      );
    }
  }

  const defaultEndpoint = manifest.mcpEndpoint || "/mcp";
  const candidateTools: Array<{ tool: BrowserModelContextTool; controller: AbortController }> = manifest.tools.map(
    (descriptor) => {
      const endpoint = descriptor.endpoint || defaultEndpoint;
      const toolController = new AbortController();
      if (handleController.signal.aborted) {
        toolController.abort(handleController.signal.reason);
      } else {
        handleController.signal.addEventListener(
          "abort",
          () => {
            toolController.abort(handleController.signal.reason);
          },
          { once: true },
        );
      }

      const tool: BrowserModelContextTool = {
        name: descriptor.name,
        ...(descriptor.title !== undefined ? { title: descriptor.title } : {}),
        description: descriptor.description,
        inputSchema: descriptor.inputSchema,
        ...(descriptor.outputSchema !== undefined ? { outputSchema: descriptor.outputSchema } : {}),
        ...(descriptor.annotations ? { annotations: descriptor.annotations } : {}),
        execute: async (
          args: Record<string, unknown>,
          execOptions?: BrowserModelContextExecuteOptions,
        ): Promise<unknown> => {
          const normalizedArgs = args ?? {};
          if (descriptor.requiresConfirmation === true) {
            const approved = await resolveConfirmation(
              descriptor,
              normalizedArgs,
              resolvedOptions.onConfirm,
              execOptions,
            );
            if (!approved) {
              throw new Error(`WebMCP tool call '${descriptor.name}' cancelled: user confirmation declined`);
            }
          }
          const rpcId = `webmcp-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
          const activeSignal = execOptions?.signal ?? toolController.signal;
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
                arguments: normalizedArgs,
                _meta: {
                  "io.modelcontextprotocol/protocolVersion": MCP_PROTOCOL_VERSION,
                  "io.modelcontextprotocol/clientCapabilities": {},
                },
              },
            }),
            signal: activeSignal,
          });
          if (!resp.ok) {
            throw new Error(`WebMCP tool call '${descriptor.name}' failed with HTTP ${resp.status}`);
          }
          return await parseMcpJsonRpcResponse(resp);
        },
      };
      return { tool, controller: toolController };
    },
  );

  const ctx = getBrowserModelContext(resolvedOptions.modelContext);
  const registeredTools: BrowserModelContextTool[] = [];
  const ownedControllers = new Map<string, AbortController>();
  const errors: WebMCPRegistrationError[] = [];

  if (ctx && typeof ctx.registerTool === "function") {
    let ctxMap = activeContextRegistrations.get(ctx);
    if (!ctxMap) {
      ctxMap = new Map<string, AbortController>();
      activeContextRegistrations.set(ctx, ctxMap);
    }

    for (const { tool, controller } of candidateTools) {
      const previous = ctxMap.get(tool.name);
      if (previous) {
        previous.abort();
        ctxMap.delete(tool.name);
        if (typeof ctx.unregisterTool === "function") {
          try {
            await ctx.unregisterTool(tool.name);
          } catch {
            // Ignore cleanup failure during HMR replacement
          }
        }
      }

      if (controller.signal.aborted) {
        continue;
      }

      try {
        await ctx.registerTool(tool, { signal: controller.signal });
        ctxMap.set(tool.name, controller);
        ownedControllers.set(tool.name, controller);
        registeredTools.push(tool);
      } catch (err) {
        controller.abort();
        errors.push({ name: tool.name, error: err });
      }
    }
  } else {
    for (const { tool } of candidateTools) {
      registeredTools.push(tool);
    }
  }

  let disposed = false;
  const dispose = (): void => {
    if (disposed) {
      return;
    }
    disposed = true;
    handleController.abort();
    if (ctx) {
      const ctxMap = activeContextRegistrations.get(ctx);
      for (const tool of registeredTools) {
        const owned = ownedControllers.get(tool.name);
        if (owned && ctxMap?.get(tool.name) === owned) {
          ctxMap.delete(tool.name);
          if (typeof ctx.unregisterTool === "function") {
            try {
              void ctx.unregisterTool(tool.name);
            } catch {
              // Ignore unregister errors on disposal
            }
          }
        }
      }
    }
  };

  if (resolvedOptions.signal) {
    if (resolvedOptions.signal.aborted) {
      dispose();
    } else {
      resolvedOptions.signal.addEventListener("abort", dispose, { once: true });
    }
  }

  return {
    tools: registeredTools,
    errors,
    dispose,
  };
}
