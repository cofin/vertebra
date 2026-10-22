import { LiveStateClient, type StateDeltaMessage } from "../live/index";
import { TraceContext } from "../telemetry";

export const MCP_PROTOCOL_VERSION = "2026-07-28";

/**
 * Encode an MCP header value, applying MIME RFC 2047 base64 encoding if non-ASCII.
 */
export function encodeMcpHeaderValue(value: string): string {
  if (/^[\x20-\x7E]*$/.test(value)) {
    return value;
  }
  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  const b64 = typeof btoa === "function" ? btoa(binary) : Buffer.from(binary, "binary").toString("base64");
  return `=?base64?${b64}?=`;
}

export interface McpClientOptions {
  baseUrl?: string;
  transport?: (req: Request) => Promise<Response>;
  sessionId?: string;
  traceContext?: TraceContext;
}

export class McpClient {
  readonly baseUrl: string;
  private readonly transport: (req: Request) => Promise<Response>;
  private sessionId?: string;
  private traceContext?: TraceContext;

  constructor(options: McpClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "").replace(/\/+$/, "");
    this.transport = options.transport ?? ((req: Request) => fetch(req));
    this.sessionId = options.sessionId;
    this.traceContext = options.traceContext;
  }

  getSessionId(): string | undefined {
    return this.sessionId;
  }

  setSessionId(id: string | undefined): void {
    this.sessionId = id;
  }

  private resolveMcpName(method: string, params: Record<string, unknown>): string | undefined {
    if (method === "tools/call" && typeof params.name === "string") {
      return params.name;
    }
    if (method === "resources/read" && typeof params.uri === "string") {
      return params.uri;
    }
    if (method === "prompts/get" && typeof params.name === "string") {
      return params.name;
    }
    if ((method.startsWith("tasks/") || method === "tasks/get") && typeof params.taskId === "string") {
      return params.taskId;
    }
    if (method === "skills/get") {
      if (typeof params.name === "string") {
        return params.name;
      }
      if (typeof params.uri === "string") {
        return params.uri;
      }
    }
    return undefined;
  }

  async sendJsonRpc<T = unknown>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    const url = `${this.baseUrl}/mcp`;
    const headers: Record<string, string> = {
      "content-type": "application/json",
      "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
      "Mcp-Method": method,
    };

    const targetName = this.resolveMcpName(method, params);
    if (targetName) {
      headers["Mcp-Name"] = encodeMcpHeaderValue(targetName);
    }

    if (this.sessionId) {
      headers["Mcp-Session-Id"] = this.sessionId;
    }

    const headersWithTrace = TraceContext.injectHeader(headers, this.traceContext);

    const payload = {
      jsonrpc: "2.0",
      id: Math.floor(Math.random() * 1_000_000),
      method,
      params,
    };

    const req = new Request(url, {
      method: "POST",
      headers: headersWithTrace,
      body: JSON.stringify(payload),
    });

    const resp = await this.transport(req);

    const incomingSessionId = resp.headers.get("Mcp-Session-Id");
    if (incomingSessionId) {
      this.sessionId = incomingSessionId;
    }

    if (!resp.ok) {
      throw new Error(`MCP error ${resp.status}: ${await resp.text()}`);
    }

    const data = (await resp.json()) as { result?: T; error?: { message: string } };
    if (data.error) {
      throw new Error(data.error.message);
    }
    return data.result as T;
  }

  async initialize(capabilities: Record<string, unknown> = {}): Promise<unknown> {
    return this.sendJsonRpc("initialize", {
      protocolVersion: MCP_PROTOCOL_VERSION,
      capabilities,
      clientInfo: { name: "@vertebra/client", version: "0.1.0" },
    });
  }

  async listTools(cursor?: string): Promise<{ tools: unknown[]; nextCursor?: string }> {
    return this.sendJsonRpc<{ tools: unknown[]; nextCursor?: string }>("tools/list", { cursor });
  }

  async callTool<T = unknown>(name: string, args: Record<string, unknown> = {}): Promise<T> {
    return this.sendJsonRpc<T>("tools/call", { name, arguments: args });
  }

  async listResources(cursor?: string): Promise<{ resources: unknown[]; nextCursor?: string }> {
    return this.sendJsonRpc<{ resources: unknown[]; nextCursor?: string }>("resources/list", { cursor });
  }

  async readResource(uri: string): Promise<unknown> {
    return this.sendJsonRpc("resources/read", { uri });
  }

  async listPrompts(cursor?: string): Promise<{ prompts: unknown[]; nextCursor?: string }> {
    return this.sendJsonRpc<{ prompts: unknown[]; nextCursor?: string }>("prompts/list", { cursor });
  }

  async getPrompt(name: string, args: Record<string, unknown> = {}): Promise<unknown> {
    return this.sendJsonRpc("prompts/get", { name, arguments: args });
  }

  async listSkills(cursor?: string): Promise<{ skills: unknown[]; nextCursor?: string }> {
    return this.sendJsonRpc<{ skills: unknown[]; nextCursor?: string }>("skills/list", { cursor });
  }

  async getSkill(nameOrUri: string): Promise<unknown> {
    return this.sendJsonRpc("skills/get", { name: nameOrUri, uri: nameOrUri });
  }

  async getTask(taskId: string): Promise<unknown> {
    return this.sendJsonRpc("tasks/get", { taskId });
  }

  async cancelTask(taskId: string): Promise<unknown> {
    return this.sendJsonRpc("tasks/cancel", { taskId });
  }
}

export interface A2aClientOptions {
  baseUrl?: string;
  transport?: (req: Request) => Promise<Response>;
  traceContext?: TraceContext;
}

export class A2aClient {
  readonly baseUrl: string;
  private readonly transport: (req: Request) => Promise<Response>;
  private traceContext?: TraceContext;
  private cachedCard?: unknown;
  private cachedEtag?: string;

  constructor(options: A2aClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "").replace(/\/+$/, "");
    this.transport = options.transport ?? ((req: Request) => fetch(req));
    this.traceContext = options.traceContext;
  }

  async getAgentCard(): Promise<unknown> {
    const url = `${this.baseUrl}/.well-known/agent-card.json`;
    const headers: Record<string, string> = {
      accept: "application/json",
    };
    if (this.cachedEtag) {
      headers["if-none-match"] = this.cachedEtag;
    }

    const req = new Request(url, { method: "GET", headers });
    const resp = await this.transport(req);

    if (resp.status === 304 && this.cachedCard) {
      return this.cachedCard;
    }

    if (!resp.ok) {
      throw new Error(`Failed to fetch AgentCard: ${resp.status}`);
    }

    const etag = resp.headers.get("etag");
    if (etag) {
      this.cachedEtag = etag;
    }

    const card = await resp.json();
    this.cachedCard = card;
    return card;
  }

  async sendMessage(
    message: { role: string; content?: string; parts?: unknown[] },
    configuration: Record<string, unknown> = {},
  ): Promise<unknown> {
    const url = `${this.baseUrl}/a2a`;
    const headers = TraceContext.injectHeader({ "content-type": "application/json" }, this.traceContext);

    const payload = {
      jsonrpc: "2.0",
      id: Math.floor(Math.random() * 1_000_000),
      method: "message/send",
      params: { message, configuration },
    };

    const req = new Request(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });

    const resp = await this.transport(req);
    if (!resp.ok) {
      throw new Error(`A2A error ${resp.status}: ${await resp.text()}`);
    }
    const data = (await resp.json()) as { result?: unknown; error?: { message: string } };
    if (data.error) {
      throw new Error(data.error.message);
    }
    return data.result;
  }

  async streamMessage(
    message: { role: string; content?: string; parts?: unknown[] },
    onEvent: (event: unknown) => void,
  ): Promise<void> {
    const url = `${this.baseUrl}/a2a`;
    const headers = TraceContext.injectHeader(
      {
        "content-type": "application/json",
        accept: "text/event-stream",
      },
      this.traceContext,
    );

    const payload = {
      jsonrpc: "2.0",
      id: Math.floor(Math.random() * 1_000_000),
      method: "message/stream",
      params: { message },
    };

    const req = new Request(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });

    const resp = await this.transport(req);
    if (!resp.ok) {
      throw new Error(`A2A stream error ${resp.status}`);
    }

    const reader = resp.body?.getReader();
    if (!reader) return;

    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        if (line.startsWith("data:")) {
          const raw = line.slice(5).trim();
          if (raw) {
            try {
              onEvent(JSON.parse(raw));
            } catch {
              onEvent(raw);
            }
          }
        }
      }
    }
  }

  async getTask(taskId: string): Promise<unknown> {
    const url = `${this.baseUrl}/a2a`;
    const payload = {
      jsonrpc: "2.0",
      id: 1,
      method: "tasks/get",
      params: { taskId, id: taskId },
    };
    const req = new Request(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    const resp = await this.transport(req);
    const data = (await resp.json()) as { result?: unknown };
    return data.result;
  }

  async cancelTask(taskId: string): Promise<unknown> {
    const url = `${this.baseUrl}/a2a`;
    const payload = {
      jsonrpc: "2.0",
      id: 1,
      method: "tasks/cancel",
      params: { taskId, id: taskId },
    };
    const req = new Request(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    const resp = await this.transport(req);
    const data = (await resp.json()) as { result?: unknown };
    return data.result;
  }

  async resubscribeTask(
    taskId: string,
    lastEventId: string | undefined,
    onEvent: (event: unknown) => void,
  ): Promise<void> {
    const url = `${this.baseUrl}/a2a`;
    const headers: Record<string, string> = {
      "content-type": "application/json",
      accept: "text/event-stream",
    };
    if (lastEventId) {
      headers["last-event-id"] = lastEventId;
    }

    const payload = {
      jsonrpc: "2.0",
      id: 1,
      method: "tasks/resubscribe",
      params: { taskId, id: taskId },
    };

    const req = new Request(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });

    const resp = await this.transport(req);
    const reader = resp.body?.getReader();
    if (!reader) return;

    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        if (line.startsWith("data:")) {
          const raw = line.slice(5).trim();
          if (raw) {
            try {
              onEvent(JSON.parse(raw));
            } catch {
              onEvent(raw);
            }
          }
        }
      }
    }
  }
}

export interface AgentStreamClientOptions {
  baseUrl?: string;
  transport?: (req: Request) => Promise<Response>;
}

export class AgentStreamClient {
  readonly baseUrl: string;
  private readonly transport: (req: Request) => Promise<Response>;

  constructor(options: AgentStreamClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "").replace(/\/+$/, "");
    this.transport = options.transport ?? ((req: Request) => fetch(req));
  }

  async streamChat(
    request: {
      message: string;
      sessionId?: string;
      agentName?: string;
      confirmationToken?: string;
    },
    onFrame: (frame: unknown) => void,
  ): Promise<void> {
    const url = `${this.baseUrl}/agent/chat`;
    const req = new Request(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        accept: "text/event-stream",
      },
      body: JSON.stringify(request),
    });

    const resp = await this.transport(req);
    if (!resp.ok) {
      throw new Error(`Chat stream failed: ${resp.status}`);
    }

    const reader = resp.body?.getReader();
    if (!reader) return;

    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        if (line.startsWith("data:")) {
          const raw = line.slice(5).trim();
          if (raw) {
            try {
              onFrame(JSON.parse(raw));
            } catch {
              onFrame(raw);
            }
          }
        }
      }
    }
  }

  async runTurn(request: {
    message: string;
    sessionId?: string;
    agentName?: string;
    confirmationToken?: string;
  }): Promise<unknown> {
    const url = `${this.baseUrl}/agent/turns`;
    const req = new Request(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(request),
    });
    const resp = await this.transport(req);
    return resp.json();
  }

  async approveConfirmation(token: string): Promise<unknown> {
    const url = `${this.baseUrl}/agent/confirmations/${token}`;
    const req = new Request(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ decision: "approve" }),
    });
    const resp = await this.transport(req);
    return resp.json();
  }

  async rejectConfirmation(token: string, reason?: string): Promise<unknown> {
    const url = `${this.baseUrl}/agent/confirmations/${token}`;
    const req = new Request(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ decision: "reject", reason }),
    });
    const resp = await this.transport(req);
    return resp.json();
  }

  async getSessionHistory(sessionId: string): Promise<unknown> {
    const url = `${this.baseUrl}/agent/sessions/${sessionId}`;
    const req = new Request(url, { method: "GET" });
    const resp = await this.transport(req);
    return resp.json();
  }
}

export interface WorkflowLiveStatePayload {
  [key: string]: unknown;
  runId: string;
  workflowName: string;
  status: string;
  activeNodes: string[];
  completedNodes: string[];
  nodeOutputs: Record<string, unknown>;
}

export class WorkflowLiveSubscriber {
  readonly roomName: string;
  private readonly liveClient: LiveStateClient<WorkflowLiveStatePayload>;
  private readonly listeners = new Set<(state: WorkflowLiveStatePayload) => void>();

  constructor(roomName: string, client?: LiveStateClient<WorkflowLiveStatePayload>) {
    this.roomName = roomName;
    const initial: WorkflowLiveStatePayload = {
      runId: "",
      workflowName: "",
      status: "pending",
      activeNodes: [],
      completedNodes: [],
      nodeOutputs: {},
    };
    this.liveClient = client ?? new LiveStateClient<WorkflowLiveStatePayload>("WorkflowLiveState", initial);

    this.liveClient.subscribe((state) => {
      for (const listener of this.listeners) {
        listener(state);
      }
    });
  }

  subscribe(listener: (state: WorkflowLiveStatePayload) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  applyDelta(delta: StateDeltaMessage): boolean {
    return this.liveClient.applyServerDelta(delta);
  }

  getState(): Readonly<WorkflowLiveStatePayload> {
    return this.liveClient.getSnapshot();
  }
}
