import { describe, expect, it } from "vitest";
import {
  A2aClient,
  AgentStreamClient,
  encodeMcpHeaderValue,
  MCP_PROTOCOL_VERSION,
  McpClient,
  WorkflowLiveSubscriber,
} from "../src/agent/index";
import { fnv1aFieldId, fnv1aStateId, StateOpcode } from "../src/live/index";
import { interpolateRoutePath, type RouteDefinition, type RouteRegistry, routeUrl, wsUrl } from "../src/router";
import { type BrowserModelContextTool, registerWebMCPTools } from "../src/webmcp";

describe("@vertebra/client/agent SDK", () => {
  it("encodes MCP headers correctly for ASCII, non-ASCII, and control/newline characters", () => {
    expect(encodeMcpHeaderValue("read_file")).toBe("read_file");
    expect(encodeMcpHeaderValue("tools/execute_query")).toBe("tools/execute_query");

    const nonAscii = "résumé_task";
    const encoded = encodeMcpHeaderValue(nonAscii);
    expect(encoded.startsWith("=?base64?")).toBe(true);
    expect(encoded.endsWith("?=")).toBe(true);

    const crlf = "bad\r\nInjected: 1";
    const encodedCrlf = encodeMcpHeaderValue(crlf);
    expect(encodedCrlf.startsWith("=?base64?")).toBe(true);
    expect(encodedCrlf.includes("\r")).toBe(false);
    expect(encodedCrlf.includes("\n")).toBe(false);

    const spoofed = "=?base64?already?=";
    const encodedSpoof = encodeMcpHeaderValue(spoofed);
    expect(encodedSpoof).not.toBe(spoofed);
    expect(encodedSpoof.startsWith("=?base64?")).toBe(true);
  });

  it("McpClient sets protocol version, accept, _meta, and parses SSE JSON-RPC responses", async () => {
    let capturedRequest: Request | null = null;
    let capturedBody: Record<string, unknown> | null = null;

    const mockTransport = async (req: Request): Promise<Response> => {
      capturedRequest = req;
      capturedBody = (await req.clone().json()) as Record<string, unknown>;
      return new Response(
        'event: message\ndata: {"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"SSE tool output"}]}}\n\n',
        {
          status: 200,
          headers: {
            "content-type": "text/event-stream",
            "Mcp-Session-Id": "sess_server_999",
          },
        },
      );
    };

    const client = new McpClient({
      baseUrl: "https://agent.example.com",
      transport: mockTransport,
      sessionId: "initial_sess",
    });

    const result = await client.callTool<{ content: Array<{ text: string }> }>("lookup_order", { orderId: 45001 });

    expect(result.content[0]?.text).toBe("SSE tool output");
    expect(capturedRequest).not.toBeNull();
    const req = capturedRequest as unknown as Request;

    expect(req.headers.get("accept")).toBe("application/json, text/event-stream");
    expect(req.headers.get("MCP-Protocol-Version")).toBe(MCP_PROTOCOL_VERSION);
    expect(req.headers.get("Mcp-Method")).toBe("tools/call");
    expect(req.headers.get("Mcp-Name")).toBe("lookup_order");
    expect(req.headers.get("Mcp-Session-Id")).toBe("initial_sess");
    expect(req.headers.get("traceparent")).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/i);

    const params = (capturedBody?.params ?? {}) as Record<string, unknown>;
    const meta = (params._meta ?? {}) as Record<string, unknown>;
    expect(meta["io.modelcontextprotocol/protocolVersion"]).toBe(MCP_PROTOCOL_VERSION);
    expect(meta["io.modelcontextprotocol/clientCapabilities"]).toEqual({});

    expect(client.getSessionId()).toBe("sess_server_999");
  });

  it("A2aClient caches AgentCard with ETag and handles 304 Not Modified", async () => {
    let callCount = 0;
    const cardData = { name: "OrderAgent", version: "1.0" };

    const mockTransport = async (req: Request): Promise<Response> => {
      callCount += 1;
      const ifNoneMatch = req.headers.get("if-none-match");
      if (ifNoneMatch === '"etag-card-123"') {
        return new Response(null, { status: 304 });
      }
      return new Response(JSON.stringify(cardData), {
        status: 200,
        headers: {
          "content-type": "application/json",
          etag: '"etag-card-123"',
        },
      });
    };

    const client = new A2aClient({
      baseUrl: "https://agent.example.com",
      transport: mockTransport,
    });

    const card1 = (await client.getAgentCard()) as typeof cardData;
    expect(card1.name).toBe("OrderAgent");
    expect(callCount).toBe(1);

    const card2 = (await client.getAgentCard()) as typeof cardData;
    expect(card2.name).toBe("OrderAgent");
    expect(callCount).toBe(2);
  });

  it("A2aClient sends messages and cancels tasks", async () => {
    let sentMethod = "";

    const mockTransport = async (req: Request): Promise<Response> => {
      const body = (await req.json()) as { method: string };
      sentMethod = body.method;
      return new Response(
        JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          result: { status: { state: "accepted" } },
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    };

    const client = new A2aClient({
      baseUrl: "https://agent.example.com",
      transport: mockTransport,
    });

    const sendRes = (await client.sendMessage({ role: "user", content: "Check status" })) as {
      status: { state: string };
    };
    expect(sentMethod).toBe("message/send");
    expect(sendRes.status.state).toBe("accepted");

    await client.cancelTask("task-123");
    expect(sentMethod).toBe("tasks/cancel");
  });

  it("AgentStreamClient approves and rejects confirmation tickets", async () => {
    let capturedBody: unknown = null;
    let capturedUrl = "";

    const mockTransport = async (req: Request): Promise<Response> => {
      capturedUrl = req.url;
      capturedBody = await req.json();
      return new Response(JSON.stringify({ status: "ok" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    };

    const client = new AgentStreamClient({
      baseUrl: "https://agent.example.com",
      transport: mockTransport,
    });

    await client.approveConfirmation("token-abc");
    expect(capturedUrl).toContain("/agent/confirmations/token-abc");
    expect(capturedBody).toEqual({ decision: "approve" });

    await client.rejectConfirmation("token-abc", "Too risky");
    expect(capturedBody).toEqual({ decision: "reject", reason: "Too risky" });
  });

  it("WorkflowLiveSubscriber updates reactive state on FieldSet deltas", () => {
    const subscriber = new WorkflowLiveSubscriber("workflow:run_123");
    const states: string[] = [];

    const unsubscribe = subscriber.subscribe((state) => {
      states.push(state.status);
    });

    const sid = fnv1aStateId("WorkflowLiveState");
    const statusFid = fnv1aFieldId("status");

    subscriber.applyDelta({
      opcode: StateOpcode.FieldSet,
      stateId: sid,
      fieldId: statusFid,
      timestampNs: 1000n,
      value: "running",
    });

    expect(subscriber.getState().status).toBe("running");
    expect(states).toContain("running");

    subscriber.applyDelta({
      opcode: StateOpcode.FieldSet,
      stateId: sid,
      fieldId: statusFid,
      timestampNs: 2000n,
      value: "completed",
    });

    expect(subscriber.getState().status).toBe("completed");
    expect(states).toContain("completed");

    unsubscribe();
  });

  it("routeUrl and wsUrl interpolate typed path and query parameters cleanly", () => {
    const httpRoutes = {
      "get.users.by_id": { method: "GET", path: "/users/{id}" },
    } as const satisfies RouteRegistry;
    const wsRoutes = {
      chat_room: { method: "WS", path: "/ws/rooms/{room_id}" },
    } as const satisfies RouteRegistry;
    const def: RouteDefinition = httpRoutes["get.users.by_id"];
    expect(def.method).toBe("GET");

    expect(interpolateRoutePath("/items/{id:int}", { id: 42 }, { page: 2, empty: null, skipped: undefined })).toBe(
      "/items/42?page=2",
    );
    expect(
      routeUrl(
        httpRoutes,
        "get.users.by_id",
        { id: 7 },
        { baseUrl: "https://api.example.com/", query: { tab: "profile", omit: undefined, blank: null, page: 1 } },
      ),
    ).toBe("https://api.example.com/users/7?tab=profile&page=1");
    expect(
      wsUrl(
        wsRoutes,
        "chat_room",
        { room_id: "general" },
        { baseUrl: "https://api.example.com", query: { token: "abc", skip: null } },
      ),
    ).toBe("wss://api.example.com/ws/rooms/general?token=abc");
    expect(() => routeUrl(httpRoutes, "missing.route" as keyof typeof httpRoutes)).toThrow(
      "Unknown route: missing.route",
    );
    expect(() => wsUrl(wsRoutes, "missing_ws" as keyof typeof wsRoutes)).toThrow("Unknown WebSocket route: missing_ws");

    const flatHttp = { "get.users.by_id": "/users/{id}" } as const;
    const flatWs = { chat_room: "/ws/rooms/{room_id}" } as const;
    const _rejectFlatHttp = () =>
      // @ts-expect-error flat string map is not a RouteRegistry
      routeUrl(flatHttp, "get.users.by_id");
    const _rejectFlatWs = () =>
      // @ts-expect-error flat string map is not a RouteRegistry
      wsUrl(flatWs, "chat_room");
    expect(typeof _rejectFlatHttp).toBe("function");
    expect(typeof _rejectFlatWs).toBe("function");
  });

  it("registerWebMCPTools prioritizes document.modelContext, forwards signal, isolates errors, and supports HMR disposal", async () => {
    const registered = new Map<string, { tool: BrowserModelContextTool; signal?: AbortSignal }>();
    const unregistered: string[] = [];
    let capturedHeaders: Headers | null = null;
    let capturedBody: Record<string, unknown> | null = null;
    let capturedSignal: AbortSignal | null | undefined = null;

    const fakeFetch: typeof fetch = async (_input, init) => {
      capturedHeaders = new Headers(init?.headers);
      capturedBody = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
      capturedSignal = init?.signal;
      return new Response(
        JSON.stringify({
          jsonrpc: "2.0",
          id: capturedBody.id,
          result: { status: "created", id: 101 },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    };

    const docContext = {
      registerTool: (tool: BrowserModelContextTool, opts?: { signal?: AbortSignal }) => {
        if (tool.name === "broken_tool") {
          throw new Error("registration rejected");
        }
        registered.set(tool.name, { tool, signal: opts?.signal });
        opts?.signal?.addEventListener("abort", () => {
          registered.delete(tool.name);
        });
      },
      unregisterTool: (name: string) => {
        unregistered.push(name);
        registered.delete(name);
      },
    };
    const navContext = {
      registerTool: () => {
        throw new Error("navigator.modelContext should not be used when document.modelContext is present");
      },
    };

    const g = globalThis as Record<string, unknown>;
    const navObj = globalThis.navigator as unknown as Record<string, unknown>;
    const prevDoc = g.document;
    const prevNavModelContext = navObj.modelContext;
    g.document = { modelContext: docContext };
    navObj.modelContext = navContext;

    try {
      const manifest = {
        protocolVersion: MCP_PROTOCOL_VERSION,
        mcpEndpoint: "https://app.example.com/mcp",
        tools: [
          {
            name: "broken_tool",
            description: "Fails registration",
            inputSchema: { type: "object" },
          },
          {
            name: "create_ticket",
            title: "Create Ticket",
            description: "Create a support ticket",
            inputSchema: { type: "object", properties: { title: { type: "string" } } },
            outputSchema: { type: "object", properties: { id: { type: "number" } } },
            annotations: { consequentialHint: true },
          },
        ],
      };

      const handle1 = await registerWebMCPTools(manifest, { fetchImpl: fakeFetch });
      expect(handle1.tools).toHaveLength(1);
      expect(handle1.errors).toHaveLength(1);
      expect(handle1.errors[0]?.name).toBe("broken_tool");
      expect(registered.size).toBe(1);

      const firstEntry = registered.get("create_ticket");
      expect(firstEntry?.tool.title).toBe("Create Ticket");
      expect(firstEntry?.tool.outputSchema).toEqual({
        type: "object",
        properties: { id: { type: "number" } },
      });
      expect(firstEntry?.signal?.aborted).toBe(false);

      // HMR re-registration replaces previous tool cleanly without duplicates
      const abortCtrl = new AbortController();
      const handle2 = await registerWebMCPTools(manifest, {
        fetchImpl: fakeFetch,
        signal: abortCtrl.signal,
      });
      expect(firstEntry?.signal?.aborted).toBe(true);
      expect(registered.size).toBe(1);

      const execAbort = new AbortController();
      const activeTool = handle2.tools[0];
      const out = await activeTool?.execute({ title: "Broken login" }, { signal: execAbort.signal });
      expect(out).toEqual({ status: "created", id: 101 });
      expect(capturedHeaders?.get("MCP-Protocol-Version")).toBe(MCP_PROTOCOL_VERSION);
      expect(capturedHeaders?.get("Mcp-Method")).toBe("tools/call");
      expect(capturedHeaders?.get("Mcp-Name")).toBe("create_ticket");
      expect(capturedSignal).toBe(execAbort.signal);
      expect((capturedBody?.params as { name?: string } | undefined)?.name).toBe("create_ticket");

      handle2.dispose();
      expect(registered.size).toBe(0);
      expect(unregistered).toContain("create_ticket");
    } finally {
      g.document = prevDoc;
      if (prevNavModelContext === undefined) {
        delete navObj.modelContext;
      } else {
        navObj.modelContext = prevNavModelContext;
      }
    }
  });

  it("WebMCP execute parses text/event-stream SSE responses with intermediate notifications", async () => {
    const manifest = {
      protocolVersion: MCP_PROTOCOL_VERSION,
      mcpEndpoint: "https://app.example.com/mcp",
      tools: [
        {
          name: "stream_tool",
          description: "Streams SSE result",
          inputSchema: { type: "object" },
        },
      ],
    };

    const sseFetch: typeof fetch = async () =>
      new Response(
        [
          'event: message\ndata: {"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":50}}\n\n',
          'event: message\ndata: {"jsonrpc":"2.0","id":"1","result":{"ok":true}}\n\n',
        ].join(""),
        {
          status: 200,
          headers: { "content-type": "text/event-stream" },
        },
      );

    const handle = await registerWebMCPTools(manifest, {
      fetchImpl: sseFetch,
      modelContext: null,
    });
    const out = await handle.tools[0]?.execute({});
    expect(out).toEqual({ ok: true });
    handle.dispose();
  });

  it("WebMCP enforces requiresConfirmation fail-closed before calling fetchImpl", async () => {
    const manifest = {
      protocolVersion: MCP_PROTOCOL_VERSION,
      mcpEndpoint: "https://app.example.com/mcp",
      tools: [
        {
          name: "delete_record",
          description: "Deletes a record",
          inputSchema: { type: "object" },
          requiresConfirmation: true,
        },
      ],
    };

    let fetchCalls = 0;
    const mockFetch: typeof fetch = async () => {
      fetchCalls += 1;
      return new Response(JSON.stringify({ jsonrpc: "2.0", id: "1", result: { deleted: true } }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    };

    const declinedHandle = await registerWebMCPTools(manifest, {
      fetchImpl: mockFetch,
      modelContext: null,
      onConfirm: () => false,
    });
    await expect(declinedHandle.tools[0]?.execute({ id: 7 })).rejects.toThrow(
      "WebMCP tool call 'delete_record' cancelled: user confirmation declined",
    );
    expect(fetchCalls).toBe(0);

    const rejectedHandle = await registerWebMCPTools(manifest, {
      fetchImpl: mockFetch,
      modelContext: null,
      onConfirm: async () => {
        throw new Error("prompt crashed");
      },
    });
    await expect(rejectedHandle.tools[0]?.execute({ id: 7 })).rejects.toThrow("prompt crashed");
    expect(fetchCalls).toBe(0);

    const g = globalThis as Record<string, unknown>;
    const prevConfirm = g.confirm;
    delete g.confirm;
    try {
      const nonInteractiveHandle = await registerWebMCPTools(manifest, {
        fetchImpl: mockFetch,
        modelContext: null,
      });
      await expect(nonInteractiveHandle.tools[0]?.execute({ id: 7 })).rejects.toThrow(
        "WebMCP tool call 'delete_record' cancelled: user confirmation declined",
      );
      expect(fetchCalls).toBe(0);

      g.confirm = () => true;
      let interactionWrapped = false;
      const wrappedConfirmHandle = await registerWebMCPTools(manifest, {
        fetchImpl: mockFetch,
        modelContext: null,
      });
      const wrappedOut = await wrappedConfirmHandle.tools[0]?.execute(
        { id: 8 },
        {
          requestUserInteraction: async (cb) => {
            interactionWrapped = true;
            return await cb();
          },
        },
      );
      expect(interactionWrapped).toBe(true);
      expect(wrappedOut).toEqual({ deleted: true });
      expect(fetchCalls).toBe(1);
    } finally {
      if (prevConfirm === undefined) {
        delete g.confirm;
      } else {
        g.confirm = prevConfirm;
      }
    }

    const acceptedHandle = await registerWebMCPTools(manifest, {
      fetchImpl: mockFetch,
      modelContext: null,
      onConfirm: async ({ tool, args }) => tool.name === "delete_record" && args.id === 9,
    });
    const out = await acceptedHandle.tools[0]?.execute({ id: 9 });
    expect(out).toEqual({ deleted: true });
    expect(fetchCalls).toBe(2);
  });

  it("WebMCP supports browser registerTool(tool, { signal }) without unregisterTool", async () => {
    const signals: AbortSignal[] = [];
    const registeredArgsCount: number[] = [];
    const docContext = {
      registerTool: (...args: [BrowserModelContextTool, { signal?: AbortSignal }?]) => {
        registeredArgsCount.push(args.length);
        const opts = args[1];
        if (opts?.signal) {
          signals.push(opts.signal);
        }
      },
    };

    const g = globalThis as Record<string, unknown>;
    const prevDoc = g.document;
    g.document = { modelContext: docContext };
    try {
      const manifest = {
        protocolVersion: MCP_PROTOCOL_VERSION,
        mcpEndpoint: "https://app.example.com/mcp",
        tools: [
          {
            name: "ping_tool",
            description: "Ping",
            inputSchema: { type: "object" },
          },
        ],
      };

      const handle1 = await registerWebMCPTools(manifest);
      expect(registeredArgsCount).toEqual([2]);
      expect(signals).toHaveLength(1);
      expect(signals[0]?.aborted).toBe(false);

      const handle2 = await registerWebMCPTools(manifest);
      expect(registeredArgsCount).toEqual([2, 2]);
      expect(signals).toHaveLength(2);
      expect(signals[0]?.aborted).toBe(true);
      expect(signals[1]?.aborted).toBe(false);

      handle1.dispose();
      expect(signals[1]?.aborted).toBe(false);

      handle2.dispose();
      expect(signals[1]?.aborted).toBe(true);
    } finally {
      g.document = prevDoc;
    }
  });
});
