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
import { interpolateRoutePath, routeUrl, wsUrl } from "../src/router";
import { type BrowserModelContextTool, registerWebMCPTools } from "../src/webmcp";

describe("@vertebra/client/agent SDK", () => {
  it("encodes MCP headers correctly for ASCII and non-ASCII characters", () => {
    expect(encodeMcpHeaderValue("read_file")).toBe("read_file");
    expect(encodeMcpHeaderValue("tools/execute_query")).toBe("tools/execute_query");

    const nonAscii = "résumé_task";
    const encoded = encodeMcpHeaderValue(nonAscii);
    expect(encoded.startsWith("=?base64?")).toBe(true);
    expect(encoded.endsWith("?=")).toBe(true);
  });

  it("McpClient sets protocol version, method, name, and traceparent headers", async () => {
    let capturedRequest: Request | null = null;

    const mockTransport = async (req: Request): Promise<Response> => {
      capturedRequest = req;
      return new Response(
        JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          result: { content: [{ type: "text", text: "Tool output" }] },
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
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

    expect(result.content[0]?.text).toBe("Tool output");
    expect(capturedRequest).not.toBeNull();
    const req = capturedRequest as unknown as Request;

    expect(req.headers.get("MCP-Protocol-Version")).toBe(MCP_PROTOCOL_VERSION);
    expect(req.headers.get("Mcp-Method")).toBe("tools/call");
    expect(req.headers.get("Mcp-Name")).toBe("lookup_order");
    expect(req.headers.get("Mcp-Session-Id")).toBe("initial_sess");
    expect(req.headers.get("traceparent")).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/i);

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
      "get.users.by_id": "/users/{id:int}",
    } as const;
    const wsRoutes = {
      chat_room: "/ws/rooms/{room_id:str}",
    } as const;

    expect(interpolateRoutePath("/items/{id:int}", { id: 42 }, { page: 2 })).toBe("/items/42?page=2");
    expect(routeUrl(httpRoutes, "get.users.by_id", { id: 7 }, { baseUrl: "https://api.example.com/" })).toBe(
      "https://api.example.com/users/7",
    );
    expect(
      wsUrl(
        wsRoutes,
        "chat_room",
        { room_id: "general" },
        { baseUrl: "https://api.example.com", query: { token: "abc" } },
      ),
    ).toBe("wss://api.example.com/ws/rooms/general?token=abc");
  });

  it("registerWebMCPTools registers tools with navigator.modelContext and dispatches JSON-RPC tools/call", async () => {
    const registered: BrowserModelContextTool[] = [];
    let capturedHeaders: Headers | null = null;
    let capturedBody: Record<string, unknown> | null = null;

    const fakeFetch: typeof fetch = async (_input, init) => {
      capturedHeaders = new Headers(init?.headers);
      capturedBody = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
      return new Response(
        JSON.stringify({
          jsonrpc: "2.0",
          id: capturedBody.id,
          result: { status: "created", id: 101 },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    };

    const tools = await registerWebMCPTools(
      {
        protocolVersion: MCP_PROTOCOL_VERSION,
        mcpEndpoint: "https://app.example.com/mcp",
        tools: [
          {
            name: "create_ticket",
            description: "Create a support ticket",
            inputSchema: { type: "object", properties: { title: { type: "string" } } },
          },
        ],
      },
      {
        fetchImpl: fakeFetch,
        modelContext: {
          registerTool: (tool) => {
            registered.push(tool);
          },
        },
      },
    );

    expect(tools).toHaveLength(1);
    expect(registered).toHaveLength(1);
    const out = await registered[0]?.execute({ title: "Broken login" });
    expect(out).toEqual({ status: "created", id: 101 });
    expect(capturedHeaders?.get("MCP-Protocol-Version")).toBe(MCP_PROTOCOL_VERSION);
    expect(capturedHeaders?.get("Mcp-Method")).toBe("tools/call");
    expect(capturedHeaders?.get("Mcp-Name")).toBe("create_ticket");
    expect((capturedBody?.params as { name?: string } | undefined)?.name).toBe("create_ticket");
  });
});
