import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  HotReloadClient,
  SseClient,
  VertebraEventSource,
  VertebraSseClient,
  VertebraWebSocket,
  VertebraWebSocketClient,
  WebSocketClient,
} from "../src/index";

class MockEventSource {
  static instances: MockEventSource[] = [];
  url: string;
  withCredentials: boolean;
  readyState = 0;
  onopen: ((ev: Event) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  private listeners = new Map<string, Set<(ev: MessageEvent) => void>>();

  constructor(url: string, init?: EventSourceInit) {
    this.url = url;
    this.withCredentials = init?.withCredentials ?? false;
    MockEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (ev: MessageEvent) => void): void {
    let set = this.listeners.get(type);
    if (!set) {
      set = new Set();
      this.listeners.set(type, set);
    }
    set.add(listener);
  }

  removeEventListener(type: string, listener: (ev: MessageEvent) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  emitOpen(): void {
    this.readyState = 1;
    this.onopen?.(new Event("open"));
  }

  emitEvent(type: string, data: string, lastEventId = ""): void {
    const ev = new MessageEvent(type, { data, lastEventId });
    if (type === "message") {
      this.onmessage?.(ev);
    }
    const set = this.listeners.get(type);
    if (set) {
      for (const cb of set) {
        cb(ev);
      }
    }
  }

  emitError(): void {
    this.readyState = 2;
    this.onerror?.(new Event("error"));
  }

  close(): void {
    this.readyState = 2;
  }
}

class MockWebSocket {
  static instances: MockWebSocket[] = [];
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  url: string;
  protocols?: string | string[];
  protocol = "";
  readyState = MockWebSocket.CONNECTING;
  sent: string[] = [];
  onopen: ((ev: Event) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onclose: ((ev: CloseEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;

  constructor(url: string, protocols?: string | string[]) {
    this.url = url;
    this.protocols = protocols;
    MockWebSocket.instances.push(this);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(code = 1000, reason = ""): void {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.({ code, reason } as CloseEvent);
  }

  simulateOpen(negotiatedProtocol = ""): void {
    this.readyState = MockWebSocket.OPEN;
    this.protocol = negotiatedProtocol;
    this.onopen?.(new Event("open"));
  }

  simulateMessage(payload: unknown): void {
    const data = typeof payload === "string" ? payload : JSON.stringify(payload);
    this.onmessage?.(new MessageEvent("message", { data }));
  }
}

describe("VertebraSseClient & VertebraWebSocketClient", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    MockEventSource.instances = [];
    MockWebSocket.instances = [];
    vi.stubGlobal("EventSource", MockEventSource);
    vi.stubGlobal("WebSocket", MockWebSocket);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("exports clean aliases SseClient and WebSocketClient alongside Vertebra* names", () => {
    expect(SseClient).toBe(VertebraSseClient);
    expect(VertebraEventSource).toBe(VertebraSseClient);
    expect(WebSocketClient).toBe(VertebraWebSocketClient);
    expect(VertebraWebSocket).toBe(VertebraWebSocketClient);
  });

  it("tracks Last-Event-ID, dispatches typed handlers, and reconnects with lastEventId query param", () => {
    const client = new VertebraSseClient("https://api.example.com/events", {
      baseDelayMs: 100,
      maxDelayMs: 1000,
    });

    const received: Array<{ id: number; text: string }> = [];
    client.on<{ id: number; text: string }>("chat.message", (data, meta) => {
      received.push(data);
      expect(meta.lastEventId).toBe("evt-42");
    });

    client.connect();
    expect(MockEventSource.instances).toHaveLength(1);
    const firstEs = MockEventSource.instances[0];
    if (!firstEs) throw new Error("expected first EventSource instance");
    firstEs.emitOpen();
    firstEs.emitEvent("chat.message", JSON.stringify({ id: 1, text: "hello" }), "evt-42");

    expect(received).toEqual([{ id: 1, text: "hello" }]);
    expect(client.getLastEventId()).toBe("evt-42");

    firstEs.emitError();
    vi.advanceTimersByTime(250);

    expect(MockEventSource.instances).toHaveLength(2);
    const secondEs = MockEventSource.instances[1];
    if (!secondEs) throw new Error("expected second EventSource instance");
    expect(secondEs.url).toBe("https://api.example.com/events?lastEventId=evt-42");
    client.close();
  });

  it("negotiates subprotocols, tracks lastEventId, deduplicates 1024 idempotency keys, and sends heartbeats", () => {
    const wsClient = new VertebraWebSocketClient("wss://api.example.com/ws", {
      protocols: ["vtb.pubsub.v1", "json.v1"],
      heartbeatIntervalMs: 1000,
      heartbeatTimeoutMs: 2500,
      baseDelayMs: 100,
    });

    const orders: Array<{ orderId: number }> = [];
    wsClient.subscribe<{ orderId: number }>("orders", (payload) => {
      orders.push(payload);
    });

    expect(MockWebSocket.instances).toHaveLength(1);
    const ws1 = MockWebSocket.instances[0];
    if (!ws1) throw new Error("expected first WebSocket instance");
    expect(ws1.protocols).toEqual(["vtb.pubsub.v1", "json.v1"]);
    ws1.simulateOpen("vtb.pubsub.v1");
    expect(wsClient.getNegotiatedSubprotocol()).toBe("vtb.pubsub.v1");

    const firstSent = ws1.sent[0];
    if (!firstSent) throw new Error("expected initial subscribe frame");
    expect(JSON.parse(firstSent)).toEqual({
      type: "subscribe",
      topic: "orders",
      channels: ["orders"],
      lastEventId: null,
    });

    ws1.simulateMessage({
      type: "event",
      topic: "orders",
      eventId: "10",
      idempotencyKey: "idem-1",
      data: { orderId: 500 },
    });
    ws1.simulateMessage({
      type: "event",
      topic: "orders",
      eventId: "11",
      idempotencyKey: "idem-1",
      data: { orderId: 500 },
    });

    expect(orders).toEqual([{ orderId: 500 }]);

    vi.advanceTimersByTime(1000);
    expect(ws1.sent.some((frame) => JSON.parse(frame).type === "ping")).toBe(true);

    ws1.close(1006, "abnormal");
    vi.advanceTimersByTime(200);

    expect(MockWebSocket.instances).toHaveLength(2);
    const ws2 = MockWebSocket.instances[1];
    if (!ws2) throw new Error("expected second WebSocket instance");
    ws2.simulateOpen("vtb.pubsub.v1");
    const secondSent = ws2.sent[0];
    if (!secondSent) throw new Error("expected reconnect subscribe frame");
    expect(JSON.parse(secondSent)).toEqual({
      type: "subscribe",
      topic: "orders",
      channels: ["orders"],
      lastEventId: "10",
    });

    wsClient.close();
  });

  it("HotReloadClient handles both msg.type and msg.event for css-update and full-reload", () => {
    const reloadSpy = vi.fn();
    vi.stubGlobal("window", {
      location: {
        protocol: "http:",
        host: "localhost:8000",
        reload: reloadSpy,
      },
      dispatchEvent: vi.fn(),
    });

    const hmr = new HotReloadClient("/__vertebra/hmr");
    hmr.connect();
    const ws = MockWebSocket.instances[0];
    if (!ws) throw new Error("expected HMR WebSocket instance");
    ws.simulateOpen();

    ws.simulateMessage({
      event: "full-reload",
      path: "/src/app.py",
      generation: 2,
    });
    expect(reloadSpy).toHaveBeenCalledTimes(1);
    hmr.destroy();
  });
});
