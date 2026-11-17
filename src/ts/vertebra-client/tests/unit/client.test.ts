import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { HttpClient } from "../../src/client/client";
import { TimeoutError } from "../../src/client/errors";
import { computeBackoffDelay } from "../../src/client/retry";
import { HotReloadClient } from "../../src/hmr/index";
import { initHtmx } from "../../src/htmx/index";
import * as rootExports from "../../src/index";
import { QueryCache } from "../../src/react/cache";
import { WebSocketClient } from "../../src/websocket";

describe("HttpClient", () => {
  let client: HttpClient;

  beforeEach(() => {
    client = new HttpClient({ baseURL: "http://api.test" });
    vi.stubGlobal("fetch", vi.fn());
  });

  it("exports canonical domain names from root", () => {
    expect(rootExports.HttpClient).toBe(HttpClient);
    expect(rootExports.WebSocketClient).toBe(WebSocketClient);
    expect(rootExports.HotReloadClient).toBe(HotReloadClient);
    expect(typeof rootExports.routeUrl).toBe("function");
    expect(typeof rootExports.wsUrl).toBe("function");
    expect(typeof rootExports.interpolateRoutePath).toBe("function");
  });

  it("routeUrl and wsUrl support RouteRegistry contracts from root export", () => {
    const httpRoutes = {
      "users.get": { method: "GET", path: "/users/{id}" },
    } as const satisfies rootExports.RouteRegistry;
    const wsRoutes = {
      ws_rooms: { method: "WS", path: "/ws/rooms/{room_id}" },
    } as const satisfies rootExports.RouteRegistry;

    expect(rootExports.routeUrl(httpRoutes, "users.get", { id: 42 }, { query: { active: true, skip: null } })).toBe(
      "/users/42?active=true",
    );
    expect(rootExports.wsUrl(wsRoutes, "ws_rooms", { room_id: "lobby" }, { baseUrl: "http://localhost:8000" })).toBe(
      "ws://localhost:8000/ws/rooms/lobby",
    );
    expect(() => rootExports.routeUrl(httpRoutes, "nope" as keyof typeof httpRoutes)).toThrow("Unknown route: nope");
    expect(() => rootExports.wsUrl(wsRoutes, "nope_ws" as keyof typeof wsRoutes)).toThrow(
      "Unknown WebSocket route: nope_ws",
    );

    const flat = { "users.get": "/users/{id}" } as const;
    const _rejectFlat = () =>
      // @ts-expect-error flat string map is rejected by RouteRegistry
      rootExports.routeUrl(flat, "users.get");
    expect(typeof _rejectFlat).toBe("function");
  });

  it("interpolates path parameters correctly", async () => {
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ id: 123 }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await client.call("GET", "/users/{id}", { id: 123 });

    expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining("/users/123"), expect.any(Object));
  });

  it("interpolates and URL-encodes path parameters for POST while omitting them from body", async () => {
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await client.call("POST", "/users/{id}/profile", { id: "user 42/special", role: "admin" });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining("/users/user%2042%2Fspecial/profile"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ role: "admin" }),
      }),
    );
  });

  it("returns undefined on 204 No Content or empty bodies without throwing SyntaxError", async () => {
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockResolvedValueOnce(
      new Response(null, {
        status: 204,
        headers: { "Content-Length": "0" },
      }),
    );

    const res = await client.callWithResponse("DELETE", "/items/1");
    expect(res.status).toBe(204);
    expect(res.data).toBeUndefined();
  });

  it("preserves string correlation IDs in X-Request-ID", async () => {
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "X-Request-ID": "req-abc-123",
        },
      }),
    );

    const res = await client.callWithResponse("GET", "/ping");
    expect(res.requestId).toBe("req-abc-123");
  });

  it("appends remaining params as query string for GET", async () => {
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify([]), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await client.call("GET", "/search", { q: "vertebra", page: 1 });

    const url = mockFetch.mock.calls[0]?.[0] as string;
    expect(url).toContain("q=vertebra");
    expect(url).toContain("page=1");
  });

  it("throws TimeoutError on abort", async () => {
    // Disable retries so abort throws immediately
    const noRetryClient = new HttpClient({
      baseURL: "http://api.test",
      retry: { maxAttempts: 0 },
    });
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockImplementation(() => {
      const error = new Error("Abort");
      error.name = "AbortError";
      return Promise.reject(error);
    });

    await expect(noRetryClient.call("GET", "/slow")).rejects.toThrow(TimeoutError);
  });

  it("handles non-JSON error responses", async () => {
    // Disable retries — 500 is retryable by default
    const noRetryClient = new HttpClient({
      baseURL: "http://api.test",
      retry: { maxAttempts: 0 },
    });
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockResolvedValueOnce(
      new Response("Server Error", {
        status: 500,
        statusText: "Internal Server Error",
      }),
    );

    await expect(noRetryClient.call("GET", "/error")).rejects.toThrow("Internal Server Error");
  });

  it("retries on retryable status codes", async () => {
    const mockFetch = vi.mocked(fetch);
    // First call: 503, second call: 200
    mockFetch.mockResolvedValueOnce(new Response("", { status: 503 })).mockResolvedValueOnce(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    const result = await client.call("GET", "/flaky");
    expect(result).toEqual({ ok: true });
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it("does not retry non-retryable status codes", async () => {
    const noRetryClient = new HttpClient({
      baseURL: "http://api.test",
      retry: { maxAttempts: 0 },
    });
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify({ message: "Forbidden" }), {
        status: 403,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await expect(noRetryClient.call("GET", "/forbidden")).rejects.toThrow("Forbidden");
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });
});

describe("computeBackoffDelay", () => {
  it("computes exponential delay with jitter capped at maxDelayMs", () => {
    const d0 = computeBackoffDelay(0, 1000, 30_000);
    expect(d0).toBeGreaterThanOrEqual(1000);
    expect(d0).toBeLessThanOrEqual(1500);

    const capped = computeBackoffDelay(10, 1000, 5000);
    expect(capped).toBe(5000);
  });
});

describe("QueryCache", () => {
  it("returns undefined from get() when entry is stale", () => {
    const cache = new QueryCache();
    const key = QueryCache.stableKey("GET", "/items", undefined);
    cache.set(key, ["a"]);

    expect(cache.get(key, 5000)?.data).toEqual(["a"]);

    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 10_000);
    expect(cache.get(key, 5000)).toBeUndefined();
    vi.restoreAllMocks();
  });

  it("clears pending promise on rejection so subsequent getOrCreate retries", async () => {
    const cache = new QueryCache();
    const key = QueryCache.stableKey("GET", "/items", undefined);
    let calls = 0;

    await expect(
      cache.getOrCreate(key, 5000, async () => {
        calls++;
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    const data = await cache.getOrCreate(key, 5000, async () => {
      calls++;
      return { ok: true };
    });

    expect(data).toEqual({ ok: true });
    expect(calls).toBe(2);
  });

  it("canonicalizes object key order in stableKey", () => {
    const k1 = QueryCache.stableKey("GET", "/items", { a: 1, b: 2 });
    const k2 = QueryCache.stableKey("GET", "/items", { b: 2, a: 1 });
    expect(k1).toBe(k2);

    const kNested1 = QueryCache.stableKey("POST", "/items", { z: { b: 1, a: 2 }, y: [1, 2] });
    const kNested2 = QueryCache.stableKey("POST", "/items", { y: [1, 2], z: { a: 2, b: 1 } });
    expect(kNested1).toBe(kNested2);
  });

  it("does not refetch on re-entry at staleTime=0 until invalidate bumps epoch", async () => {
    const cache = new QueryCache();
    const key = QueryCache.stableKey("GET", "/items", undefined);
    let calls = 0;
    const fetcher = async () => {
      calls += 1;
      return { count: calls };
    };

    const first = await cache.getOrCreate(key, 0, fetcher);
    expect(first).toEqual({ count: 1 });
    expect(calls).toBe(1);

    const cached = await cache.getOrCreate(key, 0, fetcher);
    expect(cached).toEqual({ count: 1 });
    expect(calls).toBe(1);

    cache.invalidate();
    const refetched = await cache.getOrCreate(key, 0, fetcher);
    expect(refetched).toEqual({ count: 2 });
    expect(calls).toBe(2);
  });
});

describe("initHtmx", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("injects configured headers and JSON Content-Type for non-GET htmx:configRequest events", () => {
    const doc = new EventTarget();
    vi.stubGlobal("document", doc);

    initHtmx({
      baseURL: "http://api.test",
      headers: { "X-Vertebra-Token": "test-token-placeholder" },
    });

    const postHeaders: Record<string, string> = {};
    doc.dispatchEvent(
      new CustomEvent("htmx:configRequest", {
        detail: { verb: "POST", headers: postHeaders },
      }),
    );
    expect(postHeaders["X-Vertebra-Token"]).toBe("test-token-placeholder");
    expect(postHeaders["Content-Type"]).toBe("application/json");

    const getHeaders: Record<string, string> = {};
    doc.dispatchEvent(
      new CustomEvent("htmx:configRequest", {
        detail: { verb: "GET", headers: getHeaders },
      }),
    );
    expect(getHeaders["X-Vertebra-Token"]).toBe("test-token-placeholder");
    expect(getHeaders["Content-Type"]).toBeUndefined();
  });

  it("logs htmx:responseError details to console.error", () => {
    const doc = new EventTarget();
    vi.stubGlobal("document", doc);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    initHtmx({ baseURL: "http://api.test" });

    const errorDetail = { xhr: { status: 500 }, pathInfo: { requestPath: "/items" } };
    doc.dispatchEvent(new CustomEvent("htmx:responseError", { detail: errorDetail }));

    expect(errorSpy).toHaveBeenCalledTimes(1);
    expect(errorSpy).toHaveBeenCalledWith("[vertebra:htmx] request failed:", errorDetail);
  });
});
