import { beforeEach, describe, expect, it, vi } from "vitest";
import { HttpClient } from "../../src/client/client";
import { TimeoutError } from "../../src/client/errors";
import { computeBackoffDelay } from "../../src/client/retry";
import { RealtimeChannel } from "../../src/client/ws";
import { HotReloadClient } from "../../src/hmr/index";
import { initHtmx } from "../../src/htmx/index";
import * as rootExports from "../../src/index";
import { QueryCache } from "../../src/react/cache";

describe("HttpClient", () => {
  let client: HttpClient;

  beforeEach(() => {
    client = new HttpClient({ baseURL: "http://api.test" });
    vi.stubGlobal("fetch", vi.fn());
  });

  it("exports canonical domain names from root", () => {
    expect(rootExports.HttpClient).toBe(HttpClient);
    expect(rootExports.RealtimeChannel).toBe(RealtimeChannel);
    expect(rootExports.HotReloadClient).toBe(HotReloadClient);
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

  it("appends remaining params as query string for GET", async () => {
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify([]), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await client.call("GET", "/search", { q: "vertebra", page: 1 });

    const url = mockFetch.mock.calls[0][0] as string;
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
});

describe("initHtmx", () => {
  it("returns undefined without allocating an unused client", () => {
    expect(initHtmx({ baseURL: "http://api.test" })).toBeUndefined();
  });
});
