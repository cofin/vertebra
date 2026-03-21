import { beforeEach, describe, expect, it, vi } from "vitest";
import { VertebraClient } from "../../src/client/client";
import { TimeoutError } from "../../src/client/errors";

describe("VertebraClient", () => {
  let client: VertebraClient;

  beforeEach(() => {
    client = new VertebraClient({ baseURL: "http://api.test" });
    vi.stubGlobal("fetch", vi.fn());
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
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockImplementationOnce(() => {
      const error = new Error("Abort");
      error.name = "AbortError";
      return Promise.reject(error);
    });

    await expect(client.call("GET", "/slow")).rejects.toThrow(TimeoutError);
  });

  it("handles non-JSON error responses", async () => {
    const mockFetch = vi.mocked(fetch);
    mockFetch.mockResolvedValueOnce(
      new Response("Server Error", {
        status: 500,
        statusText: "Internal Server Error",
      }),
    );

    await expect(client.call("GET", "/error")).rejects.toThrow("Internal Server Error");
  });
});
