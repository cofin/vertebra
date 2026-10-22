import { describe, expect, it, vi } from "vitest";
import { ARROW_FILE_MAGIC, ARROW_STREAM_MIME, ArrowStreamClient } from "../src/arrow";

describe("ArrowStreamClient", () => {
  it("detects Apache Arrow IPC stream and file headers", () => {
    expect(ArrowStreamClient.hasIpcHeader(ARROW_FILE_MAGIC)).toBe(true);
    const continuation = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x10, 0x00, 0x00, 0x00]);
    expect(ArrowStreamClient.hasIpcHeader(continuation)).toBe(true);
    expect(ArrowStreamClient.hasIpcHeader(new Uint8Array([0x00, 0x01]))).toBe(false);
  });

  it("sets Accept header to application/vnd.apache.arrow.stream", () => {
    const client = new ArrowStreamClient({ endpoint: "/api/analytics" });
    expect(client.url).toBe("/api/analytics");
    expect(client.headers.Accept).toBe(ARROW_STREAM_MIME);
  });

  it("fetchBytes requests arrow stream mime and validates IPC header", async () => {
    const continuation = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x10, 0x00, 0x00, 0x00]);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce(new Response(continuation)));

    const client = new ArrowStreamClient({ endpoint: "/api/stream" });
    const bytes = await client.fetchBytes();
    expect(bytes).toEqual(continuation);
    expect(fetch).toHaveBeenCalledWith(
      "/api/stream",
      expect.objectContaining({
        headers: expect.objectContaining({
          Accept: ARROW_STREAM_MIME,
        }),
      }),
    );
    vi.unstubAllGlobals();
  });

  it("fetchBytes rejects non-Arrow responses", async () => {
    const invalid = new Uint8Array([0x01, 0x02, 0x03, 0x04]);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce(new Response(invalid)));

    const client = new ArrowStreamClient({ endpoint: "/api/stream" });
    await expect(client.fetchBytes()).rejects.toThrow("Invalid Arrow IPC payload");
    vi.unstubAllGlobals();
  });
});
