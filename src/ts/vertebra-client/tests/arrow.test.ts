import { describe, expect, it } from "vitest";
import { ARROW_FILE_MAGIC, ARROW_STREAM_MIME, hasArrowIpcHeader, VertebraArrowStreamClient } from "../src/arrow";

describe("VertebraArrowStreamClient", () => {
  it("detects Apache Arrow IPC stream and file headers", () => {
    expect(hasArrowIpcHeader(ARROW_FILE_MAGIC)).toBe(true);
    const continuation = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x10, 0x00, 0x00, 0x00]);
    expect(hasArrowIpcHeader(continuation)).toBe(true);
    expect(hasArrowIpcHeader(new Uint8Array([0x00, 0x01]))).toBe(false);
  });

  it("sets Accept header to application/vnd.apache.arrow.stream", () => {
    const client = new VertebraArrowStreamClient({ endpoint: "/api/analytics" });
    expect(client.url).toBe("/api/analytics");
    expect(client.headers.Accept).toBe(ARROW_STREAM_MIME);
  });
});
