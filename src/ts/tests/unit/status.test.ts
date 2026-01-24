import { describe, expect, it } from "bun:test";
import { createSimpleStatusHandler, createStatusHandler, type ServerStatus } from "../../src/server/status";

describe("Status Handler", () => {
  it("should return JSON response", async () => {
    const handler = createSimpleStatusHandler();
    const req = new Request("http://localhost/internal/status");
    const res = await handler(req);

    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toBe("application/json");
  });

  it("should return bun status as ready", async () => {
    const handler = createSimpleStatusHandler();
    const req = new Request("http://localhost/internal/status");
    const res = await handler(req);

    const status: ServerStatus = await res.json();

    expect(status.bun.status).toBe("ready");
    expect(status.bun.version).toBe(Bun.version);
    expect(status.bun.uptime_ms).toBeGreaterThanOrEqual(0);
  });

  it("should return broker status", async () => {
    const handler = createSimpleStatusHandler();
    const req = new Request("http://localhost/internal/status");
    const res = await handler(req);

    const status: ServerStatus = await res.json();

    expect(status.broker.status).toBe("ready");
    expect(status.broker.clients).toBeGreaterThanOrEqual(0);
    expect(typeof status.broker.pools).toBe("object");
  });

  it("should return ipc status", async () => {
    const handler = createSimpleStatusHandler();
    const req = new Request("http://localhost/internal/status");
    const res = await handler(req);

    const status: ServerStatus = await res.json();

    expect(status.ipc.status).toBe("ready");
    expect(status.ipc.channels).toBeGreaterThanOrEqual(0);
  });

  it("should set no-cache headers", async () => {
    const handler = createSimpleStatusHandler();
    const req = new Request("http://localhost/internal/status");
    const res = await handler(req);

    expect(res.headers.get("Cache-Control")).toBe("no-cache, no-store, must-revalidate");
  });

  it("should show starting status when no broker provided", async () => {
    const handler = createStatusHandler(undefined, undefined);
    const req = new Request("http://localhost/internal/status");
    const res = await handler(req);

    const status: ServerStatus = await res.json();

    expect(status.broker.status).toBe("starting");
    expect(status.ipc.status).toBe("starting");
  });
});
