import { describe, expect, it } from "bun:test";
import { AsgiBridge, type AsgiIpcFrame, type AsgiRequest } from "../../src/ipc/asgi";
import type { IpcRequester } from "../../src/ipc/client";
import { ShmRouter } from "../../src/broker/shm";

class MockRequester implements IpcRequester {
  requests: AsgiRequest[] = [];
  streams: unknown[] = [];

  async request<T>(payload: unknown): Promise<T> {
    this.requests.push(payload as AsgiRequest);
    return {
      id: crypto.randomUUID(),
      status: 200,
      headers: [],
      body: new Uint8Array(),
    } as T;
  }

  async *requestStream<T>(payload: unknown): AsyncGenerator<T> {
    this.streams.push(payload);
    yield {
      id: crypto.randomUUID(),
      type: "http.response.start",
      status: 200,
      headers: [],
    } as AsgiIpcFrame as T;
    yield {
      id: crypto.randomUUID(),
      type: "http.response.body",
      body: new Uint8Array(),
      more_body: false,
    } as AsgiIpcFrame as T;
  }
}

class UnaryRequester implements IpcRequester {
  requests: AsgiRequest[] = [];

  async request<T>(payload: unknown): Promise<T> {
    this.requests.push(payload as AsgiRequest);
    return {
      id: crypto.randomUUID(),
      status: 200,
      headers: [],
      body: new Uint8Array(),
    } as T;
  }
}

describe("AsgiBridge guards", () => {
  it("uses unary for direct routes even when streaming is available", async () => {
    const router = new ShmRouter();
    router.insert("/GET/hello", 123);
    const requester = new MockRequester();
    const bridge = new AsgiBridge({
      requester,
      router,
      ipc: { slotSize: 4096, slots: 64, name: "vertebra-asgi" },
    });

    const response = await bridge.handle(new Request("http://localhost/hello"));
    expect(response.status).toBe(200);
    expect(requester.requests.length).toBe(1);
    expect(requester.streams.length).toBe(0);
    expect(requester.requests[0].type).toBe("direct.http");
  });

  it("falls back to ASGI stream for multipart on direct routes", async () => {
    const router = new ShmRouter();
    router.insert("/POST/upload", 5);
    const requester = new MockRequester();
    const bridge = new AsgiBridge({
      requester,
      router,
      ipc: { slotSize: 4096, slots: 64, name: "vertebra-asgi" },
    });

    const response = await bridge.handle(
      new Request("http://localhost/upload", {
        method: "POST",
        headers: { "content-type": "multipart/form-data" },
        body: new Uint8Array([1, 2, 3]),
      }),
    );

    expect(response.status).toBe(200);
    expect(requester.requests.length).toBe(0);
    expect(requester.streams.length).toBe(1);
  });

  it("rejects oversized unary payloads when streaming is unavailable", async () => {
    const router = new ShmRouter();
    router.insert("/POST/large", 7);
    const requester = new UnaryRequester();
    const bridge = new AsgiBridge({
      requester,
      router,
      ipc: { slotSize: 128, slots: 64, name: "vertebra-asgi" },
    });

    const payload = new Uint8Array(1024);
    const response = await bridge.handle(
      new Request("http://localhost/large", {
        method: "POST",
        headers: { "content-length": String(payload.byteLength) },
        body: payload,
      }),
    );

    expect(response.status).toBe(413);
    expect(requester.requests.length).toBe(0);
  });
});
