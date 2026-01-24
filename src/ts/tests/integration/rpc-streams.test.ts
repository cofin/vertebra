import { describe, expect, it } from "bun:test";
import { createClient, type RpcTransport } from "../../src/client";

// Mock transport for testing client logic independent of network
class MockTransport implements RpcTransport {
  async request(procedureId: string, payload: unknown) {
    if (procedureId === "hello") return "world";
    return null;
  }

  subscribe(
    procedureId: string,
    payload: unknown,
    onData: (data: unknown) => void,
    handlers?: { onError?: (error: unknown) => void; onClose?: () => void },
  ) {
    if (procedureId === "counter") {
      let count = 0;
      const interval = setInterval(() => {
        onData(count++);
        if (count > 3) {
          clearInterval(interval);
          handlers?.onClose?.();
        }
      }, 10);
      return () => clearInterval(interval);
    }
    return () => {};
  }
}

describe("Vertebra Client RPC & Streams", () => {
  const client = createClient<{
    hello: (params: {}) => Promise<string>;
    counter: (params: {}) => Promise<any>; // Should be Subscription<number>
  }>(new MockTransport());

  it("should perform standard RPC calls", async () => {
    const result = await client.hello({});
    expect(result).toBe("world");
  });

  it("should handle streaming subscriptions via subscribe", async () => {
    const received: number[] = [];
    await new Promise<void>(resolve => {
      client.counter({}).subscribe((data: any) => {
        received.push(data);
        if (received.length === 3) resolve();
      });
    });
    expect(received).toEqual([0, 1, 2]);
  });

  it("should support async iterator subscriptions", async () => {
    const received: number[] = [];
    const iterator = client.counter({})[Symbol.asyncIterator]();
    for await (const value of iterator) {
      received.push(value);
      if (received.length === 3) {
        await iterator.return?.();
        break;
      }
    }
    expect(received).toEqual([0, 1, 2]);
  });
});
