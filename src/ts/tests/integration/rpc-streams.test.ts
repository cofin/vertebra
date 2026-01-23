import { describe, expect, it } from "bun:test";
import { createClient, type RpcTransport } from "../../src/client";

// Mock transport for testing client logic independent of network
class MockTransport implements RpcTransport {
  async request(procedureId: string, payload: unknown) {
    if (procedureId === "hello") return "world";
    return null;
  }

  subscribe(procedureId: string, payload: unknown, onData: (data: unknown) => void) {
    if (procedureId === "counter") {
      let count = 0;
      const interval = setInterval(() => {
        onData(count++);
        if (count > 3) clearInterval(interval);
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
  
  // Async iterator test would go here once implemented fully
});
