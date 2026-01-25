import { describe, expect, it } from "bun:test";

import { StateSseServer } from "../../../src/server/sse";

function decodeEvent(chunk: Uint8Array): string {
  return new TextDecoder().decode(chunk);
}

describe("StateSseServer", () => {
  it("broadcasts payloads to connected clients", async () => {
    const server = new StateSseServer({ keepAliveMs: 0 });
    const response = server.handle(new Request("http://localhost/state/sse"));
    const reader = response.body!.getReader();

    server.publish(new Uint8Array([1, 2, 3]));

    const { value, done } = await reader.read();
    expect(done).toBe(false);

    const text = decodeEvent(value!);
    expect(text).toContain("event: state");
    expect(text).toContain("id: 1");
    expect(text).toContain("data:");
  });

  it("replays events after Last-Event-ID", async () => {
    const server = new StateSseServer({ keepAliveMs: 0, historySize: 10 });

    server.publish(new Uint8Array([9]));
    server.publish(new Uint8Array([10]));

    const request = new Request("http://localhost/state/sse", {
      headers: { "Last-Event-ID": "1" },
    });
    const response = server.handle(request);
    const reader = response.body!.getReader();

    const { value } = await reader.read();
    const text = decodeEvent(value!);
    expect(text).toContain("id: 2");
  });
});
