import { describe, expect, it } from "bun:test";

import { StateLongPollServer } from "../../../src/server/long-poll";

describe("StateLongPollServer", () => {
  it("returns pending events immediately", async () => {
    const server = new StateLongPollServer({ timeoutMs: 5 });
    server.publish(new Uint8Array([1, 2]));

    const response = await server.handle(new Request("http://localhost/state/poll?since=0"));
    const data = await response!.json();

    expect(data.id).toBe(1);
    expect(data.payload).toBe(Buffer.from(new Uint8Array([1, 2])).toString("base64"));
  });

  it("returns 204 when no updates before timeout", async () => {
    const server = new StateLongPollServer({ timeoutMs: 5 });
    const response = await server.handle(new Request("http://localhost/state/poll?since=10"));
    expect(response?.status).toBe(204);
  });

  it("wakes waiting poll when new event arrives", async () => {
    const server = new StateLongPollServer({ timeoutMs: 50 });
    const pending = server.handle(new Request("http://localhost/state/poll?since=0"));

    server.publish(new Uint8Array([9]));

    const response = await pending;
    const data = await response!.json();
    expect(data.id).toBe(1);
  });
});
