import { describe, expect, it } from "bun:test";

import { StateWebSocketServer } from "../../../src/server/websocket";

type FakeWebSocket = {
  data: { sessionId: string; lastEventId?: number };
  sent: string[];
  send: (payload: string) => void;
  close: () => void;
};

function createSocket(lastEventId?: number): FakeWebSocket {
  return {
    data: { sessionId: crypto.randomUUID(), lastEventId },
    sent: [],
    send(payload: string) {
      this.sent.push(payload);
    },
    close() {},
  };
}

describe("StateWebSocketServer", () => {
  it("broadcasts deltas to connected sockets", () => {
    const server = new StateWebSocketServer({ heartbeatMs: 0 });
    const ws = createSocket();

    server.handleOpen(ws as any);
    server.publish(new Uint8Array([1, 2, 3]));

    expect(ws.sent.length).toBe(1);
    const message = JSON.parse(ws.sent[0]);
    expect(message.type).toBe("delta");
    expect(message.id).toBe(1);
  });

  it("replays history for reconnecting sockets", () => {
    const server = new StateWebSocketServer({ heartbeatMs: 0, historySize: 10 });
    server.publish(new Uint8Array([9]));
    server.publish(new Uint8Array([10]));

    const ws = createSocket(1);
    server.handleOpen(ws as any);

    expect(ws.sent.length).toBe(1);
    const message = JSON.parse(ws.sent[0]);
    expect(message.id).toBe(2);
  });

  it("invokes onDelta for inbound messages", () => {
    const server = new StateWebSocketServer({ heartbeatMs: 0 });
    const ws = createSocket();
    let received: Uint8Array | null = null;

    server.onDelta = (payload) => {
      received = payload;
    };

    const payload = new Uint8Array([7, 8]);
    const message = JSON.stringify({
      type: "delta",
      payload: Buffer.from(payload).toString("base64"),
    });

    server.handleMessage(ws as any, message);

    expect(received).not.toBeNull();
    expect(received?.[0]).toBe(7);
  });
});
