import { describe, expect, it } from "vitest";
import {
  fnv1aFieldId,
  fnv1aStateId,
  HybridLogicalClock,
  LiveStateClient,
  type StateDeltaMessage,
  StateOpcode,
} from "../src/live/index";

describe("LiveStateClient and HLC synchronization", () => {
  it("computes FNV-1a state and field IDs matching Rust core", () => {
    expect(fnv1aStateId("GameState")).toBe(0x374157f8ddd4c9a8n);
    expect(fnv1aFieldId("score")).toBe(0xd22f9095);
  });

  it("queues optimistic updates while offline and flushes on reconnect", () => {
    const sent: StateDeltaMessage[] = [];
    const client = new LiveStateClient(
      "GameState",
      { score: 0, title: "lobby" },
      {
        connected: false,
        onSend: (delta) => sent.push(delta),
      },
    );

    const delta = client.mutate("score", 42, 1000n);
    expect(client.getSnapshot().score).toBe(42);
    expect(client.pendingOfflineCount).toBe(1);
    expect(sent).toHaveLength(0);

    const staleRejected = client.applyServerDelta({
      opcode: StateOpcode.FieldSet,
      stateId: client.stateId,
      fieldId: fnv1aFieldId("score"),
      timestampNs: (delta.timestampNs ?? 1n) - 1n,
      value: 10,
    });
    expect(staleRejected).toBe(false);
    expect(client.getSnapshot().score).toBe(42);

    const flushed = client.setConnected(true);
    expect(flushed).toHaveLength(1);
    expect(sent).toHaveLength(1);
    expect(client.pendingOfflineCount).toBe(0);
  });

  it("maintains monotonic HLC timestamps across ticks", () => {
    const hlc = new HybridLogicalClock(5000n, 0);
    const t1 = hlc.tick(5000n);
    const t2 = hlc.tick(5000n);
    expect(t2 > t1).toBe(true);
  });

  it("deletes a field when applying StateOpcode.FieldDelete", () => {
    const client = new LiveStateClient("GameState", { score: 10, title: "level1" });
    expect(client.getSnapshot().title).toBe("level1");

    const success = client.applyServerDelta({
      opcode: StateOpcode.FieldDelete,
      stateId: client.stateId,
      fieldId: fnv1aFieldId("title"),
      timestampNs: 2000n,
    });
    expect(success).toBe(true);
    expect("title" in client.getSnapshot()).toBe(false);
  });

  it("produces state and field IDs matching expected hex representation", () => {
    expect(fnv1aStateId("GameState").toString(16)).toBe("374157f8ddd4c9a8");
    expect(fnv1aFieldId("score").toString(16)).toBe("d22f9095");
  });
});
