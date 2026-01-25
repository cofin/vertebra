import { describe, expect, it } from "bun:test";
import { unlinkSync } from "node:fs";
import { decodeDelta, StateOpcode, stateId } from "../../../src/protocol/state";

import { StateHub } from "../../../src/broker/state-hub";

describe("StateHub", () => {
  it("applies LWW updates", () => {
    const hub = new StateHub();
    const state = stateId("GameState");

    hub.applyDelta({
      opcode: StateOpcode.FieldSet,
      stateId: state,
      fieldId: 1,
      timestampNs: 2n,
      value: "new",
    });

    hub.applyDelta({
      opcode: StateOpcode.FieldSet,
      stateId: state,
      fieldId: 1,
      timestampNs: 1n,
      value: "old",
    });

    expect(hub.getField(state, 1)?.value).toBe("new");
  });

  it("broadcasts deltas to subscribers", () => {
    const hub = new StateHub();
    const state = stateId("GameState");
    const received: Uint8Array[] = [];

    const unsubscribe = hub.subscribe(payload => received.push(payload));

    hub.applyDelta({
      opcode: StateOpcode.FieldSet,
      stateId: state,
      fieldId: 2,
      timestampNs: 10n,
      value: 42,
    });

    unsubscribe();

    expect(received).toHaveLength(1);
    const decoded = decodeDelta(received[0]);
    expect(decoded.opcode).toBe(StateOpcode.FieldSet);
    expect(decoded.value).toBe(42);
  });

  it("persists and recovers state", () => {
    const path = `.tmp-state-${crypto.randomUUID()}.json`;
    const state = stateId("GameState");

    try {
      const hub = new StateHub({ persistencePath: path });
      hub.applyDelta({
        opcode: StateOpcode.FieldSet,
        stateId: state,
        fieldId: 5,
        timestampNs: 100n,
        value: "persisted",
      });

      const recovered = new StateHub({ persistencePath: path });
      expect(recovered.getField(state, 5)?.value).toBe("persisted");
    } finally {
      unlinkSync(path);
    }
  });
});
