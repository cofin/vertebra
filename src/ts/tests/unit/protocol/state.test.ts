import { decode } from "@msgpack/msgpack";
import { decodeDelta, encodeDelta, fieldId, StateOpcode, stateId } from "../../../src/protocol/state";

describe("state protocol", () => {
  test("state and field id hashing", () => {
    expect(stateId("GameState")).toBe(0x374157f8ddd4c9a8n);
    expect(fieldId("score")).toBe(0xd22f9095);
  });

  test("encode/decode field set delta", () => {
    const delta = {
      opcode: StateOpcode.FieldSet,
      stateId: stateId("GameState"),
      fieldId: fieldId("score"),
      timestampNs: 1234n,
      value: { count: 1 },
    };

    const encoded = encodeDelta(delta);
    const raw = decode(encoded, { useBigInt64: true }) as unknown[];

    expect(raw).toEqual([
      StateOpcode.FieldSet,
      delta.stateId,
      delta.fieldId,
      delta.timestampNs,
      delta.value,
    ]);

    expect(decodeDelta(encoded)).toEqual(delta);
  });

  test("encode/decode field delete delta", () => {
    const delta = {
      opcode: StateOpcode.FieldDelete,
      stateId: stateId("GameState"),
      fieldId: fieldId("score"),
      timestampNs: 999n,
    };

    const encoded = encodeDelta(delta);
    const raw = decode(encoded, { useBigInt64: true }) as unknown[];

    expect(raw).toEqual([StateOpcode.FieldDelete, delta.stateId, delta.fieldId, delta.timestampNs]);

    expect(decodeDelta(encoded)).toEqual(delta);
  });
});
