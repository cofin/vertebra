import { decode, encode } from "@msgpack/msgpack";

export enum StateOpcode {
  FieldSet = 0x01,
  FieldDelete = 0x02,
  ObjectReplace = 0x03,
  SyncRequest = 0x04,
  SyncResponse = 0x05,
}

export type StateDelta = {
  opcode: StateOpcode;
  stateId: bigint;
  fieldId?: number;
  timestampNs?: bigint;
  value?: unknown;
};

const FNV32_OFFSET = 0x811c9dc5;
const FNV32_PRIME = 0x01000193;
const FNV64_OFFSET = 0xcbf29ce484222325n;
const FNV64_PRIME = 0x100000001b3n;
const FNV64_MASK = 0xffffffffffffffffn;

export function stateId(name: string): bigint {
  let hash = FNV64_OFFSET;
  const bytes = new TextEncoder().encode(name);
  for (const byte of bytes) {
    hash ^= BigInt(byte);
    hash = (hash * FNV64_PRIME) & FNV64_MASK;
  }
  return hash;
}

export function fieldId(name: string): number {
  let hash = FNV32_OFFSET;
  const bytes = new TextEncoder().encode(name);
  for (const byte of bytes) {
    hash ^= byte;
    hash = Math.imul(hash, FNV32_PRIME);
  }
  return hash >>> 0;
}

function toBigInt(value: unknown, label: string): bigint {
  if (typeof value === "bigint") {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return BigInt(value);
  }
  throw new Error(`${label} must be a bigint or number`);
}

function toNumber(value: unknown, label: string): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  throw new Error(`${label} must be a number`);
}

export function encodeDelta(delta: StateDelta): Uint8Array {
  const opcode = delta.opcode;

  if (opcode === StateOpcode.FieldSet || opcode === StateOpcode.ObjectReplace) {
    if (delta.fieldId === undefined || delta.timestampNs === undefined) {
      throw new Error("fieldId and timestampNs are required for field updates");
    }
    return encode([opcode, delta.stateId, delta.fieldId, delta.timestampNs, delta.value], {
      useBigInt64: true,
    });
  }

  if (opcode === StateOpcode.FieldDelete) {
    if (delta.fieldId === undefined || delta.timestampNs === undefined) {
      throw new Error("fieldId and timestampNs are required for field deletes");
    }
    return encode([opcode, delta.stateId, delta.fieldId, delta.timestampNs], { useBigInt64: true });
  }

  if (opcode === StateOpcode.SyncRequest) {
    return encode([opcode, delta.stateId], { useBigInt64: true });
  }

  if (opcode === StateOpcode.SyncResponse) {
    return encode([opcode, delta.stateId, delta.value], { useBigInt64: true });
  }

  throw new Error(`Unsupported opcode: ${opcode}`);
}

export function decodeDelta(data: Uint8Array): StateDelta {
  const raw = decode(data, { useBigInt64: true }) as unknown;
  if (!Array.isArray(raw) || raw.length < 2) {
    throw new Error("State delta payload must be an array with at least 2 elements");
  }

  const opcode = toNumber(raw[0], "opcode") as StateOpcode;
  const stateIdValue = toBigInt(raw[1], "stateId");

  if (opcode === StateOpcode.FieldSet || opcode === StateOpcode.ObjectReplace) {
    if (raw.length !== 5) {
      throw new Error("Field update delta must have 5 elements");
    }
    return {
      opcode,
      stateId: stateIdValue,
      fieldId: toNumber(raw[2], "fieldId"),
      timestampNs: toBigInt(raw[3], "timestampNs"),
      value: raw[4],
    };
  }

  if (opcode === StateOpcode.FieldDelete) {
    if (raw.length !== 4) {
      throw new Error("Field delete delta must have 4 elements");
    }
    return {
      opcode,
      stateId: stateIdValue,
      fieldId: toNumber(raw[2], "fieldId"),
      timestampNs: toBigInt(raw[3], "timestampNs"),
    };
  }

  if (opcode === StateOpcode.SyncRequest) {
    if (raw.length !== 2) {
      throw new Error("Sync request delta must have 2 elements");
    }
    return { opcode, stateId: stateIdValue };
  }

  if (opcode === StateOpcode.SyncResponse) {
    if (raw.length !== 3) {
      throw new Error("Sync response delta must have 3 elements");
    }
    return { opcode, stateId: stateIdValue, value: raw[2] };
  }

  throw new Error(`Unsupported opcode: ${opcode}`);
}
