import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

import { decodeDelta, encodeDelta, StateDelta, StateOpcode } from "../protocol/state";

type FieldEntry = {
  timestampNs: bigint;
  value?: unknown;
  deleted?: boolean;
};

type PersistedField = {
  ts: string;
  value?: unknown;
  deleted?: boolean;
};

type PersistedState = {
  states: Record<string, Record<string, PersistedField>>;
};

export type StateHubOptions = {
  persistencePath?: string;
};

export class StateHub {
  private readonly states = new Map<bigint, Map<number, FieldEntry>>();
  private readonly subscribers = new Map<string, (payload: Uint8Array) => void>();
  private readonly persistencePath?: string;

  constructor(options: StateHubOptions = {}) {
    this.persistencePath = options.persistencePath;
    if (this.persistencePath && existsSync(this.persistencePath)) {
      this.loadFromDisk();
    }
  }

  subscribe(callback: (payload: Uint8Array) => void): () => void {
    const id = crypto.randomUUID();
    this.subscribers.set(id, callback);
    return () => this.subscribers.delete(id);
  }

  applyDelta(deltaOrPayload: StateDelta | Uint8Array): boolean {
    const decoded =
      deltaOrPayload instanceof Uint8Array ? decodeDelta(deltaOrPayload) : deltaOrPayload;
    const applied = this.applyDecoded(decoded);
    if (!applied) {
      return false;
    }

    const payload = deltaOrPayload instanceof Uint8Array ? deltaOrPayload : encodeDelta(decoded);
    for (const callback of this.subscribers.values()) {
      callback(payload);
    }

    if (this.persistencePath) {
      this.persist();
    }

    return true;
  }

  getField(stateId: bigint, fieldId: number): FieldEntry | undefined {
    return this.states.get(stateId)?.get(fieldId);
  }

  private applyDecoded(delta: StateDelta): boolean {
    switch (delta.opcode) {
      case StateOpcode.FieldSet:
      case StateOpcode.ObjectReplace:
        return this.applyFieldDelta(delta);
      case StateOpcode.FieldDelete:
        return this.applyDeleteDelta(delta);
      case StateOpcode.SyncRequest:
      case StateOpcode.SyncResponse:
        return false;
      default:
        return false;
    }
  }

  private applyFieldDelta(delta: StateDelta): boolean {
    if (delta.fieldId === undefined || delta.timestampNs === undefined) {
      throw new Error("fieldId and timestampNs are required for field updates");
    }

    const stateId = delta.stateId;
    const state = this.ensureState(stateId);
    const nextTimestamp = toBigInt(delta.timestampNs, "timestampNs");
    const existing = state.get(delta.fieldId);

    if (existing && existing.timestampNs >= nextTimestamp) {
      return false;
    }

    state.set(delta.fieldId, {
      timestampNs: nextTimestamp,
      value: delta.value,
      deleted: false,
    });
    return true;
  }

  private applyDeleteDelta(delta: StateDelta): boolean {
    if (delta.fieldId === undefined || delta.timestampNs === undefined) {
      throw new Error("fieldId and timestampNs are required for field deletes");
    }

    const stateId = delta.stateId;
    const state = this.ensureState(stateId);
    const nextTimestamp = toBigInt(delta.timestampNs, "timestampNs");
    const existing = state.get(delta.fieldId);

    if (existing && existing.timestampNs >= nextTimestamp) {
      return false;
    }

    state.set(delta.fieldId, {
      timestampNs: nextTimestamp,
      deleted: true,
    });
    return true;
  }

  private ensureState(stateId: bigint): Map<number, FieldEntry> {
    let state = this.states.get(stateId);
    if (!state) {
      state = new Map();
      this.states.set(stateId, state);
    }
    return state;
  }

  private persist(): void {
    if (!this.persistencePath) return;

    const data: PersistedState = { states: {} };
    for (const [stateId, fields] of this.states) {
      const record: Record<string, PersistedField> = {};
      for (const [fieldId, entry] of fields) {
        record[fieldId.toString()] = {
          ts: entry.timestampNs.toString(),
          value: entry.value,
          deleted: entry.deleted,
        };
      }
      data.states[stateId.toString()] = record;
    }

    const directory = dirname(this.persistencePath);
    if (directory && directory !== ".") {
      mkdirSync(directory, { recursive: true });
    }
    writeFileSync(this.persistencePath, JSON.stringify(data), "utf8");
  }

  private loadFromDisk(): void {
    if (!this.persistencePath || !existsSync(this.persistencePath)) {
      return;
    }

    const raw = readFileSync(this.persistencePath, "utf8");
    if (!raw) {
      return;
    }

    const data = JSON.parse(raw) as PersistedState;
    if (!data.states) {
      return;
    }

    for (const [stateIdStr, fields] of Object.entries(data.states)) {
      const stateId = BigInt(stateIdStr);
      const state = new Map<number, FieldEntry>();
      for (const [fieldIdStr, entry] of Object.entries(fields)) {
        state.set(Number(fieldIdStr), {
          timestampNs: BigInt(entry.ts),
          value: entry.value,
          deleted: entry.deleted ?? false,
        });
      }
      this.states.set(stateId, state);
    }
  }
}

function toBigInt(value: bigint | number, label: string): bigint {
  if (typeof value === "bigint") {
    return value;
  }
  if (Number.isFinite(value)) {
    return BigInt(value);
  }
  throw new Error(`${label} must be a number or bigint`);
}
