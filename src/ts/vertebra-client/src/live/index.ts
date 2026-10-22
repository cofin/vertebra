export const StateOpcode = {
  FieldSet: 0x01,
  FieldDelete: 0x02,
  ObjectReplace: 0x03,
  SyncRequest: 0x04,
  SyncResponse: 0x05,
  ActionCall: 0x06,
  ActionResult: 0x07,
  BatchPatch: 0x08,
} as const;

export type StateOpcodeValue = (typeof StateOpcode)[keyof typeof StateOpcode];

const FNV64_OFFSET = 0xcbf29ce484222325n;
const FNV64_PRIME = 0x100000001b3n;
const FNV64_MASK = 0xffffffffffffffffn;

const FNV32_OFFSET = 0x811c9dc5;
const FNV32_PRIME = 0x01000193;
const HLC_WALL_MASK = 0x0000ffffffffffffn;

const UTF8_ENCODER = new TextEncoder();

export function fnv1aStateId(name: string): bigint {
  const bytes = UTF8_ENCODER.encode(name);
  let hash = FNV64_OFFSET;
  for (const byte of bytes) {
    hash ^= BigInt(byte);
    hash = (hash * FNV64_PRIME) & FNV64_MASK;
  }
  return hash;
}

export function fnv1aFieldId(name: string): number {
  const bytes = UTF8_ENCODER.encode(name);
  let hash = FNV32_OFFSET;
  for (const byte of bytes) {
    hash ^= byte;
    hash = Math.imul(hash, FNV32_PRIME) >>> 0;
  }
  return hash >>> 0;
}

export class HybridLogicalClock {
  wallMs: bigint;
  logical: number;

  constructor(wallMs = 0n, logical = 0) {
    this.wallMs = wallMs & HLC_WALL_MASK;
    this.logical = logical & 0xffff;
  }

  pack(): bigint {
    return ((this.wallMs & HLC_WALL_MASK) << 16n) | BigInt(this.logical & 0xffff);
  }

  static unpack(packed: bigint): HybridLogicalClock {
    return new HybridLogicalClock((packed >> 16n) & HLC_WALL_MASK, Number(packed & 0xffffn));
  }

  tick(nowMs: bigint = BigInt(Date.now())): bigint {
    const clamped = nowMs & HLC_WALL_MASK;
    if (clamped > this.wallMs) {
      this.wallMs = clamped;
      this.logical = 0;
    } else {
      this.logical = Math.min(0xffff, this.logical + 1);
    }
    return this.pack();
  }

  receive(remotePacked: bigint, nowMs: bigint = BigInt(Date.now())): bigint {
    const remote = HybridLogicalClock.unpack(remotePacked);
    const clamped = nowMs & HLC_WALL_MASK;
    if (clamped > this.wallMs && clamped > remote.wallMs) {
      this.wallMs = clamped;
      this.logical = 0;
    } else if (this.wallMs === remote.wallMs) {
      this.logical = Math.min(0xffff, Math.max(this.logical, remote.logical) + 1);
    } else if (remote.wallMs > this.wallMs) {
      this.wallMs = remote.wallMs;
      this.logical = Math.min(0xffff, remote.logical + 1);
    } else {
      this.logical = Math.min(0xffff, this.logical + 1);
    }
    return this.pack();
  }
}

export interface StateDeltaMessage<T = unknown> {
  opcode: StateOpcodeValue;
  stateId: bigint;
  fieldId?: number;
  timestampNs?: bigint;
  value?: T;
  callId?: bigint;
}

export class OfflineDeltaQueue {
  private readonly queue: StateDeltaMessage[] = [];

  enqueue(delta: StateDeltaMessage): void {
    this.queue.push(delta);
  }

  drain(): StateDeltaMessage[] {
    return this.queue.splice(0, this.queue.length);
  }

  get size(): number {
    return this.queue.length;
  }
}

export type LiveStateListener<TState> = (state: Readonly<TState>) => void;

export class LiveStateClient<TState extends Record<string, unknown>> {
  readonly stateName: string;
  readonly stateId: bigint;
  private state: TState;
  private readonly hlc = new HybridLogicalClock();
  private readonly fieldTimestamps = new Map<number, bigint>();
  private readonly fieldIdToKey = new Map<number, keyof TState & string>();
  private readonly listeners = new Set<LiveStateListener<TState>>();
  private readonly offlineQueue = new OfflineDeltaQueue();
  private connected = true;
  private readonly onSend?: (delta: StateDeltaMessage) => void;

  constructor(
    stateName: string,
    initialState: TState,
    options?: {
      connected?: boolean;
      onSend?: (delta: StateDeltaMessage) => void;
    },
  ) {
    this.stateName = stateName;
    this.stateId = fnv1aStateId(stateName);
    this.state = { ...initialState };
    this.connected = options?.connected ?? true;
    this.onSend = options?.onSend;
    for (const key of Object.keys(initialState)) {
      this.fieldIdToKey.set(fnv1aFieldId(key), key);
    }
  }

  getSnapshot(): Readonly<TState> {
    return this.state;
  }

  get pendingOfflineCount(): number {
    return this.offlineQueue.size;
  }

  subscribe(listener: LiveStateListener<TState>): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  setConnected(connected: boolean): StateDeltaMessage[] {
    this.connected = connected;
    if (connected) {
      return this.flushOfflineQueue();
    }
    return [];
  }

  mutate<K extends keyof TState & string>(key: K, value: TState[K], nowMs?: bigint): StateDeltaMessage<TState[K]> {
    const fieldId = fnv1aFieldId(key);
    this.fieldIdToKey.set(fieldId, key);
    const ts = this.hlc.tick(nowMs);
    this.fieldTimestamps.set(fieldId, ts);
    this.state = { ...this.state, [key]: value };
    const delta: StateDeltaMessage<TState[K]> = {
      opcode: StateOpcode.FieldSet,
      stateId: this.stateId,
      fieldId,
      timestampNs: ts,
      value,
    };
    if (this.connected) {
      this.onSend?.(delta);
    } else {
      this.offlineQueue.enqueue(delta);
    }
    this.notify();
    return delta;
  }

  applyServerDelta(delta: StateDeltaMessage): boolean {
    if (delta.stateId !== this.stateId) {
      return false;
    }
    if (
      (delta.opcode === StateOpcode.FieldSet || delta.opcode === StateOpcode.ObjectReplace) &&
      delta.fieldId !== undefined &&
      delta.timestampNs !== undefined
    ) {
      const currentTs = this.fieldTimestamps.get(delta.fieldId) ?? 0n;
      if (delta.timestampNs < currentTs) {
        return false;
      }
      this.hlc.receive(delta.timestampNs);
      this.fieldTimestamps.set(delta.fieldId, delta.timestampNs);
      const key = this.fieldIdToKey.get(delta.fieldId);
      if (key !== undefined) {
        this.state = { ...this.state, [key]: delta.value };
        this.notify();
      }
      return true;
    }
    if (delta.opcode === StateOpcode.FieldDelete && delta.fieldId !== undefined && delta.timestampNs !== undefined) {
      const currentTs = this.fieldTimestamps.get(delta.fieldId) ?? 0n;
      if (delta.timestampNs < currentTs) {
        return false;
      }
      this.hlc.receive(delta.timestampNs);
      this.fieldTimestamps.set(delta.fieldId, delta.timestampNs);
      const key = this.fieldIdToKey.get(delta.fieldId);
      if (key !== undefined) {
        const nextState = { ...this.state };
        delete nextState[key];
        this.state = nextState;
        this.notify();
      }
      return true;
    }
    if (delta.opcode === StateOpcode.SyncResponse && delta.value && typeof delta.value === "object") {
      const incoming = delta.value as Record<string, unknown>;
      for (const k of Object.keys(incoming)) {
        this.fieldIdToKey.set(fnv1aFieldId(k), k as keyof TState & string);
      }
      this.state = { ...this.state, ...(incoming as Partial<TState>) };
      this.notify();
      return true;
    }
    return false;
  }

  flushOfflineQueue(): StateDeltaMessage[] {
    const drained = this.offlineQueue.drain();
    for (const delta of drained) {
      this.onSend?.(delta);
    }
    return drained;
  }

  private notify(): void {
    const snapshot = this.getSnapshot();
    for (const listener of this.listeners) {
      listener(snapshot);
    }
  }
}
