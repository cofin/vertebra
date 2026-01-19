import { encode, decode } from "@msgpack/msgpack";

import { ShmRing } from "./shm";
import type { PoolStats } from "./base-pool";

export type { PoolStats };

export type BrokerClientConfig = {
  name?: string;
  controlName?: string;
  slotSize?: number;
  slots?: number;
  timeoutMs?: number;
  clientId?: string;
  weight?: number;
};

export type SchedulerClientStats = {
  id: string;
  weight: number;
  pending: number;
  oldestMs: number | null;
  lastServedMs: number | null;
  requestsTotal: number;
  waitTimeMsTotal: number;
  starvationEvents: number;
};

export type SchedulerPoolStats = {
  name: string;
  pending: number;
  clients: SchedulerClientStats[];
};

export type BrokerStats = {
  pools: PoolStats[];
  scheduler?: { pools: SchedulerPoolStats[] };
};

export class BrokerClient {
  private config: Required<BrokerClientConfig>;
  private control?: ShmRing;
  private request?: ShmRing;
  private response?: ShmRing;
  private seq = 0;

  constructor(config: BrokerClientConfig = {}) {
    const name = config.name ?? "vertebra-broker";
    this.config = {
      name,
      controlName: config.controlName ?? `/${name}.control`,
      slotSize: config.slotSize ?? 65_536,
      slots: config.slots ?? 1024,
      timeoutMs: config.timeoutMs ?? 5_000,
      clientId: config.clientId ?? crypto.randomUUID().replaceAll("-", ""),
      weight: config.weight ?? 1,
    };
  }

  async stats(pool?: string): Promise<BrokerStats> {
    await this.ensureConnected();
    const response = await this.requestPayload({ kind: "stats", pool: pool ?? null });
    if (!response.ok) {
      throw new Error(response.error?.message ?? "Broker stats error");
    }
    return response.stats ?? { pools: [] };
  }

  private async ensureConnected(): Promise<void> {
    if (this.control) {
      return;
    }
    this.control = ShmRing.open(this.config.controlName, this.config.slotSize, this.config.slots);
    this.request = ShmRing.create(this.requestRingName(), this.config.slotSize, this.config.slots);
    this.response = ShmRing.create(
      this.responseRingName(),
      this.config.slotSize,
      this.config.slots,
    );
    await this.sendControl({
      id: this.nextId(),
      kind: "register_client",
      client_id: this.config.clientId,
      req_ring: this.requestRingName(),
      resp_ring: this.responseRingName(),
      weight: this.config.weight,
    });
    const response = await this.recvResponse();
    if (!response.ok) {
      throw new Error(response.error?.message ?? "Broker register error");
    }
  }

  private requestRingName(): string {
    return `/${this.config.name}.client.${this.config.clientId}.req`;
  }

  private responseRingName(): string {
    return `/${this.config.name}.client.${this.config.clientId}.resp`;
  }

  private nextId(): number {
    this.seq += 1;
    return this.seq;
  }

  private async sendControl(payload: Record<string, unknown>): Promise<void> {
    if (!this.control) {
      throw new Error("Control ring unavailable");
    }
    const encoded = encode(payload);
    this.control.send(encoded, 0, this.config.timeoutMs);
  }

  private async requestPayload(payload: Record<string, unknown>): Promise<Record<string, any>> {
    if (!this.request) {
      throw new Error("Request ring unavailable");
    }
    payload.id = this.nextId();
    const encoded = encode(payload);
    this.request.send(encoded, 0, this.config.timeoutMs);
    return await this.recvResponse();
  }

  private async recvResponse(): Promise<Record<string, any>> {
    if (!this.response) {
      throw new Error("Response ring unavailable");
    }
    const chunk = this.response.recv(this.config.timeoutMs);
    if (!chunk) {
      throw new Error("Broker response timed out");
    }
    const { data } = chunk;
    return decode(data) as Record<string, any>;
  }
}
