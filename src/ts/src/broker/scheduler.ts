export type SchedulerItem<T> = {
  item: T;
  enqueuedAt: number;
  deadlineAt: number;
  cost: number;
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

export type SchedulerStats = {
  pending: number;
  clients: SchedulerClientStats[];
};

type ClientQueue<T> = {
  id: string;
  weight: number;
  deficit: number;
  misses: number;
  lastServedAt: number | null;
  requestsTotal: number;
  waitTimeMsTotal: number;
  starvationEvents: number;
  queue: Array<SchedulerItem<T>>;
};

export type SchedulerConfig = {
  quantum: number;
  starvationMs: number;
};

export class FairScheduler<T> {
  private queues = new Map<string, ClientQueue<T>>();
  private order: string[] = [];
  private cursor = 0;
  private quantum: number;
  private starvationMs: number;
  private pending = 0;

  constructor(config: SchedulerConfig) {
    this.quantum = Math.max(1, Math.floor(config.quantum));
    this.starvationMs = Math.max(1, Math.floor(config.starvationMs));
  }

  enqueue(clientId: string, weight: number, item: T, timeoutMs: number, cost = 1): void {
    const now = Date.now();
    const deadlineAt = timeoutMs > 0 ? now + timeoutMs : Number.MAX_SAFE_INTEGER;
    const entry: SchedulerItem<T> = {
      item,
      enqueuedAt: now,
      deadlineAt,
      cost: Math.max(1, Math.floor(cost)),
    };
    const queue = this.ensureQueue(clientId, weight);
    queue.queue.push(entry);
    queue.requestsTotal += 1;
    this.pending += 1;
  }

  collectExpired(now = Date.now()): SchedulerItem<T>[] {
    const expired: SchedulerItem<T>[] = [];
    for (const queue of this.queues.values()) {
      while (queue.queue.length > 0 && queue.queue[0].deadlineAt <= now) {
        expired.push(queue.queue.shift() as SchedulerItem<T>);
        this.pending -= 1;
      }
    }
    return expired;
  }

  next(now = Date.now()): SchedulerItem<T> | null {
    if (this.pending <= 0) {
      return null;
    }
    const totalQueues = this.order.length;
    if (totalQueues === 0) {
      return null;
    }
    for (let attempts = 0; attempts < totalQueues; attempts += 1) {
      const idx = (this.cursor + attempts) % totalQueues;
      const clientId = this.order[idx];
      const queue = this.queues.get(clientId);
      if (!queue || queue.queue.length === 0) {
        continue;
      }
      queue.deficit += this.quantum * queue.weight;
      const candidate = queue.queue[0];
      if (queue.lastServedAt && now - queue.lastServedAt >= this.starvationMs) {
        queue.starvationEvents += 1;
        queue.deficit = Math.max(queue.deficit, candidate.cost);
      }
      if (queue.deficit < candidate.cost) {
        queue.misses += 1;
        continue;
      }
      queue.queue.shift();
      queue.deficit -= candidate.cost;
      queue.lastServedAt = now;
      queue.misses = 0;
      queue.waitTimeMsTotal += now - candidate.enqueuedAt;
      this.pending -= 1;
      this.cursor = (idx + 1) % totalQueues;
      return candidate;
    }
    return null;
  }

  stats(now = Date.now()): SchedulerStats {
    const clients: SchedulerClientStats[] = [];
    for (const queue of this.queues.values()) {
      const oldest = queue.queue[0]?.enqueuedAt ?? null;
      clients.push({
        id: queue.id,
        weight: queue.weight,
        pending: queue.queue.length,
        oldestMs: oldest ? Math.max(0, now - oldest) : null,
        lastServedMs: queue.lastServedAt ? Math.max(0, now - queue.lastServedAt) : null,
        requestsTotal: queue.requestsTotal,
        waitTimeMsTotal: queue.waitTimeMsTotal,
        starvationEvents: queue.starvationEvents,
      });
    }
    return { pending: this.pending, clients };
  }

  updateWeight(clientId: string, weight: number): void {
    const queue = this.ensureQueue(clientId, weight);
    queue.weight = Math.max(1, Math.floor(weight));
  }

  private ensureQueue(clientId: string, weight: number): ClientQueue<T> {
    const existing = this.queues.get(clientId);
    if (existing) {
      return existing;
    }
    const queue: ClientQueue<T> = {
      id: clientId,
      weight: Math.max(1, Math.floor(weight)),
      deficit: 0,
      misses: 0,
      lastServedAt: null,
      requestsTotal: 0,
      waitTimeMsTotal: 0,
      starvationEvents: 0,
      queue: [],
    };
    this.queues.set(clientId, queue);
    this.order.push(clientId);
    return queue;
  }
}
