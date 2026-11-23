import { BoundedKeySet } from "./internal/lru_set";
import { ReconnectScheduler } from "./internal/reconnect";

export interface SseClientOptions {
  withCredentials?: boolean;
  initialLastEventId?: string | null;
  lastEventIdQueryParam?: string;
  maxReconnectAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
}

export interface SseMessageMeta {
  event: string;
  lastEventId: string | null;
  raw: MessageEvent;
}

export type SseEventHandler<T = unknown> = (data: T, meta: SseMessageMeta) => void;

const DEFAULT_MAX_RECONNECT_ATTEMPTS = 20;
const DEFAULT_BASE_DELAY_MS = 500;
const DEFAULT_MAX_DELAY_MS = 30_000;
const MAX_IDEMPOTENCY_KEYS = 1024;

export class SseClient {
  private readonly baseUrl: string;
  private readonly withCredentials: boolean;
  private readonly lastEventIdParam: string;
  private readonly reconnect: ReconnectScheduler;

  private es: EventSource | null = null;
  private lastEventId: string | null;
  private manuallyClosed = false;
  private handlers = new Map<string, Set<SseEventHandler<unknown>>>();
  private boundDomListeners = new Map<string, (ev: Event) => void>();
  private seenIdempotencyKeys = new BoundedKeySet(MAX_IDEMPOTENCY_KEYS);

  constructor(url: string, options: SseClientOptions = {}) {
    this.baseUrl = url;
    this.withCredentials = options.withCredentials ?? false;
    this.lastEventId = options.initialLastEventId ?? null;
    this.lastEventIdParam = options.lastEventIdQueryParam ?? "lastEventId";
    this.reconnect = new ReconnectScheduler(
      options.maxReconnectAttempts ?? DEFAULT_MAX_RECONNECT_ATTEMPTS,
      options.baseDelayMs ?? DEFAULT_BASE_DELAY_MS,
      options.maxDelayMs ?? DEFAULT_MAX_DELAY_MS,
    );
  }

  /** Return the most recently observed SSE `Last-Event-ID`. */
  getLastEventId(): string | null {
    return this.lastEventId;
  }

  /** Return `true` when the underlying `EventSource` is open. */
  isConnected(): boolean {
    return this.es !== null && this.es.readyState === 1;
  }

  /** Register a typed handler for `event` (use `"message"` for default unnamed events). */
  on<T = unknown>(event: string, handler: SseEventHandler<T>): () => void {
    let set = this.handlers.get(event);
    if (!set) {
      set = new Set();
      this.handlers.set(event, set);
      if (this.es) {
        this.attachEventListener(this.es, event);
      }
    }
    const wrapped: SseEventHandler<unknown> = (data, meta) => {
      handler(data as T, meta);
    };
    set.add(wrapped);

    return () => {
      const current = this.handlers.get(event);
      if (!current) return;
      current.delete(wrapped);
      if (current.size === 0) {
        this.handlers.delete(event);
        const domListener = this.boundDomListeners.get(event);
        if (domListener && this.es) {
          this.es.removeEventListener(event, domListener as EventListener);
        }
        this.boundDomListeners.delete(event);
      }
    };
  }

  /** Open the `EventSource` stream, appending `lastEventId` when resuming. */
  connect(): void {
    if (typeof EventSource === "undefined") return;
    if (this.es && this.es.readyState !== 2) return;

    this.manuallyClosed = false;
    const targetUrl = this.buildConnectionUrl();
    const es = new EventSource(targetUrl, {
      withCredentials: this.withCredentials,
    });
    this.es = es;

    es.onopen = () => {
      this.reconnect.reset();
    };

    es.onerror = () => {
      if (this.manuallyClosed) return;
      this.cleanupEventSource();
      this.reconnect.schedule(() => this.connect());
    };

    for (const eventName of this.handlers.keys()) {
      this.attachEventListener(es, eventName);
    }
  }

  /** Permanently close the SSE stream and cancel pending reconnect timers. */
  close(): void {
    this.manuallyClosed = true;
    this.reconnect.cancel();
    this.cleanupEventSource();
  }

  private buildConnectionUrl(): string {
    if (!this.lastEventId) {
      return this.baseUrl;
    }
    const separator = this.baseUrl.includes("?") ? "&" : "?";
    return `${this.baseUrl}${separator}${encodeURIComponent(this.lastEventIdParam)}=${encodeURIComponent(this.lastEventId)}`;
  }

  private attachEventListener(es: EventSource, eventName: string): void {
    if (this.boundDomListeners.has(eventName)) return;
    const listener = (rawEvent: Event) => {
      const msgEvent = rawEvent as MessageEvent;
      if (msgEvent.lastEventId) {
        this.lastEventId = msgEvent.lastEventId;
      }
      let parsed: unknown = msgEvent.data;
      if (typeof msgEvent.data === "string") {
        try {
          parsed = JSON.parse(msgEvent.data) as unknown;
        } catch {
          parsed = msgEvent.data;
        }
      }
      const idempotencyKey =
        parsed !== null && typeof parsed === "object" && "idempotencyKey" in parsed
          ? (parsed as Record<string, unknown>).idempotencyKey
          : undefined;
      if (typeof idempotencyKey === "string" && !this.seenIdempotencyKeys.add(idempotencyKey)) {
        return;
      }
      const meta: SseMessageMeta = {
        event: eventName,
        lastEventId: this.lastEventId,
        raw: msgEvent,
      };
      const set = this.handlers.get(eventName);
      if (set) {
        for (const handler of set) {
          handler(parsed, meta);
        }
      }
    };
    this.boundDomListeners.set(eventName, listener);
    es.addEventListener(eventName, listener as EventListener);
  }

  private cleanupEventSource(): void {
    if (!this.es) return;
    for (const [eventName, listener] of this.boundDomListeners.entries()) {
      this.es.removeEventListener(eventName, listener as EventListener);
    }
    this.boundDomListeners.clear();
    this.es.close();
    this.es = null;
  }
}
