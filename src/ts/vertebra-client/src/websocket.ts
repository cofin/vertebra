import { computeBackoffDelay } from "./client/retry";
import { BoundedKeySet } from "./internal/lru_set";

export interface WebSocketClientOptions {
  protocols?: string | string[];
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  maxReconnectAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
}

export interface PubSubWireFrame<T = unknown> {
  type: "subscribe" | "unsubscribe" | "publish" | "event" | "ping" | "pong";
  topic?: string;
  channel?: string;
  channels?: string[];
  eventId?: string | number;
  lastEventId?: string | null;
  idempotencyKey?: string;
  data?: T;
}

type TopicCallback<T = any> = (data: T, frame: PubSubWireFrame<T>) => void;

const DEFAULT_HEARTBEAT_INTERVAL_MS = 20_000;
const DEFAULT_HEARTBEAT_TIMEOUT_MS = 45_000;
const DEFAULT_MAX_RECONNECT_ATTEMPTS = 20;
const DEFAULT_BASE_DELAY_MS = 1000;
const DEFAULT_MAX_DELAY_MS = 30_000;
const MAX_IDEMPOTENCY_KEYS = 1024;

export class WebSocketClient {
  private readonly url: string;
  private readonly protocols?: string | string[];
  private readonly heartbeatIntervalMs: number;
  private readonly heartbeatTimeoutMs: number;
  private readonly maxReconnectAttempts: number;
  private readonly baseDelayMs: number;
  private readonly maxDelayMs: number;

  private ws: WebSocket | null = null;
  private connected = false;
  private manuallyClosed = false;
  private negotiatedSubprotocol = "";
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatIntervalTimer: ReturnType<typeof setInterval> | null = null;
  private heartbeatTimeoutTimer: ReturnType<typeof setTimeout> | null = null;

  private subscriptions = new Map<string, Set<TopicCallback>>();
  private lastEventIds = new Map<string, string>();
  private seenIdempotencyKeys = new BoundedKeySet(MAX_IDEMPOTENCY_KEYS);

  constructor(url: string, options: WebSocketClientOptions = {}) {
    this.url = url;
    this.protocols = options.protocols;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
    this.heartbeatTimeoutMs = options.heartbeatTimeoutMs ?? DEFAULT_HEARTBEAT_TIMEOUT_MS;
    this.maxReconnectAttempts = options.maxReconnectAttempts ?? DEFAULT_MAX_RECONNECT_ATTEMPTS;
    this.baseDelayMs = options.baseDelayMs ?? DEFAULT_BASE_DELAY_MS;
    this.maxDelayMs = options.maxDelayMs ?? DEFAULT_MAX_DELAY_MS;
  }

  /** Return `true` if the WebSocket connection is currently open. */
  isConnected(): boolean {
    return this.connected;
  }

  /** Return the `Sec-WebSocket-Protocol` subprotocol selected by the server. */
  getNegotiatedSubprotocol(): string {
    return this.negotiatedSubprotocol;
  }

  /** Return the most recent `eventId` received for `topic`. */
  getLastEventId(topic: string): string | null {
    return this.lastEventIds.get(topic) ?? null;
  }

  /** Subscribe to `topic`. Lazy-connects on first subscription and returns an unsubscribe closure. */
  subscribe<T = unknown>(topic: string, callback: TopicCallback<T>): () => void {
    let subs = this.subscriptions.get(topic);
    const isNewTopic = !subs;
    if (!subs) {
      subs = new Set();
      this.subscriptions.set(topic, subs);
    }
    subs.add(callback as TopicCallback);

    if (!this.ws && typeof WebSocket !== "undefined") {
      this.connect();
    } else if (this.connected && isNewTopic) {
      this.sendSubscribeFrame(topic);
    }

    return () => {
      const current = this.subscriptions.get(topic);
      if (!current) return;
      current.delete(callback as TopicCallback);
      if (current.size === 0) {
        this.subscriptions.delete(topic);
        if (this.connected) {
          this.sendFrame({ type: "unsubscribe", topic, channels: [topic] });
        }
      }
      if (this.subscriptions.size === 0) {
        this.close();
      }
    };
  }

  /** Publish `data` to `topic` with an optional `idempotencyKey`. */
  publish<T = unknown>(topic: string, data: T, idempotencyKey?: string): void {
    this.sendFrame({
      type: "publish",
      topic,
      channel: topic,
      data,
      ...(idempotencyKey ? { idempotencyKey } : {}),
    });
  }

  /** Explicitly open the WebSocket connection. */
  connect(): void {
    if (typeof WebSocket === "undefined") return;
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return;

    this.manuallyClosed = false;
    try {
      this.ws = this.protocols !== undefined ? new WebSocket(this.url, this.protocols) : new WebSocket(this.url);

      this.ws.onopen = () => {
        this.connected = true;
        this.reconnectAttempts = 0;
        this.negotiatedSubprotocol = this.ws?.protocol ?? "";
        this.startHeartbeat();
        for (const topic of this.subscriptions.keys()) {
          this.sendSubscribeFrame(topic);
        }
      };

      this.ws.onmessage = (event: MessageEvent) => {
        this.resetHeartbeatTimeout();
        this.handleIncomingMessage(event.data);
      };

      this.ws.onclose = (event: CloseEvent) => {
        this.ws = null;
        this.connected = false;
        this.stopHeartbeat();
        if (!this.manuallyClosed && event.code !== 1000 && this.subscriptions.size > 0) {
          this.scheduleReconnect();
        }
      };

      this.ws.onerror = () => {};
    } catch {
      this.scheduleReconnect();
    }
  }

  /** Close the WebSocket connection and stop reconnection and heartbeat timers. */
  close(): void {
    this.manuallyClosed = true;
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.stopHeartbeat();
    if (this.ws) {
      this.ws.close(1000);
      this.ws = null;
    }
    this.connected = false;
    this.reconnectAttempts = 0;
  }

  private sendSubscribeFrame(topic: string): void {
    const lastEventId = this.lastEventIds.get(topic) ?? null;
    this.sendFrame({
      type: "subscribe",
      topic,
      channels: [topic],
      lastEventId,
    });
  }

  private sendFrame(frame: PubSubWireFrame): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(frame));
    }
  }

  private handleIncomingMessage(raw: unknown): void {
    if (typeof raw !== "string") return;
    let frame: PubSubWireFrame;
    try {
      frame = JSON.parse(raw) as PubSubWireFrame;
    } catch {
      return;
    }

    if (frame.type === "ping") {
      this.sendFrame({ type: "pong" });
      return;
    }
    if (frame.type === "pong") {
      return;
    }

    if (frame.idempotencyKey && !this.seenIdempotencyKeys.add(frame.idempotencyKey)) {
      return;
    }

    const topic = frame.topic ?? frame.channel;
    if (!topic) return;

    if (frame.eventId !== undefined && frame.eventId !== null) {
      this.lastEventIds.set(topic, String(frame.eventId));
    }

    const callbacks = this.subscriptions.get(topic);
    if (callbacks) {
      for (const cb of callbacks) {
        cb(frame.data, frame);
      }
    }
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.resetHeartbeatTimeout();
    this.heartbeatIntervalTimer = setInterval(() => {
      this.sendFrame({ type: "ping" });
    }, this.heartbeatIntervalMs);
  }

  private resetHeartbeatTimeout(): void {
    if (this.heartbeatTimeoutTimer !== null) {
      clearTimeout(this.heartbeatTimeoutTimer);
    }
    this.heartbeatTimeoutTimer = setTimeout(() => {
      if (this.ws) {
        this.ws.close(4000, "heartbeat timeout");
      }
    }, this.heartbeatTimeoutMs);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatIntervalTimer !== null) {
      clearInterval(this.heartbeatIntervalTimer);
      this.heartbeatIntervalTimer = null;
    }
    if (this.heartbeatTimeoutTimer !== null) {
      clearTimeout(this.heartbeatTimeoutTimer);
      this.heartbeatTimeoutTimer = null;
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) return;
    const delay = computeBackoffDelay(this.reconnectAttempts, this.baseDelayMs, this.maxDelayMs);
    this.reconnectAttempts += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }
}
