/**
 * WebSocket subscription manager for Vertebra.
 *
 * Lazy-connects on first subscription, auto-reconnects on disconnect,
 * and supports multiple concurrent channel subscriptions.
 */

const RECONNECT_MAX_ATTEMPTS = 20;
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;

type Callback = (data: any) => void;

export class VertebraWebSocket {
  private ws: WebSocket | null = null;
  private subscriptions = new Map<string, Set<Callback>>();
  private reconnectAttempts = 0;
  private reconnectTimer: any = null;
  private _connected = false;
  private wsURL: string;

  constructor(wsURL: string) {
    this.wsURL = wsURL;
  }

  /** Whether the WebSocket is currently connected. */
  isConnected(): boolean {
    return this._connected;
  }

  /**
   * Subscribe to a channel. Returns an unsubscribe function.
   *
   * Lazy-connects on first subscription. When the last subscription
   * is removed, the connection is closed.
   */
  subscribe<T>(channel: string, callback: (data: T) => void): () => void {
    let subs = this.subscriptions.get(channel);
    if (!subs) {
      subs = new Set();
      this.subscriptions.set(channel, subs);
    }
    subs.add(callback as Callback);

    // First subscription — connect
    if (!this.ws && typeof WebSocket !== "undefined") {
      this.connect();
    } else if (this._connected) {
      // Already connected — notify server of new channel
      this.sendSubscribe([channel]);
    }

    return () => {
      subs?.delete(callback as Callback);
      if (subs?.size === 0) {
        this.subscriptions.delete(channel);
      }
      // Last subscription removed — disconnect
      if (this.subscriptions.size === 0) {
        this.disconnect();
      }
    };
  }

  private connect(): void {
    if (this.ws?.readyState === WebSocket.OPEN) return;

    try {
      this.ws = new WebSocket(this.wsURL);
      this.ws.onopen = () => {
        this._connected = true;
        this.reconnectAttempts = 0;
        // Subscribe to all active channels
        const channels = Array.from(this.subscriptions.keys());
        if (channels.length > 0) {
          this.sendSubscribe(channels);
        }
      };
      this.ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data as string);
          if (msg.channel && this.subscriptions.has(msg.channel)) {
            for (const cb of this.subscriptions.get(msg.channel)!) {
              cb(msg.data);
            }
          }
        } catch {
          // Ignore unparseable messages
        }
      };
      this.ws.onclose = (event) => {
        this.ws = null;
        this._connected = false;
        if (event.code !== 1000 && this.subscriptions.size > 0) {
          this.scheduleReconnect();
        }
      };
      this.ws.onerror = () => {
        // onclose handles reconnect
      };
    } catch {
      this.scheduleReconnect();
    }
  }

  private disconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close(1000);
      this.ws = null;
    }
    this._connected = false;
    this.reconnectAttempts = 0;
  }

  private sendSubscribe(channels: string[]): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ type: "subscribe", channels }));
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= RECONNECT_MAX_ATTEMPTS) return;

    const delay = Math.min(RECONNECT_BASE_MS * 2 ** this.reconnectAttempts + Math.random() * 1000, RECONNECT_MAX_MS);
    this.reconnectAttempts++;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }
}
