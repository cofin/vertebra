/**
 * Vertebra HMR Client
 *
 * Provides WebSocket-based hot-reloading for CSS and full-page reload for other assets.
 * Integrated with the Vertebra Python dev server.
 */

export type ServerMessage =
  | { type: "connected"; timestamp: number }
  | {
      type: "update";
      updates: Array<{ path: string; timestamp: number }>;
      timestamp: number;
    }
  | { type: "full-reload"; reason: string; timestamp: number }
  | { type: "ping"; timestamp: number }
  | { type: "types-update"; version: string; timestamp: number };

const DEFAULT_HMR_PATH = "/__vertebra/hmr";
const RECONNECT_MAX_ATTEMPTS = 20;
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;
const PING_TIMEOUT_MS = 45_000;

export class VertebraHmrClient {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private reconnectTimer: any = null;
  private pingTimer: any = null;
  private connected = false;
  private hmrPath: string;

  constructor(hmrPath = DEFAULT_HMR_PATH) {
    this.hmrPath = hmrPath;
  }

  /** Open the WebSocket connection. */
  connect(): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      return;
    }

    if (typeof window === "undefined") return;

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${protocol}//${window.location.host}${this.hmrPath}`;

    try {
      this.ws = new WebSocket(url);
      this.ws.onopen = this.onOpen.bind(this);
      this.ws.onmessage = this.onMessage.bind(this);
      this.ws.onclose = this.onClose.bind(this);
      this.ws.onerror = this.onError.bind(this);
    } catch {
      this.scheduleReconnect();
    }
  }

  /** Close the WebSocket connection and stop reconnection. */
  destroy(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.clearPingTimer();
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.reconnectAttempts = 0;
    this.connected = false;
  }

  private onOpen(): void {
    console.log("[vertebra] HMR connected");
    this.reconnectAttempts = 0;
    this.connected = true;
    this.resetPingTimer();
  }

  private onMessage(event: MessageEvent): void {
    this.resetPingTimer();

    let msg: ServerMessage;
    try {
      msg = JSON.parse(event.data as string);
    } catch {
      return;
    }

    switch (msg.type) {
      case "connected":
        console.log("[vertebra] HMR server ready");
        break;

      case "update":
        this.handleUpdate(msg.updates);
        break;

      case "full-reload":
        console.log(`[vertebra] full reload: ${msg.reason}`);
        window.location.reload();
        break;

      case "ping":
        this.ws?.send(JSON.stringify({ type: "pong" }));
        break;

      case "types-update":
        console.log(`[vertebra] types updated: ${msg.version}`);
        // Dispatch custom event for Vite plugin to consume
        window.dispatchEvent(new CustomEvent("vertebra:types-update", { detail: msg }));
        break;

      default:
        break;
    }
  }

  private onClose(event: CloseEvent): void {
    this.ws = null;
    this.connected = false;
    this.clearPingTimer();
    if (event.code !== 1000) {
      this.scheduleReconnect();
    }
  }

  private onError(): void {
    // onclose will handle reconnect
  }

  private handleUpdate(updates: Array<{ path: string; timestamp: number }>): void {
    for (const { path } of updates) {
      if (path.endsWith(".css")) {
        this.hotSwapCss(path);
      } else {
        console.log(`[vertebra] file changed: ${path}`);
        window.location.reload();
        return;
      }
    }
  }

  private hotSwapCss(path: string): void {
    const links = document.querySelectorAll<HTMLLinkElement>('link[rel="stylesheet"]');
    for (const link of links) {
      const href = link.getAttribute("href");
      if (href && (link.href.includes(path) || href.includes(path))) {
        const url = new URL(link.href);
        url.searchParams.set("t", String(Date.now()));
        link.href = url.toString();
        console.log(`[vertebra] CSS hot-swapped: ${path}`);
        return;
      }
    }
    window.location.reload();
  }

  private resetPingTimer(): void {
    this.clearPingTimer();
    this.pingTimer = setTimeout(() => {
      console.log("[vertebra] ping timeout, reconnecting");
      this.ws?.close();
    }, PING_TIMEOUT_MS);
  }

  private clearPingTimer(): void {
    if (this.pingTimer) {
      clearTimeout(this.pingTimer);
      this.pingTimer = null;
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= RECONNECT_MAX_ATTEMPTS) {
      console.error("[vertebra] HMR max reconnect attempts reached");
      return;
    }

    const delay = Math.min(RECONNECT_BASE_MS * 2 ** this.reconnectAttempts + Math.random() * 1000, RECONNECT_MAX_MS);

    this.reconnectAttempts++;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }
}
