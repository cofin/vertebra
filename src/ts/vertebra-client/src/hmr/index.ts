import { computeBackoffDelay } from "../client/retry";

/**
 * Vertebra HMR Client
 *
 * Provides WebSocket-based hot-reloading for CSS and full-page reload for other assets.
 * Integrated with the Vertebra Python dev server.
 *
 * When imported as a script tag, auto-connects and exposes `window.__vertebra_hmr`.
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

// ---------------------------------------------------------------------------
// Error Overlay
// ---------------------------------------------------------------------------

let overlayElement: HTMLDivElement | null = null;

function showErrorOverlay(error: Error, filePath: string): void {
  if (typeof document === "undefined") return;
  hideErrorOverlay();

  const overlay = document.createElement("div");
  overlay.id = "__vertebra_error_overlay";
  overlay.style.cssText = [
    "position:fixed",
    "inset:0",
    "z-index:99999",
    "background:rgba(0,0,0,0.85)",
    "color:#ff5555",
    "font-family:monospace",
    "font-size:14px",
    "padding:32px",
    "overflow:auto",
    "white-space:pre-wrap",
  ].join(";");

  const container = document.createElement("div");
  container.style.cssText = "max-width:960px;margin:0 auto";

  const title = document.createElement("h2");
  title.style.cssText = "color:#ff7777;margin:0 0 16px";
  title.textContent = "[HMR] Update failed";
  container.appendChild(title);

  const modInfo = document.createElement("p");
  modInfo.style.cssText = "color:#aaa;margin:0 0 8px";
  modInfo.textContent = `File: ${filePath}`;
  container.appendChild(modInfo);

  const pre = document.createElement("pre");
  pre.style.cssText = "color:#ff5555;margin:0";
  pre.textContent = `${error.message}\n\n${error.stack ?? ""}`;
  container.appendChild(pre);

  const hint = document.createElement("p");
  hint.style.cssText = "color:#666;margin:16px 0 0;font-size:12px";
  hint.textContent = "Click to dismiss";
  container.appendChild(hint);

  overlay.appendChild(container);
  overlay.addEventListener("click", () => hideErrorOverlay());
  document.body.appendChild(overlay);
  overlayElement = overlay;
}

function hideErrorOverlay(): void {
  if (overlayElement) {
    overlayElement.remove();
    overlayElement = null;
  }
}

// ---------------------------------------------------------------------------
// HMR Client
// ---------------------------------------------------------------------------

class HotReloadClient {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setTimeout> | null = null;
  private _connected = false;
  private hmrPath: string;

  constructor(hmrPath = DEFAULT_HMR_PATH) {
    this.hmrPath = hmrPath;
  }

  /** Whether the WebSocket is currently connected. */
  isConnected(): boolean {
    return this._connected;
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
    this._connected = false;
  }

  private onOpen(): void {
    console.log("[vertebra] HMR connected");
    this.reconnectAttempts = 0;
    this._connected = true;
    this.resetPingTimer();
  }

  private onMessage(event: MessageEvent): void {
    this.resetPingTimer();

    let rawMsg: unknown;
    try {
      rawMsg = JSON.parse(String(event.data)) as unknown;
    } catch {
      return;
    }

    if (rawMsg === null || typeof rawMsg !== "object" || Array.isArray(rawMsg)) {
      return;
    }

    const msg = rawMsg as Record<string, unknown>;
    const kind = typeof msg.type === "string" ? msg.type : typeof msg.event === "string" ? msg.event : undefined;

    switch (kind) {
      case "connected":
        console.log("[vertebra] HMR server ready");
        break;

      case "update":
        if (Array.isArray(msg.updates)) {
          const updates = msg.updates.filter(
            (item): item is { path: string; timestamp: number } =>
              item !== null &&
              typeof item === "object" &&
              typeof (item as Record<string, unknown>).path === "string" &&
              typeof (item as Record<string, unknown>).timestamp === "number",
          );
          this.handleUpdate(updates);
        }
        break;

      case "css-update":
        if (typeof msg.path === "string") {
          this.hotSwapCss(msg.path);
        }
        break;

      case "full-reload":
        console.log(`[vertebra] full reload: ${String(msg.reason ?? msg.path ?? "")}`);
        window.location.reload();
        break;

      case "ping":
        this.ws?.send(JSON.stringify({ type: "pong" }));
        break;

      case "types-update":
      case "vertebra:types-changed":
        console.log(`[vertebra] types updated: ${String(msg.version ?? msg.generation ?? "")}`);
        window.dispatchEvent(new CustomEvent("vertebra:types-update", { detail: msg }));
        break;

      default:
        break;
    }
  }

  private onClose(event: CloseEvent): void {
    this.ws = null;
    this._connected = false;
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
      try {
        if (path.endsWith(".css")) {
          this.hotSwapCss(path);
        } else {
          console.log(`[vertebra] file changed: ${path}`);
          window.location.reload();
          return;
        }
        hideErrorOverlay();
      } catch (err) {
        console.error(`[vertebra] HMR update failed for ${path}:`, err);
        showErrorOverlay(err instanceof Error ? err : new Error(String(err)), path);
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

    const delay = computeBackoffDelay(this.reconnectAttempts, RECONNECT_BASE_MS, RECONNECT_MAX_MS);

    this.reconnectAttempts++;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }
}

export { HotReloadClient };

// ---------------------------------------------------------------------------
// Window global + manual init
// ---------------------------------------------------------------------------

declare global {
  interface Window {
    __vertebra_hmr?: HotReloadClient;
  }
}

export function initHotReload(hmrPath?: string): HotReloadClient {
  const client = new HotReloadClient(hmrPath);
  client.connect();
  if (typeof window !== "undefined") {
    window.__vertebra_hmr = client;
  }
  return client;
}
