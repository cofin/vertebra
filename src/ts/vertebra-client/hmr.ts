/**
 * Vertebra HMR Client
 *
 * Shipped pre-built in the Python wheel at runtime/js/dist/hmr.js.
 * Injected into HTML via <script type="module" src="/__vertebra/client/hmr.js">
 * during dev mode.
 *
 * @packageDocumentation
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Message types sent from the HMR server. */
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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const HMR_PATH = "/__vertebra/hmr";
const RECONNECT_MAX_ATTEMPTS = 20;
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;
const PING_TIMEOUT_MS = 45_000;

// ---------------------------------------------------------------------------
// Error Overlay
// ---------------------------------------------------------------------------

let overlayElement: HTMLDivElement | null = null;

function showErrorOverlay(error: Error, filePath: string): void {
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

/**
 * HMR WebSocket client.
 *
 * Connects to `/__vertebra/hmr`, processes server messages, and applies
 * CSS hot-swap or full page reload as appropriate.
 */
export class VertebraHmrClient {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setTimeout> | null = null;

  /** Open the WebSocket connection. */
  connect(): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      return;
    }

    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${protocol}//${location.host}${HMR_PATH}`;

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

  // -- handlers ------------------------------------------------------------

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
        this.handleUpdate(msg.updates, msg.timestamp);
        break;

      case "full-reload":
        console.log(`[vertebra] full reload: ${msg.reason}`);
        location.reload();
        break;

      case "ping":
        this.ws?.send(JSON.stringify({ type: "pong" }));
        break;

      case "types-update":
        console.log(`[vertebra] types updated: ${msg.version}`);
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
    // onclose will fire next — reconnect handled there.
  }

  // -- update logic --------------------------------------------------------

  private handleUpdate(updates: Array<{ path: string; timestamp: number }>, _batchTimestamp: number): void {
    for (const { path } of updates) {
      try {
        if (path.endsWith(".css")) {
          this.hotSwapCss(path);
        } else {
          // JS/TS changes — full page reload for now.
          // Module-level HMR (import.meta.hot) is future work.
          console.log(`[vertebra] file changed: ${path}`);
          location.reload();
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
    // No matching <link> found — full reload as fallback.
    location.reload();
  }

  // -- ping timeout --------------------------------------------------------

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

  // -- reconnection --------------------------------------------------------

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= RECONNECT_MAX_ATTEMPTS) {
      console.error("[vertebra] HMR max reconnect attempts reached");
      return;
    }

    const delay = Math.min(RECONNECT_BASE_MS * 2 ** this.reconnectAttempts + Math.random() * 1000, RECONNECT_MAX_MS);

    this.reconnectAttempts++;
    console.log(`[vertebra] HMR reconnecting in ${Math.round(delay)}ms (attempt ${this.reconnectAttempts})`);

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }
}

// ---------------------------------------------------------------------------
// Window globals
// ---------------------------------------------------------------------------

declare global {
  interface Window {
    __vertebra_hmr: VertebraHmrClient;
  }
}

// ---------------------------------------------------------------------------
// Auto-connect on import
// ---------------------------------------------------------------------------

const client = new VertebraHmrClient();
client.connect();

window.__vertebra_hmr = client;
