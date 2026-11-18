import { WebSocketClient } from "../websocket";
import { NetworkError, TimeoutError, throwParsedError } from "./errors";
import { computeBackoffDelay } from "./retry";
import type { ClientConfig, RouteResponse } from "./types";

class HttpClient {
  private config: Required<ClientConfig>;
  private ws: WebSocketClient | null = null;

  constructor(config: ClientConfig) {
    this.config = {
      baseURL: config.baseURL.replace(/\/$/, ""),
      headers: config.headers || {},
      timeout: config.timeout || 30000,
      wsURL: config.wsURL || `${config.baseURL.replace(/^http/, "ws")}/__vertebra/ws`,
      retry: {
        maxAttempts: config.retry?.maxAttempts ?? 3,
        initialDelay: config.retry?.initialDelay ?? 1000,
        retryableStatuses: config.retry?.retryableStatuses ?? [408, 429, 500, 502, 503, 504],
      },
    };
  }

  /**
   * Make a typed REST call.
   */
  async call<TInput = unknown, TOutput = unknown>(
    method: string,
    path: string,
    input?: TInput,
    options: { headers?: Record<string, string>; timeout?: number } = {},
  ): Promise<TOutput> {
    const response = await this.callWithResponse<TInput, TOutput>(method, path, input, options);
    return response.data;
  }

  /**
   * Make a typed REST call and return the full response.
   */
  async callWithResponse<TInput = unknown, TOutput = unknown>(
    method: string,
    path: string,
    input?: TInput,
    options: { headers?: Record<string, string>; timeout?: number } = {},
  ): Promise<RouteResponse<TOutput>> {
    const { url, bodyInput } = this.buildUrl(path, method, input);
    const reqHeaders = new Headers({
      ...this.config.headers,
      ...options.headers,
      "Content-Type": "application/json",
      Accept: "application/json",
    });

    const body = bodyInput !== undefined ? JSON.stringify(bodyInput) : undefined;
    const maxAttempts = this.config.retry.maxAttempts ?? 3;
    const initialDelay = this.config.retry.initialDelay ?? 1000;
    const retryableStatuses = this.config.retry.retryableStatuses ?? [408, 429, 500, 502, 503, 504];

    let lastError: Error | undefined;

    for (let attempt = 0; attempt <= maxAttempts; attempt++) {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), options.timeout || this.config.timeout);

      try {
        const response = await fetch(url, {
          method,
          headers: reqHeaders,
          body,
          signal: controller.signal,
        });

        if (!response.ok) {
          // Check if retryable and not final attempt
          if (attempt < maxAttempts && retryableStatuses.includes(response.status)) {
            const delay = parseRetryDelay(response, initialDelay, attempt);
            await sleep(delay);
            continue;
          }
          await throwParsedError(response);
        }

        let data: TOutput;
        const contentLength = response.headers.get("content-length");
        const contentType = response.headers.get("content-type");

        if (response.status === 204 || contentLength === "0") {
          data = undefined as TOutput;
        } else if (contentType?.includes("application/json")) {
          const text = await response.text();
          data = (text.length > 0 ? JSON.parse(text) : undefined) as TOutput;
        } else {
          data = (await response.text()) as TOutput;
        }

        const rawRequestId = response.headers.get("X-Request-ID");
        let requestId: string | number | null = null;
        if (rawRequestId !== null) {
          requestId = /^\d+$/.test(rawRequestId) ? Number(rawRequestId) : rawRequestId;
        }

        return {
          data,
          status: response.status,
          headers: response.headers,
          requestId,
        };
      } catch (error: unknown) {
        if ((error instanceof DOMException || error instanceof Error) && error.name === "AbortError") {
          lastError = new TimeoutError();
        } else if (error instanceof TypeError) {
          lastError = new NetworkError(error.message);
        } else {
          throw error;
        }

        // Network/timeout errors are retryable
        if (attempt < maxAttempts) {
          const delay = computeBackoffDelay(attempt, initialDelay);
          await sleep(delay);
          continue;
        }
        throw lastError;
      } finally {
        clearTimeout(timeoutId);
      }
    }

    throw lastError ?? new NetworkError("Request failed after retries");
  }

  /**
   * Subscribe to a WebSocket channel. Returns an unsubscribe function.
   * Lazy-creates the WebSocket connection on first subscription.
   */
  subscribe<T>(channel: string, callback: (data: T) => void): () => void {
    if (!this.ws) {
      this.ws = new WebSocketClient(this.config.wsURL);
    }
    return this.ws.subscribe<T>(channel, (data) => callback(data));
  }

  /**
   * Build a URL with interpolated path parameters and optional query string.
   */
  private buildUrl(path: string, method: string, input?: unknown): { url: string; bodyInput?: unknown } {
    let interpolatedPath = path;
    const queryParams = new URLSearchParams();
    const isGetOrHead = method === "GET" || method === "HEAD";

    if (input && typeof input === "object" && !Array.isArray(input)) {
      const consumedKeys = new Set<string>();
      for (const [key, value] of Object.entries(input)) {
        const placeholder = `{${key}}`;
        if (interpolatedPath.includes(placeholder)) {
          interpolatedPath = interpolatedPath.replaceAll(placeholder, encodeURIComponent(String(value)));
          consumedKeys.add(key);
        } else if (isGetOrHead && value !== undefined && value !== null) {
          queryParams.append(key, String(value));
        }
      }

      const fullPath = queryParams.size > 0 ? `${interpolatedPath}?${queryParams.toString()}` : interpolatedPath;
      const url = `${this.config.baseURL}${fullPath.startsWith("/") ? "" : "/"}${fullPath}`;

      if (isGetOrHead) {
        return { url, bodyInput: undefined };
      }

      const remaining: Record<string, unknown> = {};
      let remainingCount = 0;
      for (const [key, value] of Object.entries(input)) {
        if (!consumedKeys.has(key)) {
          remaining[key] = value;
          remainingCount++;
        }
      }
      return {
        url,
        bodyInput: remainingCount > 0 ? remaining : undefined,
      };
    }

    const fullPath = interpolatedPath;
    const url = `${this.config.baseURL}${fullPath.startsWith("/") ? "" : "/"}${fullPath}`;
    return { url, bodyInput: isGetOrHead ? undefined : input };
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Parse retry delay from response headers or compute exponential backoff. */
function parseRetryDelay(response: Response, initialDelay: number, attempt: number): number {
  const retryAfter = response.headers.get("Retry-After");
  if (retryAfter) {
    const seconds = Number(retryAfter);
    if (!Number.isNaN(seconds)) {
      return seconds * 1000;
    }
    // Try HTTP date format
    const date = Date.parse(retryAfter);
    if (!Number.isNaN(date)) {
      return Math.max(0, date - Date.now());
    }
  }
  return computeBackoffDelay(attempt, initialDelay);
}

export { HttpClient };
