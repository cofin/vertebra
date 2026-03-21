import { NetworkError, TimeoutError, throwParsedError } from "./errors";
import type { RouteResponse, VertebraConfig } from "./types";

export class VertebraClient {
  private config: Required<VertebraConfig>;

  constructor(config: VertebraConfig) {
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
  async call<TInput = any, TOutput = any>(
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
  async callWithResponse<TInput = any, TOutput = any>(
    method: string,
    path: string,
    input?: TInput,
    options: { headers?: Record<string, string>; timeout?: number } = {},
  ): Promise<RouteResponse<TOutput>> {
    const url = this.buildUrl(path, method === "GET" ? input : undefined);
    const headers = new Headers({
      ...this.config.headers,
      ...options.headers,
      "Content-Type": "application/json",
      Accept: "application/json",
    });

    const body = method !== "GET" && method !== "HEAD" && input ? JSON.stringify(input) : undefined;

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), options.timeout || this.config.timeout);

    try {
      const response = await fetch(url, {
        method,
        headers,
        body,
        signal: controller.signal,
      });

      if (!response.ok) {
        await throwParsedError(response);
      }

      const data = await response.json();

      return {
        data,
        status: response.status,
        headers: response.headers,
        requestId: Number.parseInt(response.headers.get("X-Request-ID") || "0", 10),
      };
    } catch (error: any) {
      if (error.name === "AbortError") {
        throw new TimeoutError();
      }
      if (error instanceof TypeError) {
        throw new NetworkError(error.message);
      }
      throw error;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Build a URL with interpolated path parameters and optional query string.
   */
  private buildUrl(path: string, params?: any): string {
    let interpolatedPath = path;
    const queryParams = new URLSearchParams();

    if (params && typeof params === "object") {
      for (const [key, value] of Object.entries(params)) {
        const placeholder = `{${key}}`;
        if (interpolatedPath.includes(placeholder)) {
          interpolatedPath = interpolatedPath.replace(placeholder, String(value));
        } else if (value !== undefined && value !== null) {
          queryParams.append(key, String(value));
        }
      }
    }

    const queryString = queryParams.toString();
    const fullPath = queryString ? `${interpolatedPath}?${queryString}` : interpolatedPath;

    return `${this.config.baseURL}${fullPath.startsWith("/") ? "" : "/"}${fullPath}`;
  }
}
