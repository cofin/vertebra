export { computeBackoffDelay } from "./retry";

interface ClientConfig {
  /** Base URL of the Vertebra Python server. */
  baseURL: string;
  /** Custom headers to include in every request. */
  headers?: Record<string, string>;
  /** Request timeout in milliseconds. Default: 30000. */
  timeout?: number;
  /** Retry configuration for failed requests. */
  retry?: RetryConfig;
  /** WebSocket URL for HMR updates. */
  wsURL?: string;
}

export type { ClientConfig };

export interface RetryConfig {
  /** Maximum number of retry attempts. Default: 3. */
  maxAttempts?: number;
  /** Initial delay in milliseconds for exponential backoff. Default: 1000. */
  initialDelay?: number;
  /** HTTP status codes that trigger a retry. Default: [408, 429, 500, 502, 503, 504]. */
  retryableStatuses?: number[];
}

export interface RouteResponse<T = any> {
  /** Response data. */
  data: T;
  /** HTTP status code. */
  status: number;
  /** Response headers. */
  headers: Headers;
  /** Unique request correlation ID. */
  requestId: number;
}
