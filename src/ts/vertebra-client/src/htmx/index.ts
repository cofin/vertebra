import { VertebraClient } from "../client/client";
import type { VertebraConfig } from "../client/types";

/**
 * Initialize HTMX 2 integration for Vertebra.
 *
 * Auto-intercepts HTMX requests to inject Vertebra headers
 * and handle JSON bodies for mutations.
 */
export function initVertebraHtmx(config: VertebraConfig) {
  const client = new VertebraClient(config);

  if (typeof document === "undefined") return;

  document.addEventListener("htmx:configRequest", (event: any) => {
    const { headers, method } = event.detail;

    // Inject base headers
    if (config.headers) {
      Object.assign(headers, config.headers);
    }

    // If it's a mutation, ensure JSON content type
    if (method !== "GET") {
      headers["Content-Type"] = "application/json";
    }
  });

  // Handle response errors
  document.addEventListener("htmx:responseError", (event: any) => {
    console.error("[vertebra:htmx] request failed:", event.detail);
  });

  return client;
}
