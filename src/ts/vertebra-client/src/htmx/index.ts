import type { ClientConfig } from "../client/types";

const FORM_ENCODINGS = ["multipart/form-data", "application/x-www-form-urlencoded"];

function isFormContentType(value: unknown): boolean {
  if (typeof value !== "string") {
    return false;
  }
  const lower = value.toLowerCase();
  return FORM_ENCODINGS.some((encoding) => lower.includes(encoding));
}

function hasFormEncoding(
  detail: Record<string, unknown>,
  headers: Record<string, string>,
  target: EventTarget | null,
): boolean {
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() === "content-type" && isFormContentType(value)) {
      return true;
    }
  }

  if (isFormContentType(detail.encoding)) {
    return true;
  }

  const elt = detail.elt ?? target;
  if (elt !== null && typeof elt === "object") {
    if ("getAttribute" in elt && typeof elt.getAttribute === "function") {
      const hxEncoding = (elt as Element).getAttribute("hx-encoding");
      const enctype = (elt as Element).getAttribute("enctype");
      if (isFormContentType(hxEncoding) || isFormContentType(enctype)) {
        return true;
      }
    } else if ("enctype" in elt && isFormContentType((elt as { enctype?: unknown }).enctype)) {
      return true;
    }

    if ("closest" in elt && typeof elt.closest === "function") {
      const ancestor = (elt as Element).closest("[hx-encoding], form[enctype]");
      if (ancestor) {
        const hxEncoding = ancestor.getAttribute("hx-encoding");
        const enctype = ancestor.getAttribute("enctype");
        if (isFormContentType(hxEncoding) || isFormContentType(enctype)) {
          return true;
        }
      }
    }
  }

  return false;
}

/**
 * Initialize HTMX 2 integration for Vertebra.
 *
 * Auto-intercepts HTMX requests to inject Vertebra headers
 * and handle JSON bodies for mutations.
 */
export function initHtmx(config: ClientConfig): void {
  if (typeof document === "undefined") return;

  document.addEventListener("htmx:configRequest", (event: Event) => {
    if (!("detail" in event) || typeof event.detail !== "object" || event.detail === null) {
      return;
    }
    const detail = event.detail as Record<string, unknown>;
    if (typeof detail.headers !== "object" || detail.headers === null) {
      return;
    }
    const headers = detail.headers as Record<string, string>;

    if (config.headers) {
      Object.assign(headers, config.headers);
    }

    const rawVerb =
      typeof detail.verb === "string" ? detail.verb : typeof detail.method === "string" ? detail.method : "";
    const verb = rawVerb.toUpperCase();
    if (verb !== "" && verb !== "GET" && !hasFormEncoding(detail, headers, event.target)) {
      headers["Content-Type"] = "application/json";
    }
  });

  document.addEventListener("htmx:responseError", (event: Event) => {
    if (!("detail" in event) || typeof event.detail !== "object" || event.detail === null) {
      return;
    }
    console.error("[vertebra:htmx] request failed:", event.detail);
  });
}
