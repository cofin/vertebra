export interface RouteDefinition {
  readonly method: string;
  readonly path: string;
}

export type RouteRegistry = Readonly<Record<string, RouteDefinition>>;

export interface RouteUrlOptions {
  baseUrl?: string;
  query?: Record<string, string | number | boolean | null | undefined>;
}

/**
 * Substitute `{param}`, `{param:type}`, or `:param` placeholders in `pathTemplate`
 * and append optional query parameters.
 */
export function interpolateRoutePath(
  pathTemplate: string,
  params?: Record<string, string | number | boolean>,
  query?: Record<string, string | number | boolean | null | undefined>,
): string {
  let resolved = pathTemplate;
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      const encoded = encodeURIComponent(String(value));
      resolved = resolved
        .replace(new RegExp(`\\{${key}(?::[^}]+)?\\}`, "g"), encoded)
        .replace(new RegExp(`:${key}(?=/|$)`, "g"), encoded);
    }
  }
  if (!resolved.startsWith("/")) {
    resolved = `/${resolved}`;
  }
  if (!query) {
    return resolved;
  }
  const pairs: string[] = [];
  for (const [k, v] of Object.entries(query)) {
    if (v === undefined || v === null) continue;
    pairs.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
  }
  return pairs.length > 0 ? `${resolved}?${pairs.join("&")}` : resolved;
}

/**
 * Build an HTTP URL from a route registry map, route key, path parameters, and options.
 */
export function routeUrl<R extends RouteRegistry, K extends keyof R & string>(
  routes: R,
  name: K,
  params?: Record<string, string | number | boolean>,
  options?: RouteUrlOptions,
): string {
  const entry = routes[name];
  if (!entry) {
    throw new Error(`Unknown route: ${name}`);
  }
  const path = interpolateRoutePath(entry.path, params, options?.query);
  if (!options?.baseUrl) {
    return path;
  }
  const cleanBase = options.baseUrl.replace(/\/+$/, "");
  return `${cleanBase}${path}`;
}

/**
 * Build a WebSocket (`ws:` or `wss:`) URL from a WebSocket route registry map,
 * route key, path parameters, and options.
 */
export function wsUrl<R extends RouteRegistry, K extends keyof R & string>(
  wsRoutes: R,
  name: K,
  params?: Record<string, string | number | boolean>,
  options?: RouteUrlOptions,
): string {
  const entry = wsRoutes[name];
  if (!entry) {
    throw new Error(`Unknown WebSocket route: ${name}`);
  }
  const path = interpolateRoutePath(entry.path, params, options?.query);
  const rawBase =
    options?.baseUrl ?? (typeof window !== "undefined" && window.location ? window.location.origin : "ws://localhost");
  const wsBase = rawBase
    .replace(/^http:/i, "ws:")
    .replace(/^https:/i, "wss:")
    .replace(/\/+$/, "");
  return `${wsBase}${path}`;
}
