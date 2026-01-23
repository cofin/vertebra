import type { Server } from "bun";
import { join } from "node:path";
import type { AssetConfig } from "./config";

export type RequestHandler = (
  request: Request,
  server?: Server<unknown>,
) => Response | Promise<Response | undefined> | void;

export interface RouterConfig {
  staticDir?: string;
  apiHandler?: RequestHandler;
  assets?: AssetConfig;
  staticMounts?: Record<string, string>;
}

function normalizePath(pathname: string): string {
  if (pathname.startsWith("/")) {
    return pathname.slice(1);
  }
  return pathname;
}

export interface RouterInstance {
  handler: RequestHandler;
  updateMounts: (mounts: Record<string, string>) => void;
}

export function createRouter(config: RouterConfig): RequestHandler & { updateMounts: (mounts: Record<string, string>) => void } {
  const staticDir = config.staticDir;
  const apiHandler = config.apiHandler;
  const assets = config.assets;
  let dynamicMounts: Record<string, string> = config.staticMounts || {};

  // Load manifest if available
  let manifest: Record<string, any> | undefined;
  if (staticDir && assets?.manifest) {
    const manifestPath = join(staticDir, assets.manifest);
    try {
      const content = Bun.file(manifestPath);
      // Synchronous load is tricky with Bun.file... but we are in createRouter which is async-ish context?
      // createRouter is sync. We should probably load async inside the handler or load once and cache if we can make createRouter async?
      // For now, let's load lazily on first request or just assume staticDir serving covers hashed files.
      // But for index.html injection we need manifest.

      // If we are just serving files, staticDir check handles it.
      // The requirement "Update Broker to read manifest.json... to map entry points" usually means
      // serving the correct hashed file for "index.html" or similar?
      // Or resolving imports?

      // "Implement HMR proxying" -> If devUrl is set, proxy to it.
    } catch (e) {
      console.warn("Failed to load manifest", e);
    }
  }

  const handler: RequestHandler = async (request: Request, server?: Server<unknown>) => {
    const url = new URL(request.url);

    // 1. HMR / Dev Proxy
    if (assets?.devUrl) {
      // If request matches asset path or if it's not an API call, proxy to Vite
      // Simple heuristic: if not starting with /api (or handled by apiHandler), try proxy.

      // Actually, we usually want to proxy everything that isn't handled by our API.

      // If apiHandler returns response, use it. If undefined, proxy.
      if (apiHandler) {
        const response = await apiHandler(request, server);
        if (response !== undefined) return response;
      }

      // Proxy to dev server
      try {
        const targetUrl = new URL(url.pathname + url.search, assets.devUrl);
        const proxyReq = new Request(targetUrl, {
          method: request.method,
          headers: request.headers,
          body: request.body,
          // @ts-ignore
          duplex: "half",
        });

        // Handle WebSocket upgrade for HMR
        if (request.headers.get("upgrade") === "websocket") {
          // Bun specific proxying or let Bun.serve handle it?
          // If we return undefined, Bun might handle 404.
          // We can use `server.upgrade` if we implement manual proxying, but Vite HMR is WS.
          // For now, simple fetch proxy.
        }

        return fetch(proxyReq);
      } catch (e) {
        return new Response("Vite Proxy Error", { status: 502 });
      }
    }

    // 2. Dynamic Static Mounts
    // Check specific mounts first (e.g. /uploads)
    // dynamicMounts is { "/url/path": "/local/path" }
    for (const [urlPrefix, localDir] of Object.entries(dynamicMounts)) {
      if (url.pathname.startsWith(urlPrefix)) {
        const relativePath = url.pathname.slice(urlPrefix.length);
        const path = normalizePath(relativePath);
        // Prevent directory traversal
        if (path.includes("..")) continue;
        
        const file = Bun.file(join(localDir, path));
        if (await file.exists()) {
           return new Response(file, {
            headers: {
              "cache-control": "public, max-age=60",
            },
          });
        }
      }
    }

    // 3. Static Assets (Production default dir)
    if (staticDir && (request.method === "GET" || request.method === "HEAD")) {
      const path = normalizePath(url.pathname);
      const file = Bun.file(`${staticDir}/${path}`);
      if (await file.exists()) {
        return new Response(file, {
          headers: {
            "cache-control": "public, max-age=60",
          },
        });
      }

      // SPA Fallback to index.html if not found and not API?
      // Only if we are in "SPA mode".
    }

    if (apiHandler) {
      const response = await apiHandler(request, server);
      if (response !== undefined) {
        return response;
      }
      return;
    }
    return new Response("Not Found", { status: 404 });
  };

  const router = handler as RequestHandler & { updateMounts: (mounts: Record<string, string>) => void };
  router.updateMounts = (mounts: Record<string, string>) => {
    dynamicMounts = { ...dynamicMounts, ...mounts };
  };
  
  return router;
}
