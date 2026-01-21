import type { Plugin, ViteDevServer } from "vite";

export interface VertebraTypesPluginOptions {
  endpoint?: string;
  pollIntervalMs?: number;
  moduleId?: string;
}

const DEFAULT_ENDPOINT = "http://127.0.0.1:8000/__vertebra/types.d.ts";
const DEFAULT_MODULE_ID = "virtual:vertebra-types";
const RESOLVED_ID = "\0vertebra-types";

export function vertebraTypes(options: VertebraTypesPluginOptions = {}): Plugin {
  const endpoint = options.endpoint ?? DEFAULT_ENDPOINT;
  const pollIntervalMs = options.pollIntervalMs ?? 1000; // Used for error backoff
  const moduleId = options.moduleId ?? DEFAULT_MODULE_ID;

  let cachedTypes = "// Vertebra types are not available yet\nexport {};\n";
  let cachedEtag = "";
  let server: ViteDevServer | null = null;
  let isPolling = false;

  async function fetchTypes(): Promise<boolean> {
    const headers: Record<string, string> = {};
    if (cachedEtag) {
      headers["If-None-Match"] = cachedEtag;
      headers["X-Vertebra-Watch"] = "true";
    }
    const response = await fetch(endpoint, { headers });
    if (response.status === 304) {
      return false;
    }
    if (!response.ok) {
      throw new Error(`Failed to fetch types: ${response.statusText}`);
    }
    const text = await response.text();
    const etag = response.headers.get("etag") ?? "";
    cachedTypes = text;
    cachedEtag = etag;
    return true;
  }

  async function pollLoop(): Promise<void> {
    isPolling = true;
    while (isPolling) {
      try {
        if (!server) {
          await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
          continue;
        }
        
        const changed = await fetchTypes();
        if (changed && isPolling) {
          const mod = server.moduleGraph.getModuleById(RESOLVED_ID);
          if (mod) {
            server.moduleGraph.invalidateModule(mod);
          }
          server.ws.send({ type: "full-reload" });
        }
        // If not changed (304), fetchTypes returns false after waiting.
        // We loop immediately to reconnect.
      } catch {
        // Backoff on error
        if (isPolling) {
          await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
        }
      }
    }
  }

  return {
    name: "vertebra-types",
    apply: "serve",
    resolveId(id) {
      if (id === moduleId) {
        return RESOLVED_ID;
      }
      return null;
    },
    load(id) {
      if (id === RESOLVED_ID) {
        return cachedTypes;
      }
      return null;
    },
    async configureServer(devServer) {
      server = devServer;
      try {
        await fetchTypes();
      } catch {
        // Ignore initial fetch error
      }
      // Start polling loop without awaiting it
      pollLoop();
    },
    closeBundle() {
      isPolling = false;
    },
  };
}
