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
  const pollIntervalMs = options.pollIntervalMs ?? 500;
  const moduleId = options.moduleId ?? DEFAULT_MODULE_ID;

  let cachedTypes = "// Vertebra types are not available yet\nexport {};\n";
  let cachedEtag = "";
  let server: ViteDevServer | null = null;
  let timer: ReturnType<typeof setInterval> | null = null;

  async function fetchTypes(): Promise<boolean> {
    const headers: Record<string, string> = {};
    if (cachedEtag) {
      headers["If-None-Match"] = cachedEtag;
    }
    const response = await fetch(endpoint, { headers });
    if (response.status === 304) {
      return false;
    }
    if (!response.ok) {
      return false;
    }
    const text = await response.text();
    const etag = response.headers.get("etag") ?? "";
    cachedTypes = text;
    cachedEtag = etag;
    return true;
  }

  function schedulePolling(): void {
    if (timer) {
      clearInterval(timer);
    }
    timer = setInterval(async () => {
      if (!server) {
        return;
      }
      try {
        const changed = await fetchTypes();
        if (changed) {
          const mod = server.moduleGraph.getModuleById(RESOLVED_ID);
          if (mod) {
            server.moduleGraph.invalidateModule(mod);
          }
          server.ws.send({ type: "full-reload" });
        }
      } catch {
        // Ignore polling errors; retry on next interval.
      }
    }, pollIntervalMs);
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
      await fetchTypes();
      schedulePolling();
    },
    closeBundle() {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
    },
  };
}
