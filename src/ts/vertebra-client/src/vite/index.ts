import type { Plugin, ViteDevServer } from "vite";

export interface VitePluginOptions {
  /**
   * Base URL of the Vertebra Python server.
   * @default "http://localhost:8000"
   */
  serverUrl?: string;
  /**
   * Enable HMR for type updates.
   * @default true
   */
  hmr?: boolean;
  /**
   * Virtual module name for types.
   * @default "virtual:vertebra-types"
   */
  virtualModuleName?: string;
}

/**
 * Vite plugin for Vertebra frontend integration.
 *
 * Provides:
 * 1. Virtual module resolution for types fetched from Python.
 * 2. Automatic polling for type updates in dev mode.
 * 3. HMR integration for type changes.
 * 4. Immediate type poll when .py files change.
 */
interface ManifestRouteEntry {
  method?: string;
  route?: string;
}

interface ManifestPayload {
  routes?: Record<string, ManifestRouteEntry>;
  websocketRoutes?: Record<string, ManifestRouteEntry>;
}

const EMPTY_MANIFEST_MODULE =
  "export const routes = Object.freeze({});\nexport const websocketRoutes = Object.freeze({});\n";

export function vitePlugin(options: VitePluginOptions = {}): Plugin {
  const { serverUrl = "http://localhost:8000", hmr = true, virtualModuleName = "virtual:vertebra-types" } = options;
  const cleanServerUrl = serverUrl.replace(/\/+$/, "");

  const resolvedVirtualModuleId = `\0${virtualModuleName}`;
  let lastVersion = "";
  let devServer: ViteDevServer | null = null;

  const pollTypes = async () => {
    if (!devServer) return;
    try {
      const response = await fetch(`${cleanServerUrl}/__vertebra/types/version`);
      if (!response.ok) return;
      const version = await response.text();

      if (lastVersion && version !== lastVersion) {
        console.log("[vertebra] Types updated, triggering HMR");

        // Invalidate virtual module
        const mod = devServer.moduleGraph.getModuleById(resolvedVirtualModuleId);
        if (mod) {
          devServer.moduleGraph.invalidateModule(mod);
        }

        // Broadcast update to client
        devServer.ws.send({
          type: "custom",
          event: "vertebra:types-update",
          data: { version },
        });
      }
      lastVersion = version;
    } catch {
      // Server might be down, ignore
    }
  };

  return {
    name: "vite-plugin-vertebra",

    resolveId(id) {
      if (id === virtualModuleName) {
        return resolvedVirtualModuleId;
      }
      return null;
    },

    async load(id) {
      if (id === resolvedVirtualModuleId) {
        try {
          const response = await fetch(`${cleanServerUrl}/__vertebra/manifest.json`);
          if (!response.ok) throw new Error(`Server returned ${response.status}`);
          const manifest = (await response.json()) as ManifestPayload;
          const routes: Record<string, { method: string; path: string }> = {};
          if (manifest.routes && typeof manifest.routes === "object") {
            for (const [name, entry] of Object.entries(manifest.routes)) {
              routes[name] = {
                method: typeof entry?.method === "string" ? entry.method : "GET",
                path: typeof entry?.route === "string" ? entry.route : "",
              };
            }
          }
          const websocketRoutes: Record<string, { method: string; path: string }> = {};
          if (manifest.websocketRoutes && typeof manifest.websocketRoutes === "object") {
            for (const [name, entry] of Object.entries(manifest.websocketRoutes)) {
              websocketRoutes[name] = {
                method: "WS",
                path: typeof entry?.route === "string" ? entry.route : "",
              };
            }
          }
          return `export const routes = Object.freeze(${JSON.stringify(routes)});\nexport const websocketRoutes = Object.freeze(${JSON.stringify(websocketRoutes)});\n`;
        } catch (err) {
          console.warn("[vertebra] Failed to fetch manifest, using empty stubs:", err);
          return EMPTY_MANIFEST_MODULE;
        }
      }
      return null;
    },

    configureServer(server) {
      devServer = server;
      if (!hmr) return;

      const interval = setInterval(pollTypes, 1000);
      server.httpServer?.once("close", () => clearInterval(interval));
    },

    handleHotUpdate({ file }) {
      if (file.endsWith(".py")) {
        // Python file changed — trigger immediate type poll instead of
        // waiting for the next 1s interval. The Python watcher will have
        // regenerated types by the time this fires.
        pollTypes();
      }
    },
  };
}

export default vitePlugin;
