import type { Plugin } from "vite";

export interface VertebraPluginOptions {
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
 */
export default function vertebraPlugin(options: VertebraPluginOptions = {}): Plugin {
  const { serverUrl = "http://localhost:8000", hmr = true, virtualModuleName = "virtual:vertebra-types" } = options;

  const resolvedVirtualModuleId = `\0${virtualModuleName}`;
  let lastVersion = "";

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
          const response = await fetch(`${serverUrl}/__vertebra/types.d.ts`);
          if (!response.ok) throw new Error(`Server returned ${response.status}`);
          return await response.text();
        } catch (err) {
          console.warn("[vertebra] Failed to fetch types, using stubs:", err);
          return "export namespace Procedures { /* stubs */ }";
        }
      }
      return null;
    },

    configureServer(server) {
      if (!hmr) return;

      // Type update polling
      const pollTypes = async () => {
        try {
          const response = await fetch(`${serverUrl}/__vertebra/types/version`);
          if (!response.ok) return;
          const version = await response.text();

          if (lastVersion && version !== lastVersion) {
            console.log("[vertebra] Types updated, triggering HMR");

            // Invalidate virtual module
            const mod = server.moduleGraph.getModuleById(resolvedVirtualModuleId);
            if (mod) {
              server.moduleGraph.invalidateModule(mod);
            }

            // Broadcast update to client
            server.ws.send({
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

      const interval = setInterval(pollTypes, 1000);
      server.httpServer?.once("close", () => clearInterval(interval));
    },

    handleHotUpdate({ file }) {
      // If we detect changes in python files, we could trigger type polling immediately
      if (file.endsWith(".py")) {
        // This logic is mostly handled by the Python watcher and the polling above,
        // but we could explicitly trigger a check here if we share the same project root.
      }
    },
  };
}
