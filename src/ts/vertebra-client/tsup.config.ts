import { cpSync, mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { defineConfig } from "tsup";

const distDir = path.resolve(__dirname, "dist");
const pythonBundleDir = path.resolve(__dirname, "../../py/vertebra/server/_js/bundle");

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/vite/index.ts",
    "src/react/index.tsx",
    "src/vue/index.ts",
    "src/svelte/index.ts",
    "src/angular/index.ts",
    "src/htmx/index.ts",
    "src/store/index.ts",
    "src/hmr/index.ts",
    "src/live/index.ts",
    "src/apex/index.ts",
    "src/telemetry.ts",
    "src/wasm.ts",
    "src/arrow.ts",
    "src/agent/index.ts",
  ],
  format: ["esm"],
  dts: true,
  clean: true,
  splitting: true,
  sourcemap: true,
  minify: false,
  treeshake: true,
  external: ["react", "vue", "svelte", "@angular/core", "vite"],
  async onSuccess() {
    // Sync the entire compiled dist directory (all subpaths and .d.ts files)
    // to the Python package bundle folder for native wheel distribution.
    console.log("Build successful. Syncing full dist to Python package bundle...");

    try {
      rmSync(pythonBundleDir, { recursive: true, force: true });
      mkdirSync(pythonBundleDir, { recursive: true });
      cpSync(distDir, pythonBundleDir, { recursive: true });
      console.log(`Synced ${distDir} -> ${pythonBundleDir}`);
    } catch (err) {
      console.error("Failed to sync bundles to Python package:", err);
    }
  },
});
