import { copyFileSync, mkdirSync } from "node:fs";
import path from "node:path";
import { defineConfig } from "tsup";

const pythonBundleDir = path.resolve(__dirname, "../../py/vertebra/server/_js/bundle");

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/vite/index.ts",
    "src/react/index.ts",
    "src/vue/index.ts",
    "src/svelte/index.ts",
    "src/angular/index.ts",
    "src/htmx/index.ts",
    "src/store/index.ts",
    "src/hmr/index.ts",
    "src/live/index.ts",
    "src/apex/index.ts",
    "src/telemetry.ts",
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
    // Custom hook to sync the build to the Python package bundle folder
    // This allows distributing the library within the Python package.
    console.log("Build successful. Syncing to Python package bundle...");

    try {
      mkdirSync(pythonBundleDir, { recursive: true });

      // Copy the main bundle and hmr bundle
      const filesToSync = [
        ["dist/index.js", "index.js"],
        ["dist/hmr/index.js", "hmr.js"],
      ];

      for (const [src, dest] of filesToSync) {
        const srcPath = path.resolve(__dirname, src);
        const destPath = path.resolve(pythonBundleDir, dest);
        copyFileSync(srcPath, destPath);
        console.log(`Synced ${src} -> ${dest}`);
      }
    } catch (err) {
      console.error("Failed to sync bundles to Python package:", err);
    }
  },
});
