import { cpSync, existsSync, mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const pkgRoot = path.resolve(scriptDir, "..");
const distDir = path.join(pkgRoot, "dist");
const pythonBundleDir = path.resolve(pkgRoot, "../../py/vertebra/server/_js/bundle");

const requiredArtifacts = ["index.js", "index.d.ts"];
for (const relPath of requiredArtifacts) {
  const artifactPath = path.join(distDir, relPath);
  if (!existsSync(artifactPath)) {
    console.error(`Missing built client artifact: ${artifactPath}`);
    process.exit(1);
  }
}

rmSync(pythonBundleDir, { recursive: true, force: true });
mkdirSync(pythonBundleDir, { recursive: true });
cpSync(distDir, pythonBundleDir, { recursive: true });

for (const relPath of requiredArtifacts) {
  const syncedPath = path.join(pythonBundleDir, relPath);
  if (!existsSync(syncedPath)) {
    console.error(`Missing synced Python bundle artifact: ${syncedPath}`);
    process.exit(1);
  }
}

console.log(`Synced ${distDir} -> ${pythonBundleDir}`);
