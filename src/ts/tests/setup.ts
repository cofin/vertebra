/**
 * Shared test utilities for TypeScript tests.
 */
import { existsSync } from "node:fs";
import { join } from "node:path";
import { loadShmLib } from "../src/broker/shm";

/**
 * Resolves the Python command to use for spawning workers.
 * Checks in order: VERTEBRA_PYTHON_BIN env var, uv, venv, system python.
 */
export function resolvePythonCommand(): string[] {
  if (process.env.VERTEBRA_PYTHON_BIN) {
    return [process.env.VERTEBRA_PYTHON_BIN];
  }
  const uvBin = Bun.which("uv");
  if (uvBin) {
    return [uvBin, "run", "python"];
  }
  const venvRoot = process.env.VIRTUAL_ENV ?? join(process.cwd(), ".venv");
  const venvPython = join(venvRoot, "bin", "python");
  if (existsSync(venvPython)) {
    return [venvPython];
  }
  const venvPython3 = join(venvRoot, "bin", "python3");
  if (existsSync(venvPython3)) {
    return [venvPython3];
  }
  return [Bun.which("python") ?? Bun.which("python3") ?? "python"];
}

/**
 * Simple sleep helper for async tests.
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Check if shared memory library is available.
 */
export const SHM_AVAILABLE = (() => {
  try {
    loadShmLib();
    return true;
  } catch {
    return false;
  }
})();

/**
 * Returns the platform-specific path delimiter.
 */
export function pathDelimiter(): string {
  return Bun.platform === "win32" ? ";" : ":";
}

/**
 * Concatenates an array of Uint8Array chunks into a single Uint8Array.
 */
export function concatChunks(chunks: Uint8Array[]): Uint8Array {
  if (chunks.length === 0) {
    return new Uint8Array();
  }
  const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
