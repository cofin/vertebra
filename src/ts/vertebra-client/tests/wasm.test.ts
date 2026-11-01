import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { WASM_MAGIC_BYTES, WASMClient } from "../src/wasm";

describe("WASMClient", () => {
  it("validates canonical WebAssembly binary magic header", () => {
    expect(WASMClient.isValidBinary(WASM_MAGIC_BYTES)).toBe(true);
    expect(WASMClient.isValidBinary(new Uint8Array([1, 2, 3]))).toBe(false);
  });

  it("computes memory limit bytes and rejects invalid headers on instantiate", async () => {
    const client = new WASMClient({ maxMemoryPages: 64 });
    expect(client.memoryLimitBytes).toBe(64 * 65536);
    await expect(client.instantiateBytes(new Uint8Array([0, 0, 0, 0]))).rejects.toThrow(
      "Invalid WebAssembly binary magic header",
    );
  });

  it("instantiateBytes returns module and instance on valid binary", async () => {
    const client = new WASMClient();
    const result = await client.instantiateBytes(WASM_MAGIC_BYTES);
    expect(result.module).toBeInstanceOf(WebAssembly.Module);
    expect(result.instance).toBeInstanceOf(WebAssembly.Instance);
  });

  it("rejects imported memory exceeding memoryLimitBytes", async () => {
    const client = new WASMClient({
      maxMemoryPages: 2,
      imports: {
        env: {
          memory: new WebAssembly.Memory({ initial: 4 }),
        },
      },
    });
    await expect(client.instantiateBytes(WASM_MAGIC_BYTES)).rejects.toThrow(
      /WebAssembly memory exceeds configured maxMemoryPages/,
    );
  });

  it("ensures every subpath in package.json exports has matching dist artifacts", () => {
    const pkgPath = path.resolve(__dirname, "../package.json");
    const pkg = JSON.parse(readFileSync(pkgPath, "utf-8"));
    const exportsMap = pkg.exports as Record<string, { types: string; import: string }>;

    for (const [subpath, paths] of Object.entries(exportsMap)) {
      const jsPath = path.resolve(__dirname, "..", paths.import);
      const dtsPath = path.resolve(__dirname, "..", paths.types);
      expect(existsSync(jsPath), `Missing JS for export ${subpath}: ${jsPath}`).toBe(true);
      expect(existsSync(dtsPath), `Missing DTS for export ${subpath}: ${dtsPath}`).toBe(true);
    }
  });
});
