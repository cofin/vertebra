import { describe, expect, it } from "vitest";
import { WASM_MAGIC_BYTES, WasmClient } from "../src/wasm";

describe("WasmClient", () => {
  it("validates canonical WebAssembly binary magic header", () => {
    expect(WasmClient.isValidBinary(WASM_MAGIC_BYTES)).toBe(true);
    expect(WasmClient.isValidBinary(new Uint8Array([1, 2, 3]))).toBe(false);
  });

  it("computes memory limit bytes and rejects invalid headers on instantiate", async () => {
    const client = new WasmClient({ maxMemoryPages: 64 });
    expect(client.memoryLimitBytes).toBe(64 * 65536);
    await expect(client.instantiateBytes(new Uint8Array([0, 0, 0, 0]))).rejects.toThrow(
      "Invalid WebAssembly binary magic header",
    );
  });
});
