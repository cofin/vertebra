/**
 * Browser and Worker WebAssembly execution bridge (`@vertebra/client/wasm`).
 */

export const WASM_MAGIC_BYTES = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);

export interface WASMClientOptions {
  readonly maxMemoryPages?: number;
  readonly imports?: WebAssembly.Imports;
}

export class WASMClient {
  private readonly maxMemoryPages: number;
  private readonly baseImports: WebAssembly.Imports;

  public constructor(options: WASMClientOptions = {}) {
    this.maxMemoryPages = options.maxMemoryPages ?? 256;
    this.baseImports = options.imports ?? {};
  }

  public static isValidBinary(bytes: Uint8Array): boolean {
    if (bytes.byteLength < 8) {
      return false;
    }
    for (let i = 0; i < 8; i += 1) {
      if (bytes[i] !== WASM_MAGIC_BYTES[i]) {
        return false;
      }
    }
    return true;
  }

  public get memoryLimitBytes(): number {
    return this.maxMemoryPages * 65536;
  }

  public async instantiateBytes(bytes: Uint8Array): Promise<WebAssembly.WebAssemblyInstantiatedSource> {
    if (!WASMClient.isValidBinary(bytes)) {
      throw new Error("Invalid WebAssembly binary magic header");
    }
    const module = await WebAssembly.compile(bytes as unknown as BufferSource);
    const instance = await WebAssembly.instantiate(module, this.baseImports);

    for (const exp of Object.values(instance.exports)) {
      if (exp instanceof WebAssembly.Memory && exp.buffer.byteLength > this.memoryLimitBytes) {
        throw new RangeError("WebAssembly memory exceeds configured maxMemoryPages");
      }
    }
    for (const modImports of Object.values(this.baseImports)) {
      if (typeof modImports === "object" && modImports !== null) {
        for (const imp of Object.values(modImports)) {
          if (imp instanceof WebAssembly.Memory && imp.buffer.byteLength > this.memoryLimitBytes) {
            throw new RangeError("WebAssembly memory exceeds configured maxMemoryPages");
          }
        }
      }
    }

    return { module, instance };
  }
}
