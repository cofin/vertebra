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

  public callJson<TInput = unknown, TOutput = unknown>(
    instance: WebAssembly.Instance,
    exportName: string,
    payload: TInput,
  ): TOutput {
    const fn = instance.exports[exportName];
    if (typeof fn !== "function") {
      throw new Error(`Export ${exportName} is not a callable WebAssembly function`);
    }
    const jsonStr = JSON.stringify(payload);
    const encoder = new TextEncoder();
    const decoder = new TextDecoder();
    const allocFn = instance.exports.alloc ?? instance.exports.malloc;
    const memory = instance.exports.memory;
    if (typeof allocFn === "function" && memory instanceof WebAssembly.Memory) {
      const maxLen = jsonStr.length * 3;
      const ptr = Number(allocFn(maxLen));
      const targetView = new Uint8Array(memory.buffer, ptr, maxLen);
      const { written } = encoder.encodeInto(jsonStr, targetView);
      const packedResult = BigInt(fn(ptr, written));
      const outPtr = Number(packedResult >> 32n);
      const outLen = Number(packedResult & 0xffff_ffffn);
      const outSlice = new Uint8Array(memory.buffer, outPtr, outLen);
      return JSON.parse(decoder.decode(outSlice)) as TOutput;
    }
    const encoded = encoder.encode(jsonStr);
    const rawResult = fn(encoded);
    if (rawResult instanceof Uint8Array) {
      return JSON.parse(decoder.decode(rawResult)) as TOutput;
    }
    if (typeof rawResult === "string") {
      return JSON.parse(rawResult) as TOutput;
    }
    return rawResult as TOutput;
  }

  public callMsgpack(instance: WebAssembly.Instance, exportName: string, payloadBytes: Uint8Array): Uint8Array {
    const fn = instance.exports[exportName];
    if (typeof fn !== "function") {
      throw new Error(`Export ${exportName} is not a callable WebAssembly function`);
    }
    const allocFn = instance.exports.alloc ?? instance.exports.malloc;
    const memory = instance.exports.memory;
    if (typeof allocFn === "function" && memory instanceof WebAssembly.Memory) {
      const ptr = Number(allocFn(payloadBytes.byteLength));
      const targetView = new Uint8Array(memory.buffer, ptr, payloadBytes.byteLength);
      targetView.set(payloadBytes);
      const packedResult = BigInt(fn(ptr, payloadBytes.byteLength));
      const outPtr = Number(packedResult >> 32n);
      const outLen = Number(packedResult & 0xffff_ffffn);
      return new Uint8Array(memory.buffer, outPtr, outLen);
    }
    const res = fn(payloadBytes);
    if (res instanceof Uint8Array) {
      return res;
    }
    throw new TypeError(`Export ${exportName} did not return a Uint8Array`);
  }
}
