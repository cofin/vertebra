/**
 * Type augmentations for Bun APIs and external modules.
 */

// Augment Bun namespace with missing APIs
declare module "bun" {
  /** Returns the number of available CPU cores for parallel processing. */
  export const availableParallelism: number;

  /** The current platform (e.g., "linux", "darwin", "win32"). */
  export const platform: string;

  /** Dynamically load a native library. */
  export function dlopen(
    path: string,
    symbols: Record<string, unknown>,
  ): { symbols: Record<string, unknown>; close(): void };

  /** SQL query bindings - extended to allow unknown arrays */
  export type SQLQueryBindings =
    | null
    | string
    | number
    | bigint
    | boolean
    | Uint8Array
    | Date
    | unknown[];
}

// Augment bun:ffi to accept bigint pointers (FFI returns bigint)
declare module "bun:ffi" {
  /** Convert FFI pointer to ArrayBuffer. Extended to accept bigint. */
  export function toArrayBuffer(
    ptr: number | bigint,
    byteOffset?: number,
    byteLength?: number,
  ): ArrayBuffer;
}

// Declare apache-arrow module with proper exports
declare module "apache-arrow" {
  // biome-ignore lint: any needed for arrow flexibility
  export class Table<T = any> {
    constructor(data?: unknown);
    static from<T = unknown>(data: unknown): Table<T>;
    toArray(): T[];
    get numRows(): number;
    get numCols(): number;
    get schema(): unknown;
    getChild(name: string): unknown;
    select(...names: string[]): Table<T>;
  }

  export class RecordBatchStreamWriter<T = unknown> {
    static writeAll<T>(table: Table<T>): Uint8Array;
  }

  // biome-ignore lint: any needed for arrow flexibility
  export function tableFromJSON(data: any[]): Table;
  export function tableFromIPC(data: Uint8Array | ArrayBuffer): Table;
  export function tableToIPC(table: Table): Uint8Array;
}
