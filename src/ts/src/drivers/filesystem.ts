import * as path from "path";
import { promises as fs } from "node:fs";
import { S3Client, type S3Options } from "bun";
import { decodeMessage, encodeMessage, frameMessage } from "../protocol/serialization";
import type { DriverRequest, DriverResponse } from "../protocol/messages";

type Backend = "local" | "s3" | "gcs";

export type FileSystemServerConfig = {
  hostname?: string;
  port?: number;
};

type FileEntry = {
  path: string;
  isDir: boolean;
  size?: number;
  modified?: string;
  etag?: string;
};

type ByteRange = {
  offset: number;
  length?: number;
};

type FileSystemParams = {
  backend: Backend;
  path?: string;
  bucket?: string;
  key?: string;
  prefix?: string;
  data?: Uint8Array;
  recursive?: boolean;
  options?: Record<string, unknown>;
  contentType?: string;
  metadata?: Record<string, string>;
  range?: ByteRange;
  localPath?: string;
  expiresIn?: number;
  method?: string;
  acl?: string;
};

type GcsOptions = {
  baseUrl?: string;
  apiBaseUrl?: string;
  accessToken?: string;
  headers?: Record<string, string>;
};

type SocketState = {
  buffer: Uint8Array;
};

const s3Clients = new Map<string, S3Client>();

function concatBuffers(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length + b.length);
  result.set(a, 0);
  result.set(b, a.length);
  return result;
}

function parseFrames(buffer: Uint8Array): { frames: Uint8Array[]; rest: Uint8Array } {
  const frames: Uint8Array[] = [];
  let offset = 0;
  while (buffer.length - offset >= 4) {
    const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 4);
    const size = view.getUint32(0, false);
    if (buffer.length - offset - 4 < size) {
      break;
    }
    const start = offset + 4;
    const end = start + size;
    frames.push(buffer.subarray(start, end));
    offset = end;
  }
  return { frames, rest: buffer.subarray(offset) };
}

function okResponse(request: DriverRequest, result: unknown): DriverResponse {
  return { id: request.id, ok: true, result };
}

function errorResponse(request: DriverRequest, message: string): DriverResponse {
  return { id: request.id, ok: false, error: { code: "filesystem", message } };
}

function clientKey(options?: Record<string, unknown>, bucket?: string): string {
  return JSON.stringify({ bucket: bucket ?? null, options: options ?? {} });
}

function resolveRange(range?: ByteRange): { start: number; end?: number } | null {
  if (!range) {
    return null;
  }
  const start = Math.max(0, Math.trunc(range.offset));
  if (range.length === undefined) {
    return { start };
  }
  const end = start + Math.max(0, Math.trunc(range.length));
  return { start, end };
}

function sliceBlob(blob: Blob, range?: ByteRange): Blob {
  const resolved = resolveRange(range);
  if (!resolved) {
    return blob;
  }
  return blob.slice(resolved.start, resolved.end);
}

function rangeHeader(range?: ByteRange): string | null {
  const resolved = resolveRange(range);
  if (!resolved) {
    return null;
  }
  if (resolved.end === undefined) {
    return `bytes=${resolved.start}-`;
  }
  return `bytes=${resolved.start}-${Math.max(resolved.end - 1, resolved.start)}`;
}

function getS3Client(options?: Record<string, unknown>, bucket?: string): S3Client {
  const key = clientKey(options, bucket);
  const existing = s3Clients.get(key);
  if (existing) {
    return existing;
  }
  const merged: S3Options = {
    ...(options as S3Options | undefined),
    ...(bucket ? { bucket } : {}),
  };
  const client = new S3Client(merged);
  s3Clients.set(key, client);
  return client;
}

function encodeKey(key: string): string {
  return key
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

function gcsHeaders(options?: GcsOptions): Headers {
  const headers = new Headers(options?.headers ?? {});
  if (options?.accessToken) {
    headers.set("Authorization", `Bearer ${options.accessToken}`);
  }
  return headers;
}

function gcsOptions(params?: Record<string, unknown>): GcsOptions {
  return (params ?? {}) as GcsOptions;
}

async function handleLocalRead(filePath: string, range?: ByteRange): Promise<Uint8Array> {
  const file = Bun.file(filePath);
  const buffer = await sliceBlob(file, range).arrayBuffer();
  return new Uint8Array(buffer);
}

async function handleLocalWrite(filePath: string, data: Uint8Array): Promise<number> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await Bun.write(filePath, data);
  return data.length;
}

async function handleLocalWriteFile(filePath: string, localPath: string): Promise<number> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.copyFile(localPath, filePath);
  const stat = await fs.stat(filePath);
  return stat.size;
}

async function handleLocalList(dirPath: string): Promise<FileEntry[]> {
  const entries = await fs.readdir(dirPath, { withFileTypes: true });
  return await Promise.all(entries.map(async (entry) => {
    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      return { path: fullPath, isDir: true };
    }
    const stat = await fs.stat(fullPath);
    return {
      path: fullPath,
      isDir: false,
      size: stat.size,
      modified: stat.mtime.toISOString(),
    };
  }));
}

async function handleLocalStat(filePath: string): Promise<FileEntry> {
  const stat = await fs.stat(filePath);
  return {
    path: filePath,
    isDir: stat.isDirectory(),
    size: stat.size,
    modified: stat.mtime.toISOString(),
  };
}

async function handleLocalExists(filePath: string): Promise<boolean> {
  return await Bun.file(filePath).exists();
}

async function handleLocalDelete(filePath: string, recursive: boolean): Promise<void> {
  await fs.rm(filePath, { recursive, force: true });
}

async function handleLocalMkdir(dirPath: string): Promise<void> {
  await fs.mkdir(dirPath, { recursive: true });
}

async function handleLocalRmdir(dirPath: string, recursive: boolean): Promise<void> {
  await fs.rm(dirPath, { recursive, force: true });
}

async function handleLocalReadFile(filePath: string, localPath: string): Promise<number> {
  await fs.mkdir(path.dirname(localPath), { recursive: true });
  await fs.copyFile(filePath, localPath);
  const stat = await fs.stat(localPath);
  return stat.size;
}

async function handleS3Read(
  bucket: string,
  key: string,
  options?: Record<string, unknown>,
  range?: ByteRange,
): Promise<Uint8Array> {
  const client = getS3Client(options, bucket);
  const file = client.file(key);
  const buffer = await sliceBlob(file, range).arrayBuffer();
  return new Uint8Array(buffer);
}

async function handleS3Write(
  bucket: string,
  key: string,
  data: Uint8Array,
  options?: Record<string, unknown>,
  contentType?: string,
  metadata?: Record<string, string>,
): Promise<number> {
  const client = getS3Client(options, bucket);
  const merged: S3Options = {
    ...(options as S3Options | undefined),
    ...(contentType ? { type: contentType } : {}),
    ...(metadata ? { metadata } : {}),
  };
  await client.write(key, data, merged);
  return data.length;
}

async function handleS3WriteFile(
  bucket: string,
  key: string,
  localPath: string,
  options?: Record<string, unknown>,
  contentType?: string,
  metadata?: Record<string, string>,
): Promise<number> {
  const client = getS3Client(options, bucket);
  const merged: S3Options = {
    ...(options as S3Options | undefined),
    ...(contentType ? { type: contentType } : {}),
    ...(metadata ? { metadata } : {}),
  };
  const file = Bun.file(localPath);
  await client.write(key, file, merged);
  const stat = await file.stat();
  return stat.size;
}

async function handleS3List(bucket: string, prefix: string, options?: Record<string, unknown>): Promise<FileEntry[]> {
  const client = getS3Client(options, bucket);
  const response = await client.list(prefix ? { prefix } : null);
  return (response.contents ?? []).map((item) => ({
    path: `s3://${bucket}/${item.key}`,
    isDir: false,
    size: item.size,
    modified: item.lastModified,
    etag: item.eTag,
  }));
}

async function handleS3Stat(bucket: string, key: string, options?: Record<string, unknown>): Promise<FileEntry> {
  const client = getS3Client(options, bucket);
  const stat = await client.stat(key);
  return {
    path: `s3://${bucket}/${key}`,
    isDir: false,
    size: stat.size,
    modified: stat.lastModified?.toString(),
    etag: stat.etag,
  };
}

async function handleS3Exists(bucket: string, key: string, options?: Record<string, unknown>): Promise<boolean> {
  const client = getS3Client(options, bucket);
  return await client.exists(key);
}

async function handleS3Delete(bucket: string, key: string, options?: Record<string, unknown>): Promise<void> {
  const client = getS3Client(options, bucket);
  await client.delete(key);
}

async function handleS3ReadFile(
  bucket: string,
  key: string,
  localPath: string,
  options?: Record<string, unknown>,
): Promise<number> {
  const client = getS3Client(options, bucket);
  const file = client.file(key);
  const stream = file.stream();
  await fs.mkdir(path.dirname(localPath), { recursive: true });
  await Bun.write(localPath, stream);
  const stat = await fs.stat(localPath);
  return stat.size;
}

async function handleGcsRead(
  bucket: string,
  key: string,
  options?: Record<string, unknown>,
  range?: ByteRange,
): Promise<Uint8Array> {
  const gcs = gcsOptions(options);
  const baseUrl = gcs.baseUrl ?? "https://storage.googleapis.com";
  const url = `${baseUrl}/${bucket}/${encodeKey(key)}`;
  const headers = gcsHeaders(gcs);
  const rangeValue = rangeHeader(range);
  if (rangeValue) {
    headers.set("Range", rangeValue);
  }
  const response = await fetch(url, { headers });
  if (!response.ok) {
    throw new Error(`GCS read failed: ${response.status}`);
  }
  const buffer = await response.arrayBuffer();
  return new Uint8Array(buffer);
}

async function handleGcsWrite(
  bucket: string,
  key: string,
  data: Uint8Array,
  options?: Record<string, unknown>,
  contentType?: string,
  metadata?: Record<string, string>,
): Promise<number> {
  const gcs = gcsOptions(options);
  const apiBase = gcs.apiBaseUrl ?? "https://storage.googleapis.com/storage/v1";
  const url = `${apiBase}/b/${bucket}/o?uploadType=media&name=${encodeKey(key)}`;
  const headers = gcsHeaders(gcs);
  if (contentType) {
    headers.set("Content-Type", contentType);
  }
  if (metadata) {
    headers.set("x-goog-meta-raw", JSON.stringify(metadata));
  }
  const response = await fetch(url, {
    method: "POST",
    headers,
    body: data,
  });
  if (!response.ok) {
    throw new Error(`GCS write failed: ${response.status}`);
  }
  return data.length;
}

async function handleGcsWriteFile(
  bucket: string,
  key: string,
  localPath: string,
  options?: Record<string, unknown>,
  contentType?: string,
  metadata?: Record<string, string>,
): Promise<number> {
  const gcs = gcsOptions(options);
  const apiBase = gcs.apiBaseUrl ?? "https://storage.googleapis.com/storage/v1";
  const url = `${apiBase}/b/${bucket}/o?uploadType=media&name=${encodeKey(key)}`;
  const headers = gcsHeaders(gcs);
  if (contentType) {
    headers.set("Content-Type", contentType);
  }
  if (metadata) {
    headers.set("x-goog-meta-raw", JSON.stringify(metadata));
  }
  const file = Bun.file(localPath);
  const response = await fetch(url, { method: "POST", headers, body: file });
  if (!response.ok) {
    throw new Error(`GCS write failed: ${response.status}`);
  }
  const stat = await file.stat();
  return stat.size;
}

async function handleGcsList(bucket: string, prefix: string, options?: Record<string, unknown>): Promise<FileEntry[]> {
  const gcs = gcsOptions(options);
  const apiBase = gcs.apiBaseUrl ?? "https://storage.googleapis.com/storage/v1";
  const url = new URL(`${apiBase}/b/${bucket}/o`);
  if (prefix) {
    url.searchParams.set("prefix", prefix);
  }
  const response = await fetch(url, { headers: gcsHeaders(gcs) });
  if (!response.ok) {
    throw new Error(`GCS list failed: ${response.status}`);
  }
  const data = await response.json();
  const items = (data.items ?? []) as Array<{ name: string; size?: string; updated?: string; etag?: string }>;
  return items.map((item) => ({
    path: `gs://${bucket}/${item.name}`,
    isDir: false,
    size: item.size ? Number(item.size) : undefined,
    modified: item.updated,
    etag: item.etag,
  }));
}

async function handleGcsStat(bucket: string, key: string, options?: Record<string, unknown>): Promise<FileEntry> {
  const gcs = gcsOptions(options);
  const apiBase = gcs.apiBaseUrl ?? "https://storage.googleapis.com/storage/v1";
  const url = `${apiBase}/b/${bucket}/o/${encodeKey(key)}`;
  const response = await fetch(url, { headers: gcsHeaders(gcs) });
  if (!response.ok) {
    throw new Error(`GCS stat failed: ${response.status}`);
  }
  const data = await response.json();
  return {
    path: `gs://${bucket}/${key}`,
    isDir: false,
    size: data.size ? Number(data.size) : undefined,
    modified: data.updated,
    etag: data.etag,
  };
}

async function handleGcsExists(bucket: string, key: string, options?: Record<string, unknown>): Promise<boolean> {
  try {
    await handleGcsStat(bucket, key, options);
    return true;
  } catch {
    return false;
  }
}

async function handleGcsDelete(bucket: string, key: string, options?: Record<string, unknown>): Promise<void> {
  const gcs = gcsOptions(options);
  const apiBase = gcs.apiBaseUrl ?? "https://storage.googleapis.com/storage/v1";
  const url = `${apiBase}/b/${bucket}/o/${encodeKey(key)}`;
  const response = await fetch(url, { method: "DELETE", headers: gcsHeaders(gcs) });
  if (!response.ok && response.status !== 404) {
    throw new Error(`GCS delete failed: ${response.status}`);
  }
}

async function handleGcsReadFile(
  bucket: string,
  key: string,
  localPath: string,
  options?: Record<string, unknown>,
): Promise<number> {
  const gcs = gcsOptions(options);
  const baseUrl = gcs.baseUrl ?? "https://storage.googleapis.com";
  const url = `${baseUrl}/${bucket}/${encodeKey(key)}`;
  const response = await fetch(url, { headers: gcsHeaders(gcs) });
  if (!response.ok || !response.body) {
    throw new Error(`GCS read failed: ${response.status}`);
  }
  await fs.mkdir(path.dirname(localPath), { recursive: true });
  await Bun.write(localPath, response.body);
  const stat = await fs.stat(localPath);
  return stat.size;
}

export class FileSystemDriverServer {
  private server: ReturnType<typeof Bun.listen> | null = null;

  constructor(private config: FileSystemServerConfig = {}) {}

  start(): void {
    if (this.server) {
      return;
    }
    const hostname = this.config.hostname ?? "127.0.0.1";
    const port = this.config.port ?? 7005;
    this.server = Bun.listen({
      hostname,
      port,
      socket: {
        open: (socket) => {
          socket.data = { buffer: new Uint8Array() } satisfies SocketState;
        },
        data: (socket, data) => {
          const state = socket.data as SocketState;
          state.buffer = concatBuffers(state.buffer, data);
          const parsed = parseFrames(state.buffer);
          state.buffer = parsed.rest;
          for (const frame of parsed.frames) {
            const request = decodeMessage<DriverRequest>(frame);
            void this.handleRequest(request).then((response) => {
              const payload = encodeMessage(response);
              socket.write(frameMessage(payload));
            });
          }
        },
        close: () => {
          return;
        },
      },
    });
  }

  stop(): void {
    this.server?.stop();
    this.server = null;
  }

  private async handleRequest(request: DriverRequest): Promise<DriverResponse> {
    try {
      switch (request.method) {
        case "read":
          return await this.handleRead(request);
        case "read_range":
          return await this.handleRead(request);
        case "write":
          return await this.handleWrite(request);
        case "write_file":
          return await this.handleWriteFile(request);
        case "list":
          return await this.handleList(request);
        case "stat":
          return await this.handleStat(request);
        case "exists":
          return await this.handleExists(request);
        case "delete":
          return await this.handleDelete(request);
        case "mkdir":
          return await this.handleMkdir(request);
        case "rmdir":
          return await this.handleRmdir(request);
        case "read_file":
          return await this.handleReadFile(request);
        case "signed_url":
          return await this.handleSignedUrl(request);
        default:
          return errorResponse(request, `Unknown method: ${request.method}`);
      }
    } catch (error) {
      return errorResponse(request, String(error));
    }
  }

  private async handleRead(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (params.backend === "local") {
      const data = await handleLocalRead(params.path ?? "", params.range);
      return okResponse(request, { data });
    }
    if (params.backend === "s3") {
      const data = await handleS3Read(params.bucket ?? "", params.key ?? "", params.options, params.range);
      return okResponse(request, { data });
    }
    const data = await handleGcsRead(params.bucket ?? "", params.key ?? "", params.options, params.range);
    return okResponse(request, { data });
  }

  private async handleWrite(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (!params.data) {
      return errorResponse(request, "Missing data for write");
    }
    if (params.backend === "local") {
      const bytes = await handleLocalWrite(params.path ?? "", params.data);
      return okResponse(request, { bytes });
    }
    if (params.backend === "s3") {
      const bytes = await handleS3Write(
        params.bucket ?? "",
        params.key ?? "",
        params.data,
        params.options,
        params.contentType,
        params.metadata,
      );
      return okResponse(request, { bytes });
    }
    const bytes = await handleGcsWrite(
      params.bucket ?? "",
      params.key ?? "",
      params.data,
      params.options,
      params.contentType,
      params.metadata,
    );
    return okResponse(request, { bytes });
  }

  private async handleWriteFile(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (!params.localPath) {
      return errorResponse(request, "Missing localPath for write_file");
    }
    if (params.backend === "local") {
      const bytes = await handleLocalWriteFile(params.path ?? "", params.localPath);
      return okResponse(request, { bytes });
    }
    if (params.backend === "s3") {
      const bytes = await handleS3WriteFile(
        params.bucket ?? "",
        params.key ?? "",
        params.localPath,
        params.options,
        params.contentType,
        params.metadata,
      );
      return okResponse(request, { bytes });
    }
    const bytes = await handleGcsWriteFile(
      params.bucket ?? "",
      params.key ?? "",
      params.localPath,
      params.options,
      params.contentType,
      params.metadata,
    );
    return okResponse(request, { bytes });
  }

  private async handleList(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (params.backend === "local") {
      const entries = await handleLocalList(params.path ?? "");
      return okResponse(request, { entries });
    }
    if (params.backend === "s3") {
      const entries = await handleS3List(params.bucket ?? "", params.prefix ?? params.key ?? "", params.options);
      return okResponse(request, { entries });
    }
    const entries = await handleGcsList(params.bucket ?? "", params.prefix ?? params.key ?? "", params.options);
    return okResponse(request, { entries });
  }

  private async handleStat(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (params.backend === "local") {
      const stat = await handleLocalStat(params.path ?? "");
      return okResponse(request, stat);
    }
    if (params.backend === "s3") {
      const stat = await handleS3Stat(params.bucket ?? "", params.key ?? "", params.options);
      return okResponse(request, stat);
    }
    const stat = await handleGcsStat(params.bucket ?? "", params.key ?? "", params.options);
    return okResponse(request, stat);
  }

  private async handleExists(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (params.backend === "local") {
      const exists = await handleLocalExists(params.path ?? "");
      return okResponse(request, { exists });
    }
    if (params.backend === "s3") {
      const exists = await handleS3Exists(params.bucket ?? "", params.key ?? "", params.options);
      return okResponse(request, { exists });
    }
    const exists = await handleGcsExists(params.bucket ?? "", params.key ?? "", params.options);
    return okResponse(request, { exists });
  }

  private async handleDelete(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (params.backend === "local") {
      await handleLocalDelete(params.path ?? "", params.recursive ?? false);
      return okResponse(request, { deleted: true });
    }
    if (params.backend === "s3") {
      await handleS3Delete(params.bucket ?? "", params.key ?? "", params.options);
      return okResponse(request, { deleted: true });
    }
    await handleGcsDelete(params.bucket ?? "", params.key ?? "", params.options);
    return okResponse(request, { deleted: true });
  }

  private async handleMkdir(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (params.backend !== "local") {
      return errorResponse(request, "mkdir is only supported for local backend");
    }
    await handleLocalMkdir(params.path ?? "");
    return okResponse(request, { created: true });
  }

  private async handleRmdir(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (params.backend !== "local") {
      return errorResponse(request, "rmdir is only supported for local backend");
    }
    await handleLocalRmdir(params.path ?? "", params.recursive ?? false);
    return okResponse(request, { removed: true });
  }

  private async handleReadFile(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (!params.localPath) {
      return errorResponse(request, "Missing localPath for read_file");
    }
    if (params.backend === "local") {
      const bytes = await handleLocalReadFile(params.path ?? "", params.localPath);
      return okResponse(request, { bytes });
    }
    if (params.backend === "s3") {
      const bytes = await handleS3ReadFile(
        params.bucket ?? "",
        params.key ?? "",
        params.localPath,
        params.options,
      );
      return okResponse(request, { bytes });
    }
    const bytes = await handleGcsReadFile(
      params.bucket ?? "",
      params.key ?? "",
      params.localPath,
      params.options,
    );
    return okResponse(request, { bytes });
  }

  private async handleSignedUrl(request: DriverRequest): Promise<DriverResponse> {
    const params = request.params as FileSystemParams;
    if (params.backend === "local") {
      return okResponse(request, { supported: false, url: null });
    }
    if (params.backend === "s3") {
      const client = getS3Client(params.options, params.bucket ?? "");
      const options: Record<string, unknown> = {};
      if (params.expiresIn) {
        options.expiresIn = params.expiresIn;
      }
      if (params.method) {
        options.method = params.method;
      }
      if (params.contentType) {
        options.type = params.contentType;
      }
      if (params.acl) {
        options.acl = params.acl;
      }
      const url = client.presign(params.key ?? "", options);
      return okResponse(request, { supported: true, url });
    }
    return okResponse(request, { supported: false, url: null });
  }
}
