import { promises as fs } from "node:fs";
import { S3Client, type S3Options } from "bun";
import * as path from "path";

export type Backend = "local" | "s3" | "gcs";

export type FileEntry = {
  path: string;
  isDir: boolean;
  size?: number;
  modified?: string;
  etag?: string;
};

export type ByteRange = {
  offset: number;
  length?: number;
};

export type FileSystemParams = {
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

export type GcsOptions = {
  baseUrl?: string;
  apiBaseUrl?: string;
  accessToken?: string;
  headers?: Record<string, string>;
};

const s3Clients = new Map<string, S3Client>();

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

export function getS3Client(options?: Record<string, unknown>, bucket?: string): S3Client {
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

export async function handleLocalRead(filePath: string, range?: ByteRange): Promise<Uint8Array> {
  const file = Bun.file(filePath);
  const buffer = await sliceBlob(file, range).arrayBuffer();
  return new Uint8Array(buffer);
}

export async function handleLocalWrite(filePath: string, data: Uint8Array): Promise<number> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await Bun.write(filePath, data);
  return data.length;
}

export async function handleLocalWriteFile(filePath: string, localPath: string): Promise<number> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.copyFile(localPath, filePath);
  const stat = await fs.stat(filePath);
  return stat.size;
}

export async function handleLocalList(dirPath: string): Promise<FileEntry[]> {
  const entries = await fs.readdir(dirPath, { withFileTypes: true });
  return await Promise.all(
    entries.map(async (entry) => {
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
    }),
  );
}

export async function handleLocalStat(filePath: string): Promise<FileEntry> {
  const stat = await fs.stat(filePath);
  return {
    path: filePath,
    isDir: stat.isDirectory(),
    size: stat.size,
    modified: stat.mtime.toISOString(),
  };
}

export async function handleLocalExists(filePath: string): Promise<boolean> {
  return await Bun.file(filePath).exists();
}

export async function handleLocalDelete(filePath: string, recursive: boolean): Promise<void> {
  await fs.rm(filePath, { recursive, force: true });
}

export async function handleLocalMkdir(dirPath: string): Promise<void> {
  await fs.mkdir(dirPath, { recursive: true });
}

export async function handleLocalRmdir(dirPath: string, recursive: boolean): Promise<void> {
  await fs.rm(dirPath, { recursive, force: true });
}

export async function handleLocalReadFile(filePath: string, localPath: string): Promise<number> {
  await fs.mkdir(path.dirname(localPath), { recursive: true });
  await fs.copyFile(filePath, localPath);
  const stat = await fs.stat(localPath);
  return stat.size;
}

export async function handleS3Read(
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

export async function handleS3Write(
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

export async function handleS3WriteFile(
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

export async function handleS3List(
  bucket: string,
  prefix: string,
  options?: Record<string, unknown>,
): Promise<FileEntry[]> {
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

export async function handleS3Stat(
  bucket: string,
  key: string,
  options?: Record<string, unknown>,
): Promise<FileEntry> {
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

export async function handleS3Exists(
  bucket: string,
  key: string,
  options?: Record<string, unknown>,
): Promise<boolean> {
  const client = getS3Client(options, bucket);
  return await client.exists(key);
}

export async function handleS3Delete(
  bucket: string,
  key: string,
  options?: Record<string, unknown>,
): Promise<void> {
  const client = getS3Client(options, bucket);
  await client.delete(key);
}

export async function handleS3ReadFile(
  bucket: string,
  key: string,
  localPath: string,
  options?: Record<string, unknown>,
): Promise<number> {
  const client = getS3Client(options, bucket);
  const file = client.file(key);
  const stream = file.stream();
  await fs.mkdir(path.dirname(localPath), { recursive: true });
  // Convert ReadableStream to ArrayBuffer for Bun.write compatibility
  const buffer = await new Response(stream).arrayBuffer();
  await Bun.write(localPath, buffer);
  const stat = await fs.stat(localPath);
  return stat.size;
}

export async function handleGcsRead(
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

export async function handleGcsWrite(
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
    body: data as BodyInit,
  });
  if (!response.ok) {
    throw new Error(`GCS write failed: ${response.status}`);
  }
  return data.length;
}

export async function handleGcsWriteFile(
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

export async function handleGcsList(
  bucket: string,
  prefix: string,
  options?: Record<string, unknown>,
): Promise<FileEntry[]> {
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
  const items = (data.items ?? []) as Array<{
    name: string;
    size?: string;
    updated?: string;
    etag?: string;
  }>;
  return items.map((item) => ({
    path: `gs://${bucket}/${item.name}`,
    isDir: false,
    size: item.size ? Number(item.size) : undefined,
    modified: item.updated,
    etag: item.etag,
  }));
}

export async function handleGcsStat(
  bucket: string,
  key: string,
  options?: Record<string, unknown>,
): Promise<FileEntry> {
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

export async function handleGcsExists(
  bucket: string,
  key: string,
  options?: Record<string, unknown>,
): Promise<boolean> {
  try {
    await handleGcsStat(bucket, key, options);
    return true;
  } catch {
    return false;
  }
}

export async function handleGcsDelete(
  bucket: string,
  key: string,
  options?: Record<string, unknown>,
): Promise<void> {
  const gcs = gcsOptions(options);
  const apiBase = gcs.apiBaseUrl ?? "https://storage.googleapis.com/storage/v1";
  const url = `${apiBase}/b/${bucket}/o/${encodeKey(key)}`;
  const response = await fetch(url, { method: "DELETE", headers: gcsHeaders(gcs) });
  if (!response.ok && response.status !== 404) {
    throw new Error(`GCS delete failed: ${response.status}`);
  }
}

export async function handleGcsReadFile(
  bucket: string,
  key: string,
  localPath: string,
  options?: Record<string, unknown>,
): Promise<number> {
  const gcs = gcsOptions(options);
  const baseUrl = gcs.baseUrl ?? "https://storage.googleapis.com";
  const url = `${baseUrl}/${bucket}/${encodeKey(key)}`;
  const response = await fetch(url, { headers: gcsHeaders(gcs) });
  if (!response.ok) {
    throw new Error(`GCS read failed: ${response.status}`);
  }
  await fs.mkdir(path.dirname(localPath), { recursive: true });
  // Convert response to ArrayBuffer for Bun.write compatibility
  const buffer = await response.arrayBuffer();
  await Bun.write(localPath, buffer);
  const stat = await fs.stat(localPath);
  return stat.size;
}
