import type { Server, ServerWebSocket, WebSocketHandler } from "bun";
import { ShmRouter } from "../broker/shm";
import { encodeMessage } from "../protocol/serialization";
import {
  type IpcClientConfig,
  IpcPool,
  type IpcRequester,
  resolveIpcConfig,
  type ResolvedIpcConfig,
} from "./client";

export type AsgiRequest = {
  id: string;
  type?: string;
  method: string;
  path: string;
  query: string;
  headers: [string, string][];
  body: Uint8Array;
  handler_id?: number;
  scheme?: string;
  httpVersion?: string;
  rawPath?: Uint8Array;
  queryString?: Uint8Array;
  rootPath?: string;
  server?: [string, number | null];
  client?: [string, number | null];
  extensions?: Record<string, Record<string, unknown>>;
  auth_claims?: Record<string, any>;
};

export type AsgiRequestStartFrame = {
  id: string;
  type: "http.request.start" | "direct.http";
  method: string;
  path: string;
  query: string;
  headers: [string, string][];
  scheme?: string;
  httpVersion?: string;
  rawPath?: Uint8Array;
  queryString?: Uint8Array;
  rootPath?: string;
  server?: [string, number | null];
  client?: [string, number | null];
  extensions?: Record<string, Record<string, unknown>>;
  path_params?: Record<string, string>;
  auth_claims?: Record<string, any>;
};

export type AsgiResponse = {
  id: string;
  status: number;
  headers: [string, string][];
  body?: Uint8Array;
  path?: string;
  offset?: number;
  count?: number;
};

export type AsgiIpcFrame =
  | {
      id: string;
      type: "http.response.start";
      status: number;
      headers: [string, string][];
    }
  | {
      id: string;
      type: "http.response.body";
      body?: Uint8Array;
      more_body?: boolean;
    }
  | {
      id: string;
      type: "http.response.pathsend";
      path?: string;
    }
  | {
      id: string;
      type: "http.response.zerocopysend";
      path?: string;
      offset?: number;
      count?: number;
    }
  | {
      id: string;
      type: "http.response.early_hint";
      headers: [string, string][];
    }
  | {
      id: string;
      type: "http.response.trailers";
      headers: [string, string][];
    }
  | {
      id: string;
      type: "http.request.start";
      method: string;
      path: string;
      query: string;
      headers: [string, string][];
      scheme?: string;
      httpVersion?: string;
      rawPath?: Uint8Array;
      queryString?: Uint8Array;
      rootPath?: string;
      server?: [string, number | null];
      client?: [string, number | null];
      extensions?: Record<string, Record<string, unknown>>;
      auth_claims?: Record<string, any>;
    }
  | {
      id: string;
      type: "direct.http";
      method: string;
      path: string;
      query: string;
      headers: [string, string][];
      scheme?: string;
      httpVersion?: string;
      rawPath?: Uint8Array;
      queryString?: Uint8Array;
      rootPath?: string;
      server?: [string, number | null];
      client?: [string, number | null];
      extensions?: Record<string, Record<string, unknown>>;
      handler_id?: number;
      path_params?: Record<string, string>;
      auth_claims?: Record<string, any>;
    }
  | {
      id: string;
      type: "http.request";
      body?: Uint8Array;
      more_body?: boolean;
    }
  | {
      id: string;
      type: "http.disconnect";
    }
  | {
      id: string;
      type: "websocket.accept";
      headers?: [string, string][];
    }
  | {
      id: string;
      type: "websocket.connect";
      path: string;
      query: string;
      headers: [string, string][];
      scheme?: string;
      httpVersion?: string;
      rawPath?: Uint8Array;
      queryString?: Uint8Array;
      rootPath?: string;
      server?: [string, number | null];
      client?: [string, number | null];
      subprotocols?: string[];
      extensions?: Record<string, Record<string, unknown>>;
    }
  | {
      id: string;
      type: "websocket.receive";
      bytes?: Uint8Array;
      text?: string;
    }
  | {
      id: string;
      type: "websocket.send";
      bytes?: Uint8Array;
      text?: string;
    }
  | {
      id: string;
      type: "websocket.close";
      code?: number;
      reason?: string;
    }
  | {
      id: string;
      type: "websocket.disconnect";
      code?: number;
    }
  | {
      id: string;
      type: "ws.open";
      handler_id: number;
      path: string;
      query: string;
      headers: [string, string][];
    }
  | {
      id: string;
      type: "ws.message";
      bytes?: Uint8Array;
      text?: string;
    }
  | {
      id: string;
      type: "ws.close";
      code?: number;
      reason?: string;
    };

export type AsgiBridgeOptions = {
  ipc?: IpcClientConfig;
  requester?: IpcRequester;
  router?: ShmRouter;
};

type WebSocketState = {
  id: string;
  connectFrame: AsgiIpcFrame;
  queue: AsyncFrameQueue<AsgiIpcFrame>;
  accepted: boolean;
  closed: boolean;
  handler_id?: number;
};

export class AsgiBridge {
  private requester: IpcRequester;
  private router?: ShmRouter;
  private ipcConfig: ResolvedIpcConfig;

  constructor(options: AsgiBridgeOptions = {}) {
    this.ipcConfig = resolveIpcConfig(options.ipc ?? {});
    this.requester = options.requester ?? new IpcPool(options.ipc);
    this.router = options.router;
  }

  async handle(request: Request, authClaims?: Record<string, any> | null): Promise<Response> {
    const url = new URL(request.url);
    const match = this.router?.matchPath(`/${request.method}${url.pathname}`);

    const baseMeta = this.buildRequestMeta(request, authClaims);
    const directMeta = match
      ? {
          ...baseMeta,
          type: "direct.http" as const,
          handler_id: match.id,
          path_params: match.params,
        }
      : null;

    const contentType = request.headers.get("content-type")?.toLowerCase() ?? "";
    const isMultipart = contentType.includes("multipart/form-data");
    const transferEncoding = request.headers.get("transfer-encoding")?.toLowerCase() ?? "";
    const isChunked = transferEncoding.includes("chunked");
    const lengthHeader = request.headers.get("content-length");
    const contentLength = lengthHeader ? Number.parseInt(lengthHeader, 10) : Number.NaN;
    const lengthKnown = Number.isFinite(contentLength);
    const hasBody = request.body !== null;

    if (!this.requester.requestStream && isMultipart) {
      return new Response("Unsupported Media Type", { status: 415 });
    }

    if (directMeta) {
      const maxBody = this.maxUnaryBodySize(directMeta);
      if (isMultipart) {
        if (this.requester.requestStream) {
          return this.handleStream(request, baseMeta);
        }
        return new Response("Unsupported Media Type", { status: 415 });
      }
      if (hasBody && (!lengthKnown || isChunked)) {
        if (this.requester.requestStream) {
          return this.handleStream(request, baseMeta);
        }
        return new Response("Payload Too Large", { status: 413 });
      }
      if (hasBody && lengthKnown && contentLength > maxBody) {
        if (this.requester.requestStream) {
          return this.handleStream(request, baseMeta);
        }
        return new Response("Payload Too Large", { status: 413 });
      }

      const body = new Uint8Array(await request.arrayBuffer());
      if (body.byteLength > maxBody) {
        return new Response("Payload Too Large", { status: 413 });
      }
      return this.handleUnary(this.buildRequest(directMeta, body));
    }

    if (this.requester.requestStream) {
      return this.handleStream(request, baseMeta);
    }

    const maxBody = this.maxUnaryBodySize(baseMeta);
    if (hasBody && (!lengthKnown || isChunked)) {
      return new Response("Payload Too Large", { status: 413 });
    }
    if (hasBody && lengthKnown && contentLength > maxBody) {
      return new Response("Payload Too Large", { status: 413 });
    }
    const body = new Uint8Array(await request.arrayBuffer());
    if (body.byteLength > maxBody) {
      return new Response("Payload Too Large", { status: 413 });
    }
    return this.handleUnary(this.buildRequest(baseMeta, body));
  }

  upgradeWebSocket(request: Request, server?: Server<WebSocketState>): boolean {
    if (!server) {
      return false;
    }
    if (!this.requester.requestStream) {
      return false;
    }
    const upgrade = request.headers.get("upgrade");
    if (!upgrade || upgrade.toLowerCase() !== "websocket") {
      return false;
    }

    // Check for native handler
    const url = new URL(request.url);
    const match = this.router?.matchPath(`/WEBSOCKET${url.pathname}`);

    const state = this.createWebSocketState(request);
    if (match) {
      state.handler_id = match.id;
    }

    const headers = this.selectWebSocketHeaders(state);
    return server.upgrade(request, { data: state, headers });
  }

  createWebSocketHandler(): WebSocketHandler<WebSocketState> {
    return {
      open: (ws) => {
        const state = ws.data as WebSocketState;
        void this.startWebSocketBridge(ws, state);
      },
      message: (ws, message) => {
        const state = ws.data as WebSocketState;
        if (state.closed) {
          return;
        }
        const frame: AsgiIpcFrame =
          typeof message === "string"
            ? { id: state.id, type: "websocket.receive", text: message }
            : {
                id: state.id,
                type: "websocket.receive",
                bytes:
                  message instanceof Uint8Array ? message : new Uint8Array(message as ArrayBuffer),
              };
        state.queue.push(frame);
      },
      close: (ws, code, reason) => {
        const state = ws.data as WebSocketState;
        if (!state.closed) {
          state.closed = true;
          state.queue.push({ id: state.id, type: "websocket.disconnect", code });
          state.queue.close();
        }
        void this.stopWebSocketBridge(ws, code, reason);
      },
    };
  }

  private async handleUnary(payload: AsgiRequest): Promise<Response> {
    try {
      const response = await this.requester.request<AsgiResponse>(payload);
      if (response.path) {
        return await this.fileResponse(
          response.path,
          response.status ?? 200,
          response.headers ?? [],
          response,
        );
      }
      return new Response((response.body ?? new Uint8Array()) as BodyInit, {
        status: response.status ?? 502,
        headers: response.headers ?? [],
      });
    } catch {
      return new Response("Bad Gateway", { status: 502 });
    }
  }

  private async handleStream(request: Request, meta: AsgiRequestStartFrame): Promise<Response> {
    const stream = this.createBackpressureStream();
    const responseInfo = this.createResponseInfoPromise();
    const frames = this.requester.requestStream?.<AsgiIpcFrame>(
      this.streamRequestFrames(request, meta),
    );

    if (!frames) {
      return new Response("Bad Gateway", { status: 502 });
    }

    void this.pumpFrames(frames, stream, responseInfo);

    try {
      const info = await responseInfo.promise;
      if (info.type === "file") {
        return await this.fileResponse(info.path, info.status, info.headers, info);
      }
      return new Response(stream.readable, {
        status: info.status,
        headers: info.headers,
      });
    } catch {
      return new Response("Bad Gateway", { status: 502 });
    }
  }

  private async pumpFrames(
    frames: AsyncIterable<AsgiIpcFrame>,
    stream: BackpressureStream,
    responseInfo: ResponseInfoController,
  ): Promise<void> {
    let startInfo: { status: number; headers: [string, string][] } | null = null;
    try {
      for await (const frame of frames) {
        switch (frame.type) {
          case "http.response.start": {
            startInfo = { status: frame.status ?? 200, headers: frame.headers ?? [] };
            break;
          }
          case "http.response.body": {
            if (!responseInfo.isResolved()) {
              responseInfo.resolveStream(startInfo?.status ?? 200, startInfo?.headers ?? []);
            }
            if (frame.body) {
              await stream.enqueue(frame.body);
            }
            if (!frame.more_body) {
              stream.close();
              return;
            }
            break;
          }
          case "http.response.pathsend": {
            responseInfo.resolveFile(
              frame.path ?? "",
              startInfo?.status ?? 200,
              startInfo?.headers ?? [],
            );
            stream.close();
            return;
          }
          case "http.response.zerocopysend": {
            responseInfo.resolveFile(
              frame.path ?? "",
              startInfo?.status ?? 200,
              startInfo?.headers ?? [],
              {
                offset: frame.offset,
                count: frame.count,
              },
            );
            stream.close();
            return;
          }
          case "http.response.early_hint":
          case "http.response.trailers": {
            // Not surfaced in Fetch Response yet; keep IPC semantics but ignore.
            break;
          }
        }
      }
      if (!responseInfo.isResolved()) {
        responseInfo.resolveStream(startInfo?.status ?? 204, startInfo?.headers ?? []);
      }
      stream.close();
    } catch (error) {
      stream.error(error);
      responseInfo.reject(error);
    }
  }

  private async fileResponse(
    path: string,
    status: number,
    headers: [string, string][],
    info?: { offset?: number; count?: number },
  ): Promise<Response> {
    if (!path) {
      return new Response("Not Found", { status: 404 });
    }
    const file = Bun.file(path);
    if (!(await file.exists())) {
      return new Response("Not Found", { status: 404 });
    }
    if (info?.offset !== undefined || info?.count !== undefined) {
      const start = info?.offset ?? 0;
      const end = info?.count !== undefined ? start + info.count : undefined;
      const sliced = end === undefined ? file.slice(start) : file.slice(start, end);
      return new Response(sliced, { status, headers });
    }
    return new Response(file, { status, headers });
  }

  private createBackpressureStream(): BackpressureStream {
    let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
    let pullResolve: (() => void) | null = null;
    let closed = false;
    const readable = new ReadableStream<Uint8Array>({
      start(ctrl) {
        controller = ctrl;
      },
      pull() {
        if (pullResolve) {
          const resolve = pullResolve;
          pullResolve = null;
          resolve();
        }
      },
      cancel() {
        closed = true;
        if (pullResolve) {
          const resolve = pullResolve;
          pullResolve = null;
          resolve();
        }
      },
    });

    const enqueue = async (chunk: Uint8Array): Promise<void> => {
      if (!controller || closed) {
        return;
      }
      while (controller.desiredSize !== null && controller.desiredSize <= 0 && !closed) {
        await new Promise<void>((resolve) => {
          pullResolve = resolve;
        });
      }
      if (closed) {
        return;
      }
      controller.enqueue(chunk);
    };

    const close = (): void => {
      if (!controller || closed) {
        return;
      }
      closed = true;
      controller.close();
    };

    const error = (err: unknown): void => {
      if (!controller || closed) {
        return;
      }
      closed = true;
      controller.error(err);
    };

    return { readable, enqueue, close, error };
  }

  private createResponseInfoPromise(): ResponseInfoController {
    let resolveFn: ((info: ResponseInfo) => void) | null = null;
    let rejectFn: ((error: unknown) => void) | null = null;
    const promise = new Promise<ResponseInfo>((resolve, reject) => {
      resolveFn = resolve;
      rejectFn = reject;
    });
    let resolved = false;
    const resolve = (info: ResponseInfo): void => {
      if (resolved || !resolveFn) {
        return;
      }
      resolved = true;
      resolveFn(info);
    };
    return {
      promise,
      resolveStream: (status, headers) => resolve({ type: "stream", status, headers }),
      resolveFile: (path, status, headers, extras) =>
        resolve({ type: "file", path, status, headers, ...extras }),
      reject: (error) => {
        if (resolved || !rejectFn) {
          return;
        }
        resolved = true;
        rejectFn(error);
      },
      isResolved: () => resolved,
    };
  }

  private maxUnaryBodySize(meta: AsgiRequestStartFrame): number {
    const payload = this.buildRequest(meta, new Uint8Array());
    const encoded = encodeMessage(payload);
    const overhead = encoded.byteLength;
    const margin = 32;
    return Math.max(0, this.ipcConfig.slotSize - overhead - margin);
  }

  private buildRequestMeta(
    request: Request,
    authClaims?: Record<string, any> | null,
  ): AsgiRequestStartFrame {
    const headers: [string, string][] = [];
    request.headers.forEach((value, key) => {
      headers.push([key, value]);
    });
    const url = new URL(request.url);
    const encoder = new TextEncoder();
    const queryString = url.search.replace(/^\?/, "");
    const port = url.port ? Number(url.port) : null;
    return {
      id: crypto.randomUUID(),
      type: "http.request.start",
      method: request.method,
      path: url.pathname,
      query: queryString,
      headers,
      scheme: url.protocol.replace(":", ""),
      httpVersion: "1.1",
      rawPath: encoder.encode(url.pathname),
      queryString: encoder.encode(queryString),
      rootPath: "",
      server: [url.hostname, Number.isFinite(port) ? port : null],
      client: undefined,
      extensions: {
        "http.response.pathsend": {},
        "http.response.zerocopysend": {},
        "http.response.early_hint": {},
        "http.response.trailers": {},
      },
      auth_claims: authClaims ?? undefined,
    };
  }

  private buildRequest(meta: AsgiRequestStartFrame, body: Uint8Array): AsgiRequest {
    const { type: _type, ...rest } = meta;
    return { ...rest, type: meta.type, body, auth_claims: meta.auth_claims };
  }

  private createWebSocketState(request: Request): WebSocketState {
    const headers: [string, string][] = [];
    request.headers.forEach((value, key) => {
      headers.push([key, value]);
    });
    const url = new URL(request.url);
    const encoder = new TextEncoder();
    const queryString = url.search.replace(/^\?/, "");
    const port = url.port ? Number(url.port) : null;
    const subprotocolHeader = request.headers.get("sec-websocket-protocol");
    const subprotocols = subprotocolHeader
      ? subprotocolHeader
          .split(",")
          .map((value) => value.trim())
          .filter((value) => value.length > 0)
      : [];
    const id = crypto.randomUUID();
    const connectFrame: AsgiIpcFrame = {
      id,
      type: "websocket.connect",
      path: url.pathname,
      query: queryString,
      headers,
      scheme: url.protocol.replace(":", ""),
      httpVersion: "1.1",
      rawPath: encoder.encode(url.pathname),
      queryString: encoder.encode(queryString),
      rootPath: "",
      server: [url.hostname, Number.isFinite(port) ? port : null],
      client: undefined,
      subprotocols,
      extensions: {
        "websocket.close": {},
      },
    };
    return {
      id,
      connectFrame,
      queue: new AsyncFrameQueue<AsgiIpcFrame>(),
      accepted: false,
      closed: false,
    };
  }

  private selectWebSocketHeaders(state: WebSocketState): HeadersInit | undefined {
    if (
      state.connectFrame.type !== "websocket.connect" ||
      !state.connectFrame.subprotocols?.length
    ) {
      return undefined;
    }
    return { "sec-websocket-protocol": state.connectFrame.subprotocols[0] };
  }

  private async startWebSocketBridge(
    ws: ServerWebSocket<WebSocketState>,
    state: WebSocketState,
  ): Promise<void> {
    const outbound = this.websocketOutbound(state);
    const stream = this.requester.requestStream?.<AsgiIpcFrame>(outbound);
    if (!stream) {
      ws.close(1011, "IPC stream unavailable");
      return;
    }
    try {
      for await (const frame of stream) {
        if (state.closed) {
          break;
        }
        await this.handleWebSocketFrame(ws, state, frame);
      }
    } catch {
      if (!state.closed) {
        ws.close(1011, "IPC stream error");
      }
    }
  }

  private async handleWebSocketFrame(
    ws: ServerWebSocket<WebSocketState>,
    state: WebSocketState,
    frame: AsgiIpcFrame,
  ): Promise<void> {
    switch (frame.type) {
      case "websocket.accept":
        state.accepted = true;
        break;
      case "websocket.send":
        if (!state.accepted) {
          return;
        }
        if (frame.text !== undefined) {
          ws.send(frame.text);
        } else if (frame.bytes) {
          ws.send(frame.bytes);
        }
        break;
      case "websocket.close":
        state.closed = true;
        ws.close(frame.code ?? 1000, frame.reason ?? "");
        break;
      case "websocket.disconnect":
        state.closed = true;
        ws.close(frame.code ?? 1000);
        break;
    }
  }

  private async stopWebSocketBridge(
    ws: ServerWebSocket<WebSocketState>,
    _code: number,
    _reason: string,
  ): Promise<void> {
    if (!ws.data) {
      return;
    }
  }

  private async *websocketOutbound(state: WebSocketState): AsyncGenerator<AsgiIpcFrame> {
    if (state.handler_id !== undefined) {
      // Native WebSocket - convert connectFrame to ws.open
      const {
        type: _type,
        extensions: _ext,
        scheme: _scheme,
        httpVersion: _ver,
        rawPath: _raw,
        queryString: _rawQuery,
        rootPath: _root,
        server: _srv,
        client: _cli,
        subprotocols: _sub,
        ...rest
      } = state.connectFrame as any;
      yield {
        ...rest,
        type: "ws.open",
        handler_id: state.handler_id,
      } as AsgiIpcFrame;
    } else {
      yield state.connectFrame;
    }

    for await (const frame of state.queue) {
      if (state.handler_id !== undefined) {
        // Map standard WS frames to native ones
        if (frame.type === "websocket.receive") {
          yield {
            id: frame.id,
            type: "ws.message",
            bytes: frame.bytes,
            text: frame.text,
          } as AsgiIpcFrame;
        } else if (frame.type === "websocket.disconnect") {
          yield {
            id: frame.id,
            type: "ws.close",
            code: frame.code,
          } as AsgiIpcFrame;
        } else {
          yield frame;
        }
      } else {
        yield frame;
      }
    }
  }

  private async *streamRequestFrames(
    request: Request,
    meta: AsgiRequestStartFrame,
  ): AsyncGenerator<AsgiIpcFrame> {
    let aborted = false;
    const onAbort = () => {
      aborted = true;
    };
    request.signal?.addEventListener("abort", onAbort, { once: true });
    try {
      yield meta;

      const body = request.body;
      if (!body) {
        yield { id: meta.id, type: "http.request", body: new Uint8Array(), more_body: false };
        if (aborted) {
          yield { id: meta.id, type: "http.disconnect" };
        }
        return;
      }

      const reader = body.getReader();
      try {
        while (true) {
          if (aborted) {
            yield { id: meta.id, type: "http.disconnect" };
            return;
          }
          const { value, done } = await reader.read();
          if (done) {
            yield { id: meta.id, type: "http.request", body: new Uint8Array(), more_body: false };
            return;
          }
          if (value) {
            yield { id: meta.id, type: "http.request", body: value, more_body: true };
          }
        }
      } finally {
        reader.releaseLock();
      }
    } finally {
      request.signal?.removeEventListener("abort", onAbort);
    }
  }
}

class AsyncFrameQueue<T> implements AsyncIterable<T> {
  private queue: T[] = [];
  private waiters: Array<(value: IteratorResult<T>) => void> = [];
  private closed = false;

  push(value: T): void {
    if (this.closed) {
      return;
    }
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter({ value, done: false });
      return;
    }
    this.queue.push(value);
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    for (const waiter of this.waiters) {
      waiter({ value: undefined as T, done: true });
    }
    this.waiters = [];
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: () => {
        if (this.queue.length > 0) {
          const value = this.queue.shift() as T;
          return Promise.resolve({ value, done: false });
        }
        if (this.closed) {
          return Promise.resolve({ value: undefined as T, done: true });
        }
        return new Promise<IteratorResult<T>>((resolve) => {
          this.waiters.push(resolve);
        });
      },
    };
  }
}

type BackpressureStream = {
  readable: ReadableStream<Uint8Array>;
  enqueue: (chunk: Uint8Array) => Promise<void>;
  close: () => void;
  error: (err: unknown) => void;
};

type ResponseInfo =
  | { type: "stream"; status: number; headers: [string, string][] }
  | {
      type: "file";
      path: string;
      status: number;
      headers: [string, string][];
      offset?: number;
      count?: number;
    };

type ResponseInfoController = {
  promise: Promise<ResponseInfo>;
  resolveStream: (status: number, headers: [string, string][]) => void;
  resolveFile: (
    path: string,
    status: number,
    headers: [string, string][],
    extras?: { offset?: number; count?: number },
  ) => void;
  reject: (error: unknown) => void;
  isResolved: () => boolean;
};
