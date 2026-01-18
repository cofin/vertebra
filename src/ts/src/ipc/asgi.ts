import { IpcPool, type IpcClientConfig, type IpcRequester } from "./client";

export type AsgiRequest = {
  id: string;
  method: string;
  path: string;
  query: string;
  headers: [string, string][];
  body: Uint8Array;
  scheme?: string;
  httpVersion?: string;
  rawPath?: Uint8Array;
  queryString?: Uint8Array;
  rootPath?: string;
  server?: [string, number | null];
  client?: [string, number | null];
  extensions?: Record<string, Record<string, unknown>>;
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
    };

export type AsgiBridgeOptions = {
  ipc?: IpcClientConfig;
  requester?: IpcRequester;
};

export class AsgiBridge {
  private requester: IpcRequester;

  constructor(options: AsgiBridgeOptions = {}) {
    this.requester = options.requester ?? new IpcPool(options.ipc);
  }

  async handle(request: Request): Promise<Response> {
    const body = new Uint8Array(await request.arrayBuffer());
    const headers: [string, string][] = [];
    request.headers.forEach((value, key) => {
      headers.push([key, value]);
    });
    const url = new URL(request.url);
    const encoder = new TextEncoder();
    const queryString = url.search.replace(/^\?/, "");
    const port = url.port ? Number(url.port) : null;
    const payload: AsgiRequest = {
      id: crypto.randomUUID(),
      method: request.method,
      path: url.pathname,
      query: queryString,
      headers,
      body,
      scheme: url.protocol.replace(":", ""),
      httpVersion: "1.1",
      rawPath: encoder.encode(url.pathname),
      queryString: encoder.encode(queryString),
      rootPath: "",
      server: [url.hostname, Number.isFinite(port) ? port : null],
      client: null,
      extensions: {
        "http.response.pathsend": {},
        "http.response.zerocopysend": {},
        "http.response.early_hint": {},
        "http.response.trailers": {},
      },
    };
    if (this.requester.requestStream) {
      return this.handleStream(payload);
    }
    return this.handleUnary(payload);
  }

  private async handleUnary(payload: AsgiRequest): Promise<Response> {
    try {
      const response = await this.requester.request<AsgiResponse>(payload);
      if (response.path) {
        return await this.fileResponse(response.path, response.status ?? 200, response.headers ?? [], response);
      }
      return new Response(response.body ?? new Uint8Array(), {
        status: response.status ?? 502,
        headers: response.headers ?? [],
      });
    } catch {
      return new Response("Bad Gateway", { status: 502 });
    }
  }

  private async handleStream(payload: AsgiRequest): Promise<Response> {
    const stream = this.createBackpressureStream();
    const responseInfo = this.createResponseInfoPromise();
    const frames = this.requester.requestStream?.<AsgiIpcFrame>(payload);

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
            responseInfo.resolveFile(frame.path ?? "", startInfo?.status ?? 200, startInfo?.headers ?? []);
            stream.close();
            return;
          }
          case "http.response.zerocopysend": {
            responseInfo.resolveFile(frame.path ?? "", startInfo?.status ?? 200, startInfo?.headers ?? [], {
              offset: frame.offset,
              count: frame.count,
            });
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
      resolveFile: (path, status, headers, extras) => resolve({ type: "file", path, status, headers, ...extras }),
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
}

type BackpressureStream = {
  readable: ReadableStream<Uint8Array>;
  enqueue: (chunk: Uint8Array) => Promise<void>;
  close: () => void;
  error: (err: unknown) => void;
};

type ResponseInfo =
  | { type: "stream"; status: number; headers: [string, string][] }
  | { type: "file"; path: string; status: number; headers: [string, string][]; offset?: number; count?: number };

type ResponseInfoController = {
  promise: Promise<ResponseInfo>;
  resolveStream: (status: number, headers: [string, string][]) => void;
  resolveFile: (path: string, status: number, headers: [string, string][], extras?: { offset?: number; count?: number }) => void;
  reject: (error: unknown) => void;
  isResolved: () => boolean;
};
