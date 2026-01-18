import { IpcClient, type IpcClientConfig } from "./client";

export type AsgiRequest = {
  id: string;
  method: string;
  path: string;
  query: string;
  headers: [string, string][];
  body: Uint8Array;
  scheme?: string;
  httpVersion?: string;
};

export type AsgiResponse = {
  id: string;
  status: number;
  headers: [string, string][];
  body: Uint8Array;
};

export class AsgiBridge {
  private client: IpcClient;

  constructor(config: IpcClientConfig) {
    this.client = new IpcClient(config);
  }

  async handle(request: Request): Promise<Response> {
    const body = new Uint8Array(await request.arrayBuffer());
    const headers: [string, string][] = [];
    request.headers.forEach((value, key) => {
      headers.push([key, value]);
    });
    const url = new URL(request.url);
    const payload: AsgiRequest = {
      id: crypto.randomUUID(),
      method: request.method,
      path: url.pathname,
      query: url.search.replace(/^\?/, ""),
      headers,
      body,
      scheme: url.protocol.replace(":", ""),
      httpVersion: "1.1",
    };
    const response = await this.client.request<AsgiResponse>(payload);
    return new Response(response.body, {
      status: response.status,
      headers: response.headers,
    });
  }
}
