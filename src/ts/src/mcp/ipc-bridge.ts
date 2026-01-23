import { encode, decode } from "@msgpack/msgpack";

export interface IPCTransport {
  send(payload: Uint8Array): Promise<Uint8Array>;
}

export class MCPIPCBridge {
  constructor(private transport: IPCTransport) {}

  async callTool(name: string, args: unknown): Promise<unknown> {
    const payload = encode({
      type: "tool_call",
      name,
      args,
    });

    const responseBytes = await this.transport.send(payload);
    const response = decode(responseBytes) as any;

    if (response.error) {
      throw new Error(response.error);
    }

    return response.result;
  }
}
