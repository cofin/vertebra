export interface IpcBridgeConfig {
  transport: "uds" | "tcp";
  path?: string;
  host?: string;
  port?: number;
}

export class IpcBridge {
  constructor(private config: IpcBridgeConfig) {}

  async callTool(name: string, params: unknown): Promise<unknown> {
    throw new Error(`IPC bridge not configured for tool: ${name}`);
  }
}
