export interface IpcBridgeConfig {
  name?: string;
  controlName?: string;
  slotSize?: number;
  slots?: number;
  timeoutMs?: number;
}

export class IpcBridge {
  constructor(private config: IpcBridgeConfig) {}

  async callTool(name: string, params: unknown): Promise<unknown> {
    throw new Error(`Shared-memory IPC bridge not configured for tool: ${name}`);
  }
}
