import { Readable, Writable } from "node:stream";
import * as readline from "node:readline";

export interface Transport {
  start(): Promise<void>;
  close(): Promise<void>;
  send(message: any, sessionId?: string): Promise<void>;
  onmessage?: (message: any, sessionId?: string) => void;
  onerror?: (error: Error) => void;
}

export class StdioTransport implements Transport {
  private rl?: readline.Interface;

  constructor(
    private input: Readable,
    private output: Writable,
  ) {}

  async start(): Promise<void> {
    this.rl = readline.createInterface({
      input: this.input,
      output: undefined,
      terminal: false,
    });

    this.rl.on("line", (line) => {
      try {
        if (!line.trim()) return;
        const message = JSON.parse(line);
        this.onmessage?.(message);
      } catch (error) {
        const parseError = new Error("Parse error");
        (parseError as any).code = -32700;
        this.onerror?.(parseError);
      }
    });

    this.rl.on("error", (error) => {
      this.onerror?.(error);
    });
  }

  async close(): Promise<void> {
    this.rl?.close();
  }

  async send(message: any): Promise<void> {
    const json = JSON.stringify(message);
    this.output.write(json + "\n");
  }
}
