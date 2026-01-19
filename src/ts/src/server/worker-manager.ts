import { delimiter, join, resolve as resolvePath } from "node:path";

import { type AsgiIpcFrame, type AsgiRequest } from "../ipc/asgi";
import { IpcClient, resolveIpcConfig, type IpcClientConfig, type ResolvedIpcConfig } from "../ipc/client";
import type { PythonConfig, ServerIpcConfig } from "./config";

const HEALTH_PATH = "/__vertebra__/health";
const DEFAULT_HEALTH_INTERVAL_MS = 5000;
const DEFAULT_RESTART_BASE_DELAY_MS = 200;
const DEFAULT_RESTART_MAX_DELAY_MS = 5000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 30_000;

type WorkerProcess = {
  id: number;
  proc: Subprocess | null;
  restarts: number;
};

export type WorkerManagerOptions = {
  count: number;
  ipc: ServerIpcConfig;
  python: PythonConfig;
  healthIntervalMs?: number;
  restartBaseDelayMs?: number;
  restartMaxDelayMs?: number;
  shutdownTimeoutMs?: number;
};

export class WorkerManager {
  private workers = new Map<number, WorkerProcess>();
  private healthTimer: ReturnType<typeof setInterval> | null = null;
  private stopping = false;
  private restartTimers = new Map<number, ReturnType<typeof setTimeout>>();

  constructor(private options: WorkerManagerOptions) {
    if (!options.python.app) {
      throw new Error("Python app is required to manage workers");
    }
    for (let i = 0; i < Math.max(1, options.count); i += 1) {
      this.workers.set(i, { id: i, proc: null, restarts: 0 });
    }
  }

  start(): void {
    if (this.stopping) {
      return;
    }
    if (this.healthTimer) {
      return;
    }
    for (const worker of this.workers.values()) {
      this.spawnWorker(worker.id);
    }
    const interval = this.options.healthIntervalMs ?? DEFAULT_HEALTH_INTERVAL_MS;
    if (interval > 0) {
      this.healthTimer = setInterval(() => {
        void this.checkHealth();
      }, interval);
    }
  }

  async stop(): Promise<void> {
    this.stopping = true;
    if (this.healthTimer) {
      clearInterval(this.healthTimer);
      this.healthTimer = null;
    }
    for (const timer of this.restartTimers.values()) {
      clearTimeout(timer);
    }
    this.restartTimers.clear();

    await Promise.all(Array.from(this.workers.values()).map((worker) => this.stopWorker(worker)));
  }

  getStats(): { active: number; restarts: number } {
    let active = 0;
    let restarts = 0;
    for (const worker of this.workers.values()) {
      if (worker.proc) {
        active += 1;
      }
      restarts += worker.restarts;
    }
    return { active, restarts };
  }

  private spawnWorker(id: number): void {
    const worker = this.workers.get(id);
    if (!worker || worker.proc || this.stopping) {
      return;
    }
    const python = this.resolvePythonExecutable(this.options.python);
    const ipcConfig = this.resolveWorkerIpc(id);
    const cmd = [
      python,
      "-c",
      "from vertebra.server.worker import main; main()",
      "--app",
      this.options.python.app,
      "--name",
      ipcConfig.name,
      "--worker-id",
      String(id),
      "--slot-size",
      String(ipcConfig.slotSize),
      "--slots",
      String(ipcConfig.slots),
    ];
    if (this.options.python.timeout) {
      cmd.push("--timeout", String(this.options.python.timeout));
    }

    const env = this.buildWorkerEnv(id);
    const proc = Bun.spawn({
      cmd,
      env,
      cwd: process.cwd(),
      stdout: "inherit",
      stderr: "inherit",
    });
    worker.proc = proc;
    void proc.exited.then((code) => {
      this.handleExit(id, code);
    });
  }

  private async stopWorker(worker: WorkerProcess): Promise<void> {
    if (!worker.proc) {
      return;
    }
    const proc = worker.proc;
    worker.proc = null;
    try {
      proc.kill("SIGTERM");
    } catch {
      try {
        proc.kill();
      } catch {
        return;
      }
    }
    const timeout = setTimeout(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        return;
      }
    }, this.options.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS);

    try {
      await proc.exited;
    } finally {
      clearTimeout(timeout);
    }
  }

  private handleExit(id: number, _code: number): void {
    if (this.stopping) {
      return;
    }
    const worker = this.workers.get(id);
    if (!worker) {
      return;
    }
    worker.proc = null;
    worker.restarts += 1;
    const delay = this.computeRestartDelay(worker.restarts);
    const timer = setTimeout(() => {
      this.restartTimers.delete(id);
      this.spawnWorker(id);
    }, delay);
    this.restartTimers.set(id, timer);
  }

  private computeRestartDelay(attempt: number): number {
    const base = this.options.restartBaseDelayMs ?? DEFAULT_RESTART_BASE_DELAY_MS;
    const max = this.options.restartMaxDelayMs ?? DEFAULT_RESTART_MAX_DELAY_MS;
    return Math.min(max, base * 2 ** Math.max(0, attempt - 1));
  }

  private resolvePythonExecutable(python: PythonConfig): string {
    if (python.venv) {
      const base = resolvePath(process.cwd(), python.venv);
      const bin = Bun.platform === "win32" ? "Scripts/python.exe" : "bin/python";
      return join(base, bin);
    }
    return "python";
  }

  private resolveWorkerIpc(workerId: number): ResolvedIpcConfig {
    const { protocol: _protocol, ...ipc } = this.options.ipc;
    return resolveIpcConfig(ipc);
  }

  private buildWorkerEnv(workerId: number): Record<string, string> {
    const env: Record<string, string> = { ...(process.env as Record<string, string>) };
    env.VERTEBRA_WORKER_ID = String(workerId);
    const pythonPath = resolvePath(process.cwd(), "src", "py");
    if (env.PYTHONPATH) {
      env.PYTHONPATH = `${pythonPath}${delimiter}${env.PYTHONPATH}`;
    } else {
      env.PYTHONPATH = pythonPath;
    }
    return env;
  }

  private async checkHealth(): Promise<void> {
    for (const worker of this.workers.values()) {
      if (!worker.proc) {
        continue;
      }
      const ok = await this.pingWorker(worker.id);
      if (!ok) {
        await this.stopWorker(worker);
        this.spawnWorker(worker.id);
      }
    }
  }

  private async pingWorker(workerId: number): Promise<boolean> {
    const { protocol: _protocol, ...ipc } = this.options.ipc;
    const client = new IpcClient(ipc, workerId);
    const payload: AsgiRequest = {
      id: crypto.randomUUID(),
      method: "GET",
      path: HEALTH_PATH,
      query: "",
      headers: [],
      body: new Uint8Array(),
      scheme: "http",
      httpVersion: "1.1",
    };
    try {
      const status = await this.readStatus(client, payload);
      return status === 200;
    } catch {
      return false;
    } finally {
      await client.close();
    }
  }

  private async readStatus(client: IpcClient, payload: AsgiRequest): Promise<number> {
    let status: number | null = null;
    for await (const frame of client.requestStream<AsgiIpcFrame>(payload)) {
      if (frame.type === "http.response.start") {
        status = frame.status;
      }
      if (frame.type === "http.response.body" && !frame.more_body) {
        break;
      }
      if (frame.type === "http.response.pathsend" || frame.type === "http.response.zerocopysend") {
        status = status ?? 200;
        break;
      }
    }
    return status ?? 502;
  }
}
