import { readdirSync, existsSync } from "node:fs";
import { delimiter, join, resolve as resolvePath } from "node:path";
import type { Subprocess } from "bun";

import { ShmNotifier, ShmRing } from "../broker/shm";
import { type AsgiIpcFrame, type AsgiRequest } from "../ipc/asgi";
import {
  IpcClient,
  type IpcClientConfig,
  type ResolvedIpcConfig,
  resolveIpcConfig,
} from "../ipc/client";
import type { PythonConfig, ServerIpcConfig } from "./config";

import { registerWorkerNotifier, unregisterWorkerNotifier } from "./notifier-registry";

const HEALTH_PATH = "/__vertebra__/health";
const DEFAULT_HEALTH_INTERVAL_MS = 5000;
const DEFAULT_RESTART_BASE_DELAY_MS = 200;
const DEFAULT_RESTART_MAX_DELAY_MS = 5000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 30_000;

type WorkerProcess = {
  id: number;
  proc: Subprocess | null;
  notifier: ShmNotifier | null;
  restarts: number;
};

export type ProcessManagerOptions = {
  count: number;
  ipc: ServerIpcConfig;
  python: PythonConfig;
  healthIntervalMs?: number;
  restartBaseDelayMs?: number;
  restartMaxDelayMs?: number;
  shutdownTimeoutMs?: number;
};

export class ProcessManager {
  private workers = new Map<number, WorkerProcess>();
  private healthTimer: ReturnType<typeof setInterval> | null = null;
  private stopping = false;
  private restartTimers = new Map<number, ReturnType<typeof setTimeout>>();

  constructor(private options: ProcessManagerOptions) {
    if (!options.python.app) {
      throw new Error("Python app is required to manage workers");
    }
    for (let i = 0; i < Math.max(1, options.count); i += 1) {
      this.workers.set(i, { id: i, proc: null, notifier: null, restarts: 0 });
    }
  }

  /**
   * Clean up orphaned shared memory regions from previous runs.
   * Scans /dev/shm for regions matching our naming pattern.
   */
  static cleanupOrphanedShm(name: string): void {
    const shmDir = "/dev/shm";
    if (!existsSync(shmDir)) {
      return; // Not a Linux system or no /dev/shm
    }
    try {
      const files = readdirSync(shmDir);
      const prefix = name;
      for (const file of files) {
        if (file.startsWith(prefix)) {
          try {
            ShmRing.unlink(`/${file}`);
          } catch {
            // Ignore errors - may be in use or already cleaned up
          }
        }
      }
    } catch {
      // Ignore errors reading /dev/shm
    }
  }

  start(): void {
    if (this.stopping) {
      return;
    }
    if (this.healthTimer) {
      return;
    }
    // Clean up orphaned SHM from previous runs before spawning workers
    ProcessManager.cleanupOrphanedShm(this.options.ipc.name ?? "vertebra-asgi");
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

    // Create notifier BEFORE spawning - fd is inherited by child process
    const notifier = new ShmNotifier();
    worker.notifier = notifier;
    // Register in module-level map so IpcConnection can access it
    registerWorkerNotifier(id, notifier);

    const python = this.resolvePythonExecutable(this.options.python);
    const ipcConfig = this.resolveWorkerIpc(id);

    // Pass notifier FDs via stdio to ensure they are inherited (Bun/Node spawn closes others by default)
    // stdio: [stdin, stdout, stderr, notifyReadFd, notifyWriteFd?]
    const stdio: any[] = ["inherit", "inherit", "inherit", notifier.fd];
    let notifyReadFd = 3;
    let notifyWriteFd = 3;

    if (notifier.writeFd !== notifier.fd) {
      stdio.push(notifier.writeFd);
      notifyWriteFd = 4;
    }

    // Assertion safe: constructor validates python.app is defined
    const cmd: string[] = [
      python,
      "-c",
      "from vertebra.server.worker import main; main()",
      "--app",
      this.options.python.app!,
      "--name",
      ipcConfig.name,
      "--worker-id",
      String(id),
      "--slot-size",
      String(ipcConfig.slotSize),
      "--slots",
      String(ipcConfig.slots),
      // Pass mapped FD numbers
      "--notify-read-fd",
      String(notifyReadFd),
      "--notify-write-fd",
      String(notifyWriteFd),
    ];
    if (this.options.python.timeout) {
      cmd.push("--timeout", String(this.options.python.timeout));
    }

    const env = this.buildWorkerEnv(id);
    const proc = Bun.spawn({
      cmd,
      env,
      cwd: process.cwd(),
      stdio,
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

    // Close the notifier - this also signals the worker to stop waiting
    if (worker.notifier) {
      // Unregister from module-level map first
      unregisterWorkerNotifier(worker.id);
      try {
        worker.notifier.close();
      } catch {
        // Ignore errors closing notifier
      }
      worker.notifier = null;
    }

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

    // Close the notifier for the crashed worker
    if (worker.notifier) {
      // Unregister from module-level map first
      unregisterWorkerNotifier(id);
      try {
        worker.notifier.close();
      } catch {
        // Ignore errors closing notifier
      }
      worker.notifier = null;
    }

    // Clean up the crashed worker's SHM region to prevent /dev/shm leakage
    const ipcName = this.options.ipc.name ?? "vertebra-asgi";
    const controlRingName = `/${ipcName}.asgi.${id}.control`;
    try {
      ShmRing.unlink(controlRingName);
    } catch {
      // Ignore errors - SHM may already be cleaned up or never created
    }

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
    
    // Resolve src/py path. Try relative to cwd first, then relative to this file.
    let pythonPath = resolvePath(process.cwd(), "src", "py");
    if (!existsSync(pythonPath)) {
        // If we are in src/ts, try ../src/py
        const parentPath = resolvePath(process.cwd(), "..", "src", "py");
        if (existsSync(parentPath)) {
            pythonPath = parentPath;
        } else {
             // Fallback to relative to this file (src/ts/src/server/process-manager.ts -> src/py)
             // import.meta.dir is src/ts/src/server. Go up 3 levels to src/ts/src/server -> src/ts/src -> src/ts -> src -> py?
             // No, src/ts/src/server -> src/ts/src -> src/ts -> root -> src/py
             // root/src/ts/src/server/process-manager.ts
             // root/src/py
             // relative: ../../../py
             pythonPath = resolvePath(import.meta.dir, "..", "..", "..", "py");
        }
    }

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
    const requestId = crypto.randomUUID();
    const frames = (async function* () {
      yield {
        id: requestId,
        type: "http.request.start",
        method: "GET",
        path: HEALTH_PATH,
        query: "",
        headers: [],
        scheme: "http",
        httpVersion: "1.1",
      } as AsgiIpcFrame;
      yield {
        id: requestId,
        type: "http.request",
        body: new Uint8Array(),
        more_body: false,
      } as AsgiIpcFrame;
    })();
    try {
      const status = await this.readStatus(client, frames);
      return status === 200;
    } catch {
      return false;
    } finally {
      await client.close();
    }
  }

  private async readStatus(
    client: IpcClient,
    payload: AsyncIterable<AsgiIpcFrame>,
  ): Promise<number> {
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
