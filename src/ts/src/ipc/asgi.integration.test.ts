import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { unlink } from "node:fs/promises";
import { tmpdir } from "node:os";

import { AsgiBridge, type AsgiIpcFrame } from "./asgi";
import { IpcClient } from "./client";
import { loadShmLib } from "../broker/shm";
import { RoundRobinWorkerPool } from "../server/worker-pool";

const PYTHON_CMD = resolvePythonCommand();
const APP_PATH = "vertebra.server._test_app:app";
const ASGI_NAME = "vertebra-asgi-test";
const SLOT_SIZE = 4096;
const SLOTS = 64;
const WORKERS = 2;
const HEALTH_PATH = "/__vertebra__/health";

const procs: Subprocess[] = [];
let testFile = "";

function resolvePythonCommand(): string[] {
  if (process.env.VERTEBRA_PYTHON_BIN) {
    return [process.env.VERTEBRA_PYTHON_BIN];
  }
  const uvBin = Bun.which("uv");
  if (uvBin) {
    return [uvBin, "run", "python"];
  }
  const venvRoot = process.env.VIRTUAL_ENV ?? join(process.cwd(), ".venv");
  const venvPython = join(venvRoot, "bin", "python");
  if (existsSync(venvPython)) {
    return [venvPython];
  }
  const venvPython3 = join(venvRoot, "bin", "python3");
  if (existsSync(venvPython3)) {
    return [venvPython3];
  }
  return [Bun.which("python") ?? Bun.which("python3") ?? "python"];
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const SHM_AVAILABLE = (() => {
  try {
    loadShmLib();
    return true;
  } catch {
    return false;
  }
})();

async function waitForHealth(workerId: number, attempts = 50): Promise<void> {
  const client = new IpcClient({ name: ASGI_NAME, slotSize: SLOT_SIZE, slots: SLOTS }, workerId);
  const payload = {
    id: crypto.randomUUID(),
    method: "GET",
    path: HEALTH_PATH,
    query: "",
    headers: [],
    body: new Uint8Array(),
    scheme: "http",
    httpVersion: "1.1",
  };
  for (let i = 0; i < attempts; i += 1) {
    try {
      let status: number | null = null;
      const stream = client.requestStream ? client.requestStream<AsgiIpcFrame>(payload) : null;
      if (!stream) {
        throw new Error("IPC stream not available");
      }
      for await (const frame of stream) {
        if (frame.type === "http.response.start") {
          status = frame.status;
        }
        if (frame.type === "http.response.body" && !frame.more_body) {
          break;
        }
      }
      if (status === 200) {
        await client.close();
        return;
      }
    } catch {
      await sleep(50);
    }
  }
  await client.close();
  throw new Error(`Worker health check failed for worker ${workerId}`);
}

beforeAll(async () => {
  if (!SHM_AVAILABLE) {
    return;
  }
  testFile = join(tmpdir(), `vertebra-test-${Date.now()}.txt`);
  await Bun.write(testFile, "hello");
  const pythonPath = join(process.cwd(), "src", "py");
  for (let i = 0; i < WORKERS; i += 1) {
    const env = {
      ...(process.env as Record<string, string>),
      VERTEBRA_WORKER_ID: String(i),
      VERTEBRA_TEST_FILE: testFile,
      PYTHONPATH: process.env.PYTHONPATH
        ? `${pythonPath}${pathDelimiter()}${process.env.PYTHONPATH}`
        : pythonPath,
    };
    const proc = Bun.spawn({
      cmd: [
        ...PYTHON_CMD,
        "-c",
        "from vertebra.server.worker import main; main()",
        "--app",
        APP_PATH,
        "--name",
        ASGI_NAME,
        "--worker-id",
        String(i),
        "--slot-size",
        String(SLOT_SIZE),
        "--slots",
        String(SLOTS),
      ],
      env,
      stdout: "inherit",
      stderr: "inherit",
    });
    procs.push(proc);
  }
  await Promise.all([waitForHealth(0), waitForHealth(1)]);
});

afterAll(async () => {
  if (!SHM_AVAILABLE) {
    return;
  }
  for (const proc of procs) {
    try {
      proc.kill("SIGTERM");
    } catch {
      try {
        proc.kill();
      } catch {
        // ignore
      }
    }
  }
  if (testFile) {
    try {
      await unlink(testFile);
    } catch {
      // ignore
    }
  }
});

const test = SHM_AVAILABLE ? it : it.skip;

describe("ASGI IPC integration", () => {
  test("routes across multiple workers via round-robin pool", async () => {
    const pool = new RoundRobinWorkerPool({
      count: WORKERS,
      ipc: { name: ASGI_NAME, slotSize: SLOT_SIZE, slots: SLOTS },
    });
    const payload = {
      id: crypto.randomUUID(),
      method: "GET",
      path: "/",
      query: "",
      headers: [],
      body: new Uint8Array(),
      scheme: "http",
      httpVersion: "1.1",
    };
    const bodies: string[] = [];
    for (let i = 0; i < 4; i += 1) {
      const chunks: Uint8Array[] = [];
      const stream = pool.requestStream ? pool.requestStream<AsgiIpcFrame>(payload) : null;
      if (!stream) {
        throw new Error("IPC stream not available");
      }
      for await (const frame of stream) {
        if (frame.type === "http.response.body" && frame.body) {
          chunks.push(frame.body);
        }
        if (frame.type === "http.response.body" && !frame.more_body) {
          break;
        }
      }
      const data = concatChunks(chunks);
      bodies.push(new TextDecoder().decode(data));
    }
    await pool.close();
    const unique = new Set(bodies);
    expect(unique.size).toBeGreaterThan(1);
  });

  test("supports pathsend from ASGI worker", async () => {
    const pool = new RoundRobinWorkerPool({
      count: WORKERS,
      ipc: { name: ASGI_NAME, slotSize: SLOT_SIZE, slots: SLOTS },
    });
    const bridge = new AsgiBridge({ requester: pool });
    const response = await bridge.handle(new Request("http://127.0.0.1/file"));
    const text = await response.text();
    await pool.close();
    expect(text).toBe("hello");
  });
});

function pathDelimiter(): string {
  return Bun.platform === "win32" ? ";" : ":";
}

function concatChunks(chunks: Uint8Array[]): Uint8Array {
  if (chunks.length === 0) {
    return new Uint8Array();
  }
  const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
