import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { IpcClient } from "../../src/ipc/client";
import { AsgiBridge, type AsgiIpcFrame } from "../../src/ipc/asgi";
import { RoundRobinIpcConnectionPool } from "../../src/server/worker-connections";
import {
  concatChunks,
  pathDelimiter,
  resolvePythonCommand,
  SHM_AVAILABLE,
  sleep,
} from "../setup";

const PYTHON_CMD = resolvePythonCommand();
const APP_PATH = "tests._test_app:app";
const ASGI_NAME = "vertebra-asgi-test";
const SLOT_SIZE = 4096;
const SLOTS = 64;
const WORKERS = 2;
const HEALTH_PATH = "/__vertebra__/health";

const procs: Subprocess[] = [];
let testFile = "";

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
  const pool = new RoundRobinIpcConnectionPool({
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
    const pool = new RoundRobinIpcConnectionPool({
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
