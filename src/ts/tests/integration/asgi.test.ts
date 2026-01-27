import { afterAll, beforeAll, describe, expect, it, mock, setDefaultTimeout, type Mock } from "bun:test";
import { existsSync } from "node:fs";
import { unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Subprocess } from "bun";
import { AsgiBridge } from "../../src/ipc/asgi";
import { IpcClient } from "../../src/ipc/client";
import { RoundRobinIpcConnectionPool, StubIpcConnectionPool } from "../../src/server/worker-connections";
import { ShmRouter } from "../../src/broker/shm";
import type { Server } from "bun";
import { pathDelimiter, resolvePythonCommand, SHM_AVAILABLE, sleep } from "../setup";

const PYTHON_CMD = resolvePythonCommand();
const APP_PATH = "tests.integration.helpers._test_app:app";
const ASGI_NAME = `vertebra-asgi-test-${crypto.randomUUID().slice(0, 8)}`;
const SLOT_SIZE = 4096;
const SLOTS = 64;
const WORKERS = 2;

const test = SHM_AVAILABLE ? it : it.skip;

setDefaultTimeout(15000);

const workers: Subprocess[] = [];
let testFilePath: string | null = null;

function buildPythonPath(): string {
  const cwd = process.cwd();
  let root = join(cwd, "src", "py");
  // If running from src/ts, src/py is at ../../src/py
  if (!existsSync(root)) {
    root = join(cwd, "../../src/py");
  }
  const existing = process.env.PYTHONPATH;
  const delim = pathDelimiter();
  return existing ? `${root}${delim}${existing}` : root;
}

async function waitForWorker(workerId: number): Promise<void> {
  const config = {
    name: ASGI_NAME,
    slotSize: SLOT_SIZE,
    slots: SLOTS,
    timeoutMs: 500,
    retries: 0,
    retryDelayMs: 25,
  };
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const client = new IpcClient(config, workerId);
    try {
      await client.request({ type: "vertebra.ping" });
      return;
    } catch {
      // Retry until worker responds to ping.
    } finally {
      await client.close();
    }
    await sleep(50);
  }
  throw new Error(`ASGI worker ${workerId} did not become ready`);
}

beforeAll(async () => {
  if (!SHM_AVAILABLE) {
    return;
  }

  testFilePath = join(tmpdir(), `vertebra-asgi-test-${crypto.randomUUID()}.txt`);
  await Bun.write(testFilePath, "hello");

  const envBase = {
    ...(process.env as Record<string, string>),
    PYTHONPATH: buildPythonPath(),
    VERTEBRA_TEST_FILE: testFilePath,
  };

  for (let i = 0; i < WORKERS; i += 1) {
    const cmd = [
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
    ];
    const proc = Bun.spawn({
      cmd,
      env: { ...envBase, VERTEBRA_WORKER_ID: String(i) },
      cwd: process.cwd(),
      stdout: "inherit",
      stderr: "inherit",
    });
    workers.push(proc);
  }

  await Promise.all(Array.from({ length: WORKERS }, (_, i) => waitForWorker(i)));
});

afterAll(async () => {
  if (!SHM_AVAILABLE) {
    return;
  }

  for (const proc of workers) {
    try {
      proc.kill("SIGTERM");
    } catch {
      // Ignore already-exited processes.
    }
  }
  await Promise.all(
    workers.map(async (proc) => {
      try {
        await proc.exited;
      } catch {
        // Ignore teardown errors.
      }
    }),
  );
  workers.length = 0;

  if (testFilePath) {
    try {
      await unlink(testFilePath);
    } catch {
      // Ignore cleanup failures.
    }
  }
});

describe("ASGI IPC integration", () => {
  test("supports pathsend from ASGI worker", async () => {
    const pool = new RoundRobinIpcConnectionPool({
      count: WORKERS,
      ipc: { name: ASGI_NAME, slotSize: SLOT_SIZE, slots: SLOTS },
    });
    const bridge = new AsgiBridge({ requester: pool, ipc: { name: ASGI_NAME, slotSize: SLOT_SIZE, slots: SLOTS } });
    const response = await bridge.handle(new Request("http://127.0.0.1/file"));
    const text = await response.text();
    await pool.close();
    expect(text).toBe("hello");
  });

  test("native WebSocket upgrade uses direct opcodes", async () => {
    const pool = new StubIpcConnectionPool({
      ipc: { name: ASGI_NAME, slotSize: SLOT_SIZE, slots: SLOTS },
    });

    // Setup router with a native WS route
    const router = new ShmRouter();
    router.insert("/WEBSOCKET/chat", 123);

    const bridge = new AsgiBridge({
      requester: pool,
      router,
      ipc: { name: ASGI_NAME, slotSize: SLOT_SIZE, slots: SLOTS },
    });
    const server = { upgrade: mock(() => true) } as unknown as Server;

    const request = new Request("http://localhost/chat", {
      headers: { upgrade: "websocket" },
    });

    // We expect upgradeWebSocket to return true
    expect(bridge.upgradeWebSocket(request, server)).toBe(true);

    // Verify handler_id was passed to upgrade data
    const upgradeCall = (server.upgrade as Mock).mock.calls[0];
    const state = upgradeCall[1].data;

    expect(state.handler_id).toBe(123);

    router.close();
  });
});
