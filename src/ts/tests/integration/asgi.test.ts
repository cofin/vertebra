import { afterAll, beforeAll, describe, expect, it, mock, type Mock } from "bun:test";
import { unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Subprocess } from "bun";
import { IpcClient } from "../../src/ipc/client";
import { AsgiBridge, type AsgiIpcFrame } from "../../src/ipc/asgi";
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
const HEALTH_PATH = "/__vertebra__/health";

const test = SHM_AVAILABLE ? it : it.skip;

const workers: Subprocess[] = [];
let testFilePath: string | null = null;

function buildPythonPath(): string {
  const root = join(process.cwd(), "src", "py");
  const existing = process.env.PYTHONPATH;
  const delim = pathDelimiter();
  return existing ? `${root}${delim}${existing}` : root;
}

async function readStatus(client: IpcClient): Promise<number> {
  let status: number | null = null;
  const frames = (async function* () {
    yield {
      type: "http.request.start",
      method: "GET",
      path: HEALTH_PATH,
      query: "",
      headers: [],
      scheme: "http",
      httpVersion: "1.1",
    };
    yield { type: "http.request", body: new Uint8Array(), more_body: false };
  })();

  for await (const frame of client.requestStream<AsgiIpcFrame>(frames)) {
    if (frame.type === "http.response.start") {
      status = frame.status;
    }
    if (frame.type === "http.response.body" && !frame.more_body) {
      break;
    }
    if (frame.type === "http.response.pathsend" || frame.type === "http.response.zerocopysend") {
      break;
    }
  }
  return status ?? 502;
}

async function waitForWorker(workerId: number): Promise<void> {
  const config = {
    name: ASGI_NAME,
    slotSize: SLOT_SIZE,
    slots: SLOTS,
    timeoutMs: 500,
    retries: 0,
    retryDelayMs: 50,
  };
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const client = new IpcClient(config, workerId);
    try {
      const status = await readStatus(client);
      if (status === 200) {
        return;
      }
    } catch {
      // Retry until worker control ring is online.
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

  for (let i = 0; i < WORKERS; i += 1) {
    await waitForWorker(i);
  }
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
    const bridge = new AsgiBridge({ requester: pool });
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

    const bridge = new AsgiBridge({ requester: pool, router });
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
