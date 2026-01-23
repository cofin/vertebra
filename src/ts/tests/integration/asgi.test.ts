import { afterAll, beforeAll, describe, expect, it, mock, type Mock } from "bun:test";
import { unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { IpcClient } from "../../src/ipc/client";
import { AsgiBridge, type AsgiIpcFrame } from "../../src/ipc/asgi";
import { RoundRobinIpcConnectionPool, StubIpcConnectionPool } from "../../src/server/worker-connections";
import { ShmRouter } from "../../src/broker/shm";
import type { Server, ServerWebSocket } from "bun";
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

const test = SHM_AVAILABLE ? it : it.skip;

describe("ASGI IPC integration", () => {
  // ... (keep existing tests)

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
