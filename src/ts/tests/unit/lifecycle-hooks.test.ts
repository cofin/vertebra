import { describe, expect, test, mock } from "bun:test";
import { BasePoolAdapter } from "../../src/broker/base-pool";

class TestPool extends BasePoolAdapter {
  name = "test";
  
  constructor(max: number, hooks: any) {
    super(max, hooks);
  }

  inUse() { return 0; }
  async acquire() {
    if (this.hooks.onAcquire) await this.hooks.onAcquire("conn");
    return "token";
  }
  async release(token: string) {
    if (this.hooks.onRelease) await this.hooks.onRelease("conn");
  }
  async execute(token: string, sql: string, params: any[]) { return {}; }
  async executeMany(token: string, sql: string, params: any[][]) { return {}; }
  async command(token: string, cmd: string, args: any[]) { return {}; }
  async begin(token: string) {}
  async commit(token: string) {}
  async rollback(token: string) {}
}

describe("Lifecycle Hooks", () => {
  test("calls hooks on acquire/release", async () => {
    const onAcquire = mock(async () => {});
    const onRelease = mock(async () => {});
    
    const pool = new TestPool(10, { onAcquire, onRelease });
    
    await pool.acquire();
    expect(onAcquire).toHaveBeenCalled();
    expect(onAcquire).toHaveBeenCalledWith("conn");
    
    await pool.release("token");
    expect(onRelease).toHaveBeenCalled();
    expect(onRelease).toHaveBeenCalledWith("conn");
  });
});
