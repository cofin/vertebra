import { describe, expect, it, mock } from "bun:test";
import { SqlPool } from "../../src/broker/serve";

describe("Lifecycle Hooks", () => {
  it("calls hooks on acquire/release", async () => {
    const pool = new SqlPool(10);
    const onAcquire = mock(async () => {});
    const onRelease = mock(async () => {});
    
    pool.setOnAcquire(onAcquire);
    pool.setOnRelease(onRelease);
    
    const token = await pool.acquire();
    expect(onAcquire).toHaveBeenCalled();
    
    await pool.release(token);
    expect(onRelease).toHaveBeenCalled();
  });

  it("calls onError hook", async () => {
    const pool = new SqlPool(10);
    const onError = mock(async () => {});
    pool.setOnError(onError);
    
    // Force an error in health check (simulating internal error)
    // We need to inject a bad connection or force a failure
    // Using internal access for test setup
    (pool as any).idle.add("bad_token");
    (pool as any).connections.set("bad_token", {});
    (pool as any).validateConnection = async () => { throw new Error("Validation boom"); };
    
    await (pool as any).runHealthCheck();
    
    expect(onError).toHaveBeenCalled();
  });

  it("calls onTimeout hook", async () => {
    const pool = new SqlPool(10);
    const onTimeout = mock(async () => {});
    pool.setOnTimeout(onTimeout);
    
    const token = "stale_token";
    (pool as any).idle.add(token);
    (pool as any).connectionTimestamps.set(token, Date.now() - 1000);
    
    await (pool as any).reapIdleConnections(100);
    
    expect(onTimeout).toHaveBeenCalled();
  });
});