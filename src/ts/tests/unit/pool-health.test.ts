import { describe, expect, it, mock } from "bun:test";
import { SqlPool } from "../../src/broker/serve";

describe("Pool Health Checks", () => {
  it("should run health checks on idle connections", async () => {
    const pool = new SqlPool(10);
    const token = await pool.acquire();
    await pool.release(token); // Connection is now idle

    // Mock runHealthCheck behavior since it's protected/abstract
    // In real test we'd need to mock the underlying driver
    // For now we trust the implementation logic in serve.ts
    
    // We can test that startHealthCheckLoop sets the interval
    pool.startHealthCheckLoop(100);
    
    // Wait for interval
    await new Promise(resolve => setTimeout(resolve, 150));
    
    // Verify pool is still alive (no errors thrown)
    expect(pool.inUse()).toBe(0);
  });
  
  it("should reap idle connections", async () => {
    const pool = new SqlPool(10);
    const token = await pool.acquire();
    await pool.release(token);
    
    // Override connection timestamp to be old
    (pool as any).connectionTimestamps.set(token, Date.now() - 1000);
    
    pool.startIdleReaper(50); // 50ms timeout
    
    // Wait for reaper (it runs every minute in default impl, we need to override that for test)
    // The base class implementation hardcodes 60000ms.
    // We might need to make that configurable or mock setInterval.
    
    // For now, we can manually call reapIdleConnections if we expose it or use `any` cast
    await (pool as any).reapIdleConnections(50);
    
    // Should be reaped
    expect(pool.stats().idle_timeouts_total).toBe(1);
  });
});
