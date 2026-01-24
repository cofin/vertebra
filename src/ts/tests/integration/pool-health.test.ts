import { describe, expect, it, mock } from "bun:test";
import { SqlPool } from "../../src/broker/serve";

describe("Pool Health Integration", () => {
  it("should record metrics on health check failures", async () => {
    const pool = new SqlPool(10);
    const token = await pool.acquire();
    await pool.release(token);

    // Fail next validation
    pool["validateConnection"] = mock(async () => false);

    // Force health check
    await pool["runHealthCheck"]();

    // Check internal metrics via stats() - assuming we expose them or can inspect private state
    // We updated BasePoolAdapter to track these but stats() returns PoolStats type which we haven't updated yet in base-pool.ts definition
    // But the runtime object will have them.
    const stats = pool.stats() as any;
    // We expect _healthChecksFailure to be incremented
    // But stats() returns mapped values. Let's check the internal properties via 'any' cast for testing
    const internal = pool as any;
    expect(internal._healthChecksFailure).toBe(1);
    expect(internal._removalsHealth).toBe(1);
    expect(pool.inUse()).toBe(0);
  });

  it("should record metrics on idle timeout", async () => {
    const pool = new SqlPool(10);
    const token = await pool.acquire();
    await pool.release(token);

    // Set old timestamp
    (pool as any).connectionTimestamps.set(token, Date.now() - 1000);

    await pool["reapIdleConnections"](50);

    const internal = pool as any;
    expect(internal._removalsTimeout).toBe(1);
    expect(pool.inUse()).toBe(0);
  });
});
