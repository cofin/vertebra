import { describe, expect, it, mock } from "bun:test";
import { SqlPool } from "../../src/broker/serve";

describe("Pool Validation", () => {
  it("should validate connection on acquire", async () => {
    const pool = new SqlPool(10);
    // Mock validateConnection to succeed
    pool["validateConnection"] = mock(async () => true);
    
    const token = await pool.acquire();
    expect(token).toBeDefined();
    expect(pool["validateConnection"]).toHaveBeenCalled();
  });

  it("should fail acquire if validation fails", async () => {
    const pool = new SqlPool(10);
    pool["validateConnection"] = mock(async () => false);
    
    let error;
    try {
        await pool.acquire();
    } catch (e) {
        error = e;
    }
    
    expect(error).toBeDefined();
    expect(error.message).toBe("Connection failed validation");
  });

  it("should validate connection on release", async () => {
    const pool = new SqlPool(10);
    const token = await pool.acquire(); // Acquire first (validation passes by default or mock)
    
    pool["validateConnection"] = mock(async () => true);
    await pool.release(token);
    
    expect(pool["validateConnection"]).toHaveBeenCalled();
    // Should be back in idle pool (mock implementation specific)
    // We can check stats or internal state if accessible
  });

  it("should discard connection if post-release validation fails", async () => {
    const pool = new SqlPool(10);
    // First acquire needs to succeed
    pool["validateConnection"] = mock(async () => true);
    const token = await pool.acquire();
    
    // Now fail validation on release
    pool["validateConnection"] = mock(async () => false);
    await pool.release(token);
    
    expect(pool.stats().health_failures_total).toBe(1);
  });
});
