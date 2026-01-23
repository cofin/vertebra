import { SqlPool } from "../../src/ts/src/broker/serve";

const pool = new SqlPool(5);

pool.setOnAcquire(async (conn) => {
  console.log("Connection acquired");
});

pool.setOnRelease(async (conn) => {
  console.log("Connection released");
});

pool.setOnError(async (conn, error) => {
  console.error("Connection error:", error);
});

// Configure health checks
pool.startHealthCheckLoop(5000); // Check every 5s
pool.startIdleReaper(10000); // Reap after 10s idle

console.log("Pool health demo running...");
