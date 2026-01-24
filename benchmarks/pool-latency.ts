import { SqlPool } from "../src/ts/src/broker/serve";

async function runBenchmark() {
  const pool = new SqlPool(100);
  const iterations = 10000;
  const latencies: number[] = [];

  console.log(`Running ${iterations} pool acquisitions...`);

  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    const token = await pool.acquire();
    latencies.push(performance.now() - start);
    await pool.release(token);
  }

  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(iterations * 0.5)];
  const p99 = latencies[Math.floor(iterations * 0.99)];

  console.log(`P50: ${p50.toFixed(3)}ms`);
  console.log(`P99: ${p99.toFixed(3)}ms`);
}

runBenchmark();
