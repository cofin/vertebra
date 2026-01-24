/**
 * Comprehensive IPC performance benchmark suite for TypeScript.
 *
 * Validates performance targets from the IPC Performance Optimization PRD:
 * - p99 latency < 10µs
 * - Throughput > 1M msg/sec
 *
 * Usage:
 *   cd src/ts && bun run ../../benchmarks/ipc_performance.ts
 */

import { ShmRing, ShmMpmcRing } from "../src/ts/src/broker/shm";
import { encodeRequest, decodeResponse } from "../src/ts/src/protocol/serialization";
import { Flags, ResponseFlags } from "../src/ts/src/protocol/headers";

interface BenchmarkResult {
  name: string;
  iterations: number;
  totalTimeMs: number;
  latenciesUs: number[];
}

function median(arr: number[]): number {
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function percentile(arr: number[], p: number): number {
  const sorted = [...arr].sort((a, b) => a - b);
  const index = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, index)];
}

function mean(arr: number[]): number {
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function stdev(arr: number[]): number {
  const m = mean(arr);
  const variance = arr.reduce((sum, val) => sum + Math.pow(val - m, 2), 0) / arr.length;
  return Math.sqrt(variance);
}

function printResult(result: BenchmarkResult): void {
  const throughput = result.iterations / (result.totalTimeMs / 1000);
  const p50 = median(result.latenciesUs);
  const p95 = percentile(result.latenciesUs, 95);
  const p99 = percentile(result.latenciesUs, 99);
  const meanLat = mean(result.latenciesUs);
  const stdevLat = stdev(result.latenciesUs);

  console.log(`\n${"=".repeat(60)}`);
  console.log(`Benchmark: ${result.name}`);
  console.log(`${"=".repeat(60)}`);
  console.log(`Iterations:    ${result.iterations.toLocaleString()}`);
  console.log(`Total time:    ${(result.totalTimeMs / 1000).toFixed(3)} s`);
  console.log(`Throughput:    ${throughput.toLocaleString(undefined, { maximumFractionDigits: 0 })} msg/sec`);
  console.log();
  console.log("Latency (µs):");
  console.log(`  Mean:   ${meanLat.toFixed(2).padStart(10)}`);
  console.log(`  Stdev:  ${stdevLat.toFixed(2).padStart(10)}`);
  console.log(`  p50:    ${p50.toFixed(2).padStart(10)}`);
  console.log(`  p95:    ${p95.toFixed(2).padStart(10)}`);
  console.log(`  p99:    ${p99.toFixed(2).padStart(10)}`);

  // Check targets
  const p99Pass = p99 < 10;
  const throughputPass = throughput > 1_000_000;

  console.log();
  console.log(`[${p99Pass ? "PASS" : "FAIL"}] p99_latency: ${p99.toFixed(2)}µs (target: <10µs)`);
  console.log(`[${throughputPass ? "PASS" : "FAIL"}] throughput: ${throughput.toLocaleString(undefined, { maximumFractionDigits: 0 })}/s (target: >1M/s)`);
}

async function benchmarkProtocolEncoding(iterations: number = 100000): Promise<BenchmarkResult> {
  const latencies: number[] = [];
  const payload = { action: "ping", value: 42, data: [1, 2, 3, 4, 5] };

  // Warmup
  for (let i = 0; i < 1000; i++) {
    const encoded = encodeRequest(payload, BigInt(i), Flags.NONE);
    decodeResponse(encoded);
  }

  const startTotal = performance.now();

  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    const encoded = encodeRequest(payload, BigInt(i), Flags.NONE);
    // Simulate response by decoding the request as response (same format)
    decodeResponse(encoded);
    latencies.push((performance.now() - start) * 1000); // Convert to µs
  }

  const totalTime = performance.now() - startTotal;

  return {
    name: "Protocol Encode/Decode",
    iterations,
    totalTimeMs: totalTime,
    latenciesUs: latencies,
  };
}

async function benchmarkShmRingLatency(iterations: number = 10000): Promise<BenchmarkResult | null> {
  const ringName = `/bench-${Date.now()}`;
  let ring: ShmMpmcRing | null = null;

  try {
    ring = ShmMpmcRing.create(ringName, 4096, 256);
  } catch (error) {
    console.log("ShmRing not available (requires native library):", error);
    return null;
  }

  const latencies: number[] = [];
  const payload = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);

  // Warmup
  for (let i = 0; i < 100; i++) {
    ring.send(payload, 0, 0);
    ring.recv(10);
  }

  const startTotal = performance.now();

  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    ring.send(payload, 0, 0);
    ring.recv(10);
    latencies.push((performance.now() - start) * 1000);
  }

  const totalTime = performance.now() - startTotal;

  ring.close();
  ShmMpmcRing.unlink(ringName);

  return {
    name: "ShmRing Send/Recv",
    iterations,
    totalTimeMs: totalTime,
    latenciesUs: latencies,
  };
}

async function main(): Promise<void> {
  console.log("=".repeat(60));
  console.log("Vertebra IPC Performance Benchmark (TypeScript)");
  console.log("=".repeat(60));
  console.log(`Bun ${Bun.version}`);

  // Protocol encoding benchmark
  console.log("\nRunning protocol encoding benchmark...");
  const encodeResult = await benchmarkProtocolEncoding(100000);
  printResult(encodeResult);

  // ShmRing benchmark (if available)
  console.log("\nRunning ShmRing benchmark...");
  const ringResult = await benchmarkShmRingLatency(10000);
  if (ringResult) {
    printResult(ringResult);
  }
}

main().catch(console.error);
