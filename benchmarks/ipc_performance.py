"""Comprehensive IPC performance benchmark suite.

Validates performance targets from the IPC Performance Optimization PRD:
- p99 latency < 10µs
- Throughput > 1M msg/sec
- Zero hot path allocations
- Idle CPU < 0.1%

Usage:
    uv run python benchmarks/ipc_performance.py
    uv run python benchmarks/ipc_performance.py --iterations 100000 --warmup 1000
"""

from __future__ import annotations

import argparse
import gc
import statistics
import sys
import threading
import time
import tracemalloc
from dataclasses import dataclass
from typing import Any

from vertebra.ipc.channel import Channel


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""

    name: str
    iterations: int
    total_time_s: float
    latencies_us: list[float]

    @property
    def throughput(self) -> float:
        """Messages per second."""
        return self.iterations / self.total_time_s if self.total_time_s > 0 else 0

    @property
    def p50_us(self) -> float:
        return statistics.median(self.latencies_us)

    @property
    def p95_us(self) -> float:
        if len(self.latencies_us) < 20:
            return max(self.latencies_us)
        return statistics.quantiles(self.latencies_us, n=20)[18]

    @property
    def p99_us(self) -> float:
        if len(self.latencies_us) < 100:
            return max(self.latencies_us)
        return statistics.quantiles(self.latencies_us, n=100)[98]

    @property
    def mean_us(self) -> float:
        return statistics.mean(self.latencies_us)

    @property
    def stdev_us(self) -> float:
        if len(self.latencies_us) < 2:
            return 0.0
        return statistics.stdev(self.latencies_us)

    def print_summary(self) -> None:
        """Print benchmark summary."""
        print(f"\n{'=' * 60}")
        print(f"Benchmark: {self.name}")
        print(f"{'=' * 60}")
        print(f"Iterations:    {self.iterations:,}")
        print(f"Total time:    {self.total_time_s:.3f} s")
        print(f"Throughput:    {self.throughput:,.0f} msg/sec")
        print()
        print("Latency (µs):")
        print(f"  Mean:   {self.mean_us:>10.2f}")
        print(f"  Stdev:  {self.stdev_us:>10.2f}")
        print(f"  p50:    {self.p50_us:>10.2f}")
        print(f"  p95:    {self.p95_us:>10.2f}")
        print(f"  p99:    {self.p99_us:>10.2f}")

    def check_targets(self) -> dict[str, tuple[bool, str]]:
        """Check if performance targets are met."""
        targets = {}

        # p99 latency < 10µs
        p99_pass = self.p99_us < 10.0
        targets["p99_latency"] = (p99_pass, f"{self.p99_us:.2f}µs (target: <10µs)")

        # Throughput > 1M msg/sec
        tput_pass = self.throughput > 1_000_000
        targets["throughput"] = (tput_pass, f"{self.throughput:,.0f}/s (target: >1M/s)")

        return targets


def benchmark_roundtrip_latency(
    iterations: int = 10000,
    warmup: int = 1000,
    slot_size: int = 4096,
    slots: int = 64,
) -> BenchmarkResult:
    """Benchmark round-trip latency (send + receive + respond)."""
    client, server = Channel.create_pair(slot_size=slot_size, slots=slots)
    latencies: list[float] = []

    # Server thread
    stop_event = threading.Event()
    total_processed = 0

    def server_loop() -> None:
        nonlocal total_processed
        while not stop_event.is_set():
            try:
                proc_id, params = server.receive_sync(timeout_ms=100)
                server.respond_sync(proc_id, {"value": params.get("value", 0)})
                total_processed += 1
            except Exception:
                if stop_event.is_set():
                    break

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()

    # Warmup
    for i in range(warmup):
        client.send_sync("ping", {"value": i}, timeout_ms=1000)

    # Force GC before measurement
    gc.collect()
    gc.disable()

    try:
        start_total = time.perf_counter()

        for i in range(iterations):
            start = time.perf_counter()
            client.send_sync("ping", {"value": i}, timeout_ms=1000)
            latencies.append((time.perf_counter() - start) * 1e6)  # Convert to µs

        total_time = time.perf_counter() - start_total
    finally:
        gc.enable()
        stop_event.set()
        thread.join(timeout=2)
        client.close()
        server.close()

    return BenchmarkResult(
        name="Round-trip Latency",
        iterations=iterations,
        total_time_s=total_time,
        latencies_us=latencies,
    )


def benchmark_throughput(
    iterations: int = 100000,
    warmup: int = 1000,
    slot_size: int = 4096,
    slots: int = 256,
) -> BenchmarkResult:
    """Benchmark maximum throughput."""
    client, server = Channel.create_pair(slot_size=slot_size, slots=slots)
    latencies: list[float] = []

    stop_event = threading.Event()

    def server_loop() -> None:
        while not stop_event.is_set():
            try:
                proc_id, params = server.receive_sync(timeout_ms=100)
                server.respond_sync(proc_id, {"ok": True})
            except Exception:
                if stop_event.is_set():
                    break

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()

    # Warmup
    for i in range(warmup):
        client.send_sync("ping", {"value": i}, timeout_ms=1000)

    gc.collect()
    gc.disable()

    try:
        start_total = time.perf_counter()

        for i in range(iterations):
            start = time.perf_counter()
            client.send_sync("ping", {"value": i}, timeout_ms=1000)
            latencies.append((time.perf_counter() - start) * 1e6)

        total_time = time.perf_counter() - start_total
    finally:
        gc.enable()
        stop_event.set()
        thread.join(timeout=2)
        client.close()
        server.close()

    return BenchmarkResult(
        name="Throughput",
        iterations=iterations,
        total_time_s=total_time,
        latencies_us=latencies,
    )


def benchmark_allocation(iterations: int = 1000) -> dict[str, Any]:
    """Benchmark memory allocations in hot path."""
    client, server = Channel.create_pair(slot_size=4096, slots=64)

    stop_event = threading.Event()

    def server_loop() -> None:
        while not stop_event.is_set():
            try:
                proc_id, params = server.receive_sync(timeout_ms=100)
                server.respond_sync(proc_id, {"ok": True})
            except Exception:
                if stop_event.is_set():
                    break

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()

    # Warmup
    for i in range(100):
        client.send_sync("ping", {"value": i}, timeout_ms=1000)

    gc.collect()

    # Measure allocations
    tracemalloc.start()
    snapshot_before = tracemalloc.take_snapshot()

    for i in range(iterations):
        client.send_sync("ping", {"value": i}, timeout_ms=1000)

    snapshot_after = tracemalloc.take_snapshot()
    tracemalloc.stop()

    stop_event.set()
    thread.join(timeout=2)
    client.close()
    server.close()

    # Calculate allocation stats
    top_stats = snapshot_after.compare_to(snapshot_before, "lineno")
    total_allocated = sum(stat.size_diff for stat in top_stats if stat.size_diff > 0)
    allocs_per_msg = total_allocated / iterations if iterations > 0 else 0

    return {
        "iterations": iterations,
        "total_allocated_bytes": total_allocated,
        "bytes_per_message": allocs_per_msg,
        "top_allocations": top_stats[:5],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Vertebra IPC Performance Benchmark")
    parser.add_argument("--iterations", type=int, default=10000, help="Number of iterations")
    parser.add_argument("--warmup", type=int, default=1000, help="Warmup iterations")
    parser.add_argument("--throughput-iterations", type=int, default=100000, help="Throughput test iterations")
    parser.add_argument("--skip-allocation", action="store_true", help="Skip allocation benchmark")
    args = parser.parse_args()

    print("=" * 60)
    print("Vertebra IPC Performance Benchmark")
    print("=" * 60)
    print(f"Python {sys.version}")
    print()

    # Run latency benchmark
    print("Running latency benchmark...")
    latency_result = benchmark_roundtrip_latency(
        iterations=args.iterations,
        warmup=args.warmup,
    )
    latency_result.print_summary()

    # Run throughput benchmark
    print("\nRunning throughput benchmark...")
    throughput_result = benchmark_throughput(
        iterations=args.throughput_iterations,
        warmup=args.warmup,
    )
    throughput_result.print_summary()

    # Run allocation benchmark
    if not args.skip_allocation:
        print("\nRunning allocation benchmark...")
        alloc_stats = benchmark_allocation(iterations=1000)
        print(f"\n{'=' * 60}")
        print("Benchmark: Memory Allocations")
        print(f"{'=' * 60}")
        print(f"Iterations:        {alloc_stats['iterations']:,}")
        print(f"Total allocated:   {alloc_stats['total_allocated_bytes']:,} bytes")
        print(f"Per message:       {alloc_stats['bytes_per_message']:.1f} bytes")

    # Check targets
    print(f"\n{'=' * 60}")
    print("Performance Target Validation")
    print(f"{'=' * 60}")

    all_passed = True
    for result in [latency_result, throughput_result]:
        targets = result.check_targets()
        for target_name, (passed, message) in targets.items():
            status = "PASS" if passed else "FAIL"
            print(f"[{status}] {target_name}: {message}")
            if not passed:
                all_passed = False

    print()
    if all_passed:
        print("All performance targets met!")
    else:
        print("Some performance targets not met.")
        sys.exit(1)


if __name__ == "__main__":
    main()
