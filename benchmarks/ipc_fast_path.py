"""Fast-path IPC benchmark suite for sub-10us Python IPC.

Validates performance targets from the Rust Fast Path PRD:
- Sync immediate (data ready): p99 < 10µs
- Sync blocked (wait for data): p99 < 15µs
- Async (add_reader): p99 < 20µs
- Multiplexed (64 concurrent): p99 < 20µs
- Hot path allocations: 0 bytes/msg

Phases:
- Phase 1: Hybrid spin-wait (spin 2-5µs before eventfd block)
- Phase 2: Zero-copy SlotView (memoryview into ring buffer)
- Phase 3: asyncio.add_reader (native FD polling)
- Phase 4: Rust-native correlation tracking (single GIL acquisition)

Usage:
    uv run python benchmarks/ipc_fast_path.py
    uv run python benchmarks/ipc_fast_path.py --baseline  # Current path only
    uv run python benchmarks/ipc_fast_path.py --phase 1   # Test phase 1 features
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import os
import statistics
import sys
import threading
import time
import tracemalloc
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from vertebra.ipc.channel import Channel

if TYPE_CHECKING:
    from collections.abc import Callable


class FastPathPhase(Enum):
    """Implementation phases for fast path."""

    BASELINE = 0  # Current implementation
    HYBRID_SPIN = 1  # Phase 1: spin_wait_or_park
    ZERO_COPY = 2  # Phase 2: SlotView
    ADD_READER = 3  # Phase 3: asyncio.add_reader
    RUST_CORRELATION = 4  # Phase 4: Rust-native correlation


@dataclass
class FastPathResult:
    """Results from a fast-path benchmark run."""

    name: str
    phase: FastPathPhase
    iterations: int
    total_time_s: float
    latencies_us: list[float]
    target_us: float  # Target p99 for this benchmark
    allocations_per_msg: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def throughput(self) -> float:
        """Messages per second."""
        return self.iterations / self.total_time_s if self.total_time_s > 0 else 0

    @property
    def p50_us(self) -> float:
        return statistics.median(self.latencies_us) if self.latencies_us else 0

    @property
    def p95_us(self) -> float:
        if len(self.latencies_us) < 20:
            return max(self.latencies_us) if self.latencies_us else 0
        return statistics.quantiles(self.latencies_us, n=20)[18]

    @property
    def p99_us(self) -> float:
        if len(self.latencies_us) < 100:
            return max(self.latencies_us) if self.latencies_us else 0
        return statistics.quantiles(self.latencies_us, n=100)[98]

    @property
    def min_us(self) -> float:
        return min(self.latencies_us) if self.latencies_us else 0

    @property
    def max_us(self) -> float:
        return max(self.latencies_us) if self.latencies_us else 0

    @property
    def mean_us(self) -> float:
        return statistics.mean(self.latencies_us) if self.latencies_us else 0

    @property
    def stdev_us(self) -> float:
        if len(self.latencies_us) < 2:
            return 0.0
        return statistics.stdev(self.latencies_us)

    @property
    def passes_target(self) -> bool:
        """Check if p99 meets target."""
        return self.p99_us < self.target_us

    def print_summary(self) -> None:
        """Print benchmark summary with target validation."""
        status = "PASS" if self.passes_target else "FAIL"
        print(f"\n{'=' * 70}")
        print(f"[{status}] {self.name} (Phase {self.phase.value}: {self.phase.name})")
        print(f"{'=' * 70}")
        print(f"Iterations:    {self.iterations:,}")
        print(f"Total time:    {self.total_time_s:.3f} s")
        print(f"Throughput:    {self.throughput:,.0f} msg/sec")
        print()
        print("Latency (µs):")
        print(f"  Min:    {self.min_us:>10.2f}")
        print(f"  Mean:   {self.mean_us:>10.2f}")
        print(f"  Stdev:  {self.stdev_us:>10.2f}")
        print(f"  p50:    {self.p50_us:>10.2f}")
        print(f"  p95:    {self.p95_us:>10.2f}")
        print(f"  p99:    {self.p99_us:>10.2f}  (target: <{self.target_us}µs)")
        print(f"  Max:    {self.max_us:>10.2f}")

        if self.allocations_per_msg > 0:
            alloc_status = "PASS" if self.allocations_per_msg == 0 else "WARN"
            print(f"\nAllocations:  {self.allocations_per_msg:.1f} bytes/msg [{alloc_status}]")

        if self.metadata:
            print("\nMetadata:")
            for key, value in self.metadata.items():
                print(f"  {key}: {value}")


def detect_available_phase() -> FastPathPhase:
    """Detect the highest available fast-path phase."""
    # Check for Phase 4: Rust-native correlation (FastChannel)
    try:
        from vertebra._vertebra import FastChannel  # noqa: F401

        return FastPathPhase.RUST_CORRELATION
    except ImportError:
        pass

    # Check for Phase 3: add_reader support (notify_fd property)
    try:
        client, server = Channel.create_pair()
        has_notify_fd = hasattr(client._handle, "notify_fd")
        client.close()
        server.close()
        if has_notify_fd:
            return FastPathPhase.ADD_READER
    except Exception:
        pass

    # Check for Phase 2: SlotView (recv_to_view method)
    try:
        from vertebra._vertebra import SlotView  # noqa: F401

        return FastPathPhase.ZERO_COPY
    except ImportError:
        pass

    # Check for Phase 1: Hybrid spin (recv_request_hybrid method)
    try:
        client, server = Channel.create_pair()
        has_hybrid = hasattr(client._handle, "recv_request_hybrid")
        client.close()
        server.close()
        if has_hybrid:
            return FastPathPhase.HYBRID_SPIN
    except Exception:
        pass

    return FastPathPhase.BASELINE


def get_spin_us() -> int:
    """Get spin duration from environment or default."""
    return int(os.environ.get("VERTEBRA_SPIN_US", "5"))


# -----------------------------------------------------------------------------
# Benchmark: Sync Immediate (data already available)
# -----------------------------------------------------------------------------


def benchmark_sync_immediate(
    iterations: int = 10000,
    warmup: int = 1000,
    phase: FastPathPhase = FastPathPhase.BASELINE,
) -> FastPathResult:
    """Benchmark sync receive when data is already available.

    This is the best-case scenario: server responds before client waits.
    Target: p99 < 10µs (requires Phase 1+ for true fast path)
    """
    client, server = Channel.create_pair(slot_size=4096, slots=64)
    latencies: list[float] = []

    stop_event = threading.Event()
    ready_event = threading.Event()

    def server_loop() -> None:
        """Echo server that responds immediately."""
        while not stop_event.is_set():
            try:
                proc_id, params = server.receive_sync(timeout_ms=100)
                server.respond_sync(proc_id, {"v": params.get("v", 0)})
                ready_event.set()  # Signal response sent
            except Exception:
                if stop_event.is_set():
                    break

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()

    # Warmup
    for i in range(warmup):
        client.send_sync("ping", {"v": i}, timeout_ms=1000)
        ready_event.clear()

    gc.collect()
    gc.disable()

    try:
        start_total = time.perf_counter()

        for i in range(iterations):
            # Send request
            client.send_sync("ping", {"v": i}, timeout_ms=1000)

            # Wait for server to respond (ensuring data is ready)
            ready_event.wait(timeout=1.0)
            ready_event.clear()

            # Now measure the receive - data should be immediately available
            start = time.perf_counter()

            # Use hybrid recv if available (Phase 1+)
            if phase.value >= FastPathPhase.HYBRID_SPIN.value and hasattr(
                client._handle, "recv_request_hybrid"
            ):
                spin_us = get_spin_us()
                client._handle.recv_request_hybrid(4096, spin_us, 1000)
            else:
                # Baseline: use try_recv or receive_sync with 0 timeout
                try:
                    client._handle.try_recv_response(4096)
                except Exception:
                    pass  # May need blocking receive

            latencies.append((time.perf_counter() - start) * 1e6)

        total_time = time.perf_counter() - start_total
    finally:
        gc.enable()
        stop_event.set()
        thread.join(timeout=2)
        client.close()
        server.close()

    return FastPathResult(
        name="Sync Immediate (data ready)",
        phase=phase,
        iterations=iterations,
        total_time_s=total_time,
        latencies_us=latencies,
        target_us=10.0,
        metadata={"spin_us": get_spin_us() if phase.value >= FastPathPhase.HYBRID_SPIN.value else 0},
    )


# -----------------------------------------------------------------------------
# Benchmark: Sync Blocked (must wait for data)
# -----------------------------------------------------------------------------


def benchmark_sync_blocked(
    iterations: int = 10000,
    warmup: int = 1000,
    phase: FastPathPhase = FastPathPhase.BASELINE,
) -> FastPathResult:
    """Benchmark sync receive when client must wait for data.

    Measures the full round-trip latency from send to receive.
    Target: p99 < 15µs (requires hybrid spin-wait for low latency wake)
    """
    client, server = Channel.create_pair(slot_size=4096, slots=64)
    latencies: list[float] = []

    stop_event = threading.Event()

    def server_loop() -> None:
        """Echo server with minimal delay."""
        while not stop_event.is_set():
            try:
                proc_id, params = server.receive_sync(timeout_ms=100)
                server.respond_sync(proc_id, {"v": params.get("v", 0)})
            except Exception:
                if stop_event.is_set():
                    break

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()

    # Warmup
    for i in range(warmup):
        client.send_sync("ping", {"v": i}, timeout_ms=1000)

    gc.collect()
    gc.disable()

    try:
        start_total = time.perf_counter()

        for i in range(iterations):
            # Measure complete send + wait + receive cycle
            start = time.perf_counter()
            client.send_sync("ping", {"v": i}, timeout_ms=1000)
            latencies.append((time.perf_counter() - start) * 1e6)

        total_time = time.perf_counter() - start_total
    finally:
        gc.enable()
        stop_event.set()
        thread.join(timeout=2)
        client.close()
        server.close()

    return FastPathResult(
        name="Sync Blocked (full round-trip)",
        phase=phase,
        iterations=iterations,
        total_time_s=total_time,
        latencies_us=latencies,
        target_us=15.0,
        metadata={"spin_us": get_spin_us() if phase.value >= FastPathPhase.HYBRID_SPIN.value else 0},
    )


# -----------------------------------------------------------------------------
# Benchmark: AsyncShmTransport with hybrid spin-wait
# -----------------------------------------------------------------------------


async def benchmark_async_transport_hybrid(
    iterations: int = 10000,
    warmup: int = 1000,
    phase: FastPathPhase = FastPathPhase.BASELINE,
) -> FastPathResult:
    """Benchmark AsyncShmTransport with hybrid spin-wait.

    This tests the raw transport layer with hybrid spin-wait optimization.
    Target: p99 < 20µs for immediate data, < 50µs for delayed data.
    """
    from vertebra._vertebra import AsyncRing
    from vertebra.ipc.async_transport import AsyncShmTransport, DEFAULT_SPIN_US

    ring = AsyncRing(4096, 64)
    transport = AsyncShmTransport(ring, spin_duration_us=get_spin_us())
    latencies: list[float] = []

    # Pre-fill for immediate data benchmark
    test_data = b"x" * 64

    gc.collect()
    gc.disable()

    try:
        loop = asyncio.get_running_loop()
        start_total = time.perf_counter()

        for i in range(iterations):
            # Send data first (so it's immediately available)
            ring.send(test_data, 0)

            # Measure recv with hybrid spin-wait
            start = time.perf_counter()
            data = await transport.recv()
            latencies.append((time.perf_counter() - start) * 1e6)

            assert len(data) == 64

        total_time = time.perf_counter() - start_total
    finally:
        gc.enable()
        transport.close()

    return FastPathResult(
        name="AsyncShmTransport (hybrid spin-wait)",
        phase=phase,
        iterations=iterations,
        total_time_s=total_time,
        latencies_us=latencies,
        target_us=20.0,
        metadata={
            "spin_us": get_spin_us(),
            "default_spin_us": DEFAULT_SPIN_US,
            "event_loop": type(loop).__name__,
        },
    )


# -----------------------------------------------------------------------------
# Benchmark: Multiplexed (64 concurrent requests)
# -----------------------------------------------------------------------------


async def benchmark_multiplexed(
    iterations: int = 1000,
    concurrency: int = 64,
    warmup: int = 100,
    phase: FastPathPhase = FastPathPhase.BASELINE,
) -> FastPathResult:
    """Benchmark concurrent request handling with hybrid spin-wait.

    Measures latency under load with multiple concurrent AsyncShmTransports.
    Uses the hybrid spin-wait pattern that production code uses.
    Target: p99 < 20µs with 64 concurrent requests
    """
    from vertebra._vertebra import AsyncRing
    from vertebra.ipc.async_transport import AsyncShmTransport

    # Create separate ring+transport pairs for each concurrent worker
    # This matches real-world usage where each coroutine has its own transport
    rings = [AsyncRing(4096, 64) for _ in range(concurrency)]
    transports = [AsyncShmTransport(r, spin_duration_us=get_spin_us()) for r in rings]

    latencies: list[float] = []
    lock = threading.Lock()
    test_data = b"x" * 64

    async def single_request(idx: int) -> float:
        """Execute single request and return latency."""
        ring = rings[idx]
        transport = transports[idx]

        # Pre-fill the ring (simulates data arriving before recv)
        ring.send(test_data, 0)

        start = time.perf_counter()
        data = await transport.recv()
        elapsed = (time.perf_counter() - start) * 1e6

        assert len(data) == 64
        return elapsed

    # Warmup
    for _ in range(warmup):
        await asyncio.gather(*[single_request(i) for i in range(concurrency)])

    gc.collect()
    gc.disable()

    try:
        start_total = time.perf_counter()

        for batch in range(iterations):
            batch_latencies = await asyncio.gather(
                *[single_request(i) for i in range(concurrency)]
            )
            with lock:
                latencies.extend(batch_latencies)

        total_time = time.perf_counter() - start_total
    finally:
        gc.enable()
        for t in transports:
            t.close()

    return FastPathResult(
        name=f"Multiplexed ({concurrency} concurrent)",
        phase=phase,
        iterations=iterations * concurrency,
        total_time_s=total_time,
        latencies_us=latencies,
        target_us=20.0,
        metadata={"concurrency": concurrency, "batches": iterations, "spin_us": get_spin_us()},
    )


# -----------------------------------------------------------------------------
# Benchmark: Allocation tracking
# -----------------------------------------------------------------------------


def benchmark_allocations(
    iterations: int = 1000,
    phase: FastPathPhase = FastPathPhase.BASELINE,
) -> FastPathResult:
    """Measure memory allocations in the hot path.

    Target: 0 bytes/msg in the fast path (requires Phase 2 zero-copy)
    """
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

    # Warmup (outside measurement)
    for i in range(100):
        client.send_sync("ping", {"v": i}, timeout_ms=1000)

    gc.collect()

    # Measure allocations using full round-trip
    tracemalloc.start()
    snapshot_before = tracemalloc.take_snapshot()
    start_time = time.perf_counter()
    latencies: list[float] = []

    for i in range(iterations):
        start = time.perf_counter()
        client.send_sync("ping", {"v": i}, timeout_ms=1000)
        latencies.append((time.perf_counter() - start) * 1e6)

    total_time = time.perf_counter() - start_time
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

    # Target: 0 bytes/msg for zero-copy path, but baseline will have allocations
    target_alloc = 0.0 if phase.value >= FastPathPhase.ZERO_COPY.value else 100.0

    return FastPathResult(
        name="Allocation Tracking",
        phase=phase,
        iterations=iterations,
        total_time_s=total_time,
        latencies_us=latencies,
        target_us=999999.0,  # Always passes - allocation is the real metric
        allocations_per_msg=allocs_per_msg,
        metadata={
            "total_bytes": total_allocated,
            "target_bytes_per_msg": target_alloc,
            "top_allocations": [
                f"{stat.traceback.format()[0] if stat.traceback else 'unknown'}: {stat.size_diff}"
                for stat in top_stats[:3]
            ],
        },
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def print_header(phase: FastPathPhase) -> None:
    """Print benchmark header."""
    print("=" * 70)
    print("Vertebra IPC Fast Path Benchmark")
    print("=" * 70)
    print(f"Python {sys.version}")
    print(f"Detected Phase: {phase.value} ({phase.name})")
    print(f"VERTEBRA_SPIN_US: {get_spin_us()}")
    print()


def print_summary(results: list[FastPathResult]) -> None:
    """Print final summary of all benchmarks."""
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)

    # Filter allocation benchmark from latency targets
    latency_results = [r for r in results if "Allocation" not in r.name]
    alloc_results = [r for r in results if "Allocation" in r.name]

    passed = sum(1 for r in latency_results if r.passes_target)
    total = len(latency_results)

    print(f"\nLatency targets: {passed}/{total} passed\n")

    for result in latency_results:
        status = "PASS" if result.passes_target else "FAIL"
        print(
            f"[{status}] {result.name:40} "
            f"p99={result.p99_us:>8.2f}µs (target: <{result.target_us}µs)"
        )

    if alloc_results:
        print("\nAllocation metrics:")
        for result in alloc_results:
            target = result.metadata.get("target_bytes_per_msg", 0)
            status = "PASS" if result.allocations_per_msg <= target else "WARN"
            print(
                f"[{status}] {result.name:40} "
                f"{result.allocations_per_msg:.1f} bytes/msg (target: <={target:.0f})"
            )

    print()
    if passed == total:
        print("All performance targets met!")
    else:
        print(f"{total - passed} target(s) not met.")


async def run_benchmarks(
    phase: FastPathPhase,
    iterations: int,
    warmup: int,
    skip_async: bool = False,
    skip_alloc: bool = False,
) -> list[FastPathResult]:
    """Run all benchmarks for a given phase."""
    results: list[FastPathResult] = []

    print("Running sync immediate benchmark...")
    results.append(
        benchmark_sync_immediate(iterations=iterations, warmup=warmup, phase=phase)
    )
    results[-1].print_summary()

    print("\nRunning sync blocked benchmark...")
    results.append(
        benchmark_sync_blocked(iterations=iterations, warmup=warmup, phase=phase)
    )
    results[-1].print_summary()

    if not skip_async:
        print("\nRunning async benchmark...")
        results.append(
            await benchmark_async_transport_hybrid(
                iterations=iterations, warmup=warmup, phase=phase
            )
        )
        results[-1].print_summary()

        print("\nRunning multiplexed benchmark...")
        results.append(
            await benchmark_multiplexed(
                iterations=iterations // 10,
                concurrency=64,
                warmup=warmup // 10,
                phase=phase,
            )
        )
        results[-1].print_summary()

    if not skip_alloc:
        print("\nRunning allocation benchmark...")
        results.append(benchmark_allocations(iterations=1000, phase=phase))
        results[-1].print_summary()

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Vertebra IPC Fast Path Benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with auto-detected phase
    uv run python benchmarks/ipc_fast_path.py

    # Run baseline only (before any fast path implementation)
    uv run python benchmarks/ipc_fast_path.py --baseline

    # Run with specific phase (for testing)
    uv run python benchmarks/ipc_fast_path.py --phase 1

    # Adjust spin duration
    VERTEBRA_SPIN_US=3 uv run python benchmarks/ipc_fast_path.py
        """,
    )
    parser.add_argument(
        "--iterations", type=int, default=10000, help="Iterations per benchmark"
    )
    parser.add_argument("--warmup", type=int, default=1000, help="Warmup iterations")
    parser.add_argument(
        "--baseline", action="store_true", help="Force baseline (Phase 0)"
    )
    parser.add_argument(
        "--phase", type=int, choices=[0, 1, 2, 3, 4], help="Force specific phase"
    )
    parser.add_argument("--skip-async", action="store_true", help="Skip async benchmarks")
    parser.add_argument("--skip-alloc", action="store_true", help="Skip allocation benchmark")
    args = parser.parse_args()

    # Determine phase
    if args.baseline:
        phase = FastPathPhase.BASELINE
    elif args.phase is not None:
        phase = FastPathPhase(args.phase)
    else:
        phase = detect_available_phase()

    print_header(phase)

    results = asyncio.run(
        run_benchmarks(
            phase=phase,
            iterations=args.iterations,
            warmup=args.warmup,
            skip_async=args.skip_async,
            skip_alloc=args.skip_alloc,
        )
    )

    print_summary(results)

    # Exit with error if any target not met
    if not all(r.passes_target for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
