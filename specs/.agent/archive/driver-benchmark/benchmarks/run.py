import asyncio
import time
import statistics
import argparse
from typing import List, Type
from rich.console import Console
from rich.table import Table

from benchmarks.runners.base import BenchmarkRunner
from benchmarks.runners.native_pg import NativePostgresRunner
from benchmarks.runners.native_mysql import NativeMySQLRunner
# from benchmarks.runners.vertebra_runner import VertebraRunner

console = Console()

PG_DSN = "postgresql://benchmark:password@localhost:5432/benchmark_db"
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "benchmark",
    "password": "password",
    "db": "benchmark_db",
    "autocommit": True,
}

SCENARIOS = {
    "select_1": lambda r: r.select_1(),
    "pk_lookup": lambda r: r.pk_lookup(1),  # Always lookup ID 1 for simplicity in microbenchmark
    "large_fetch": lambda r: r.large_fetch(1000),
    "arrow_fetch": lambda r: r.fetch_arrow(10000),
}

async def run_benchmark(runner: BenchmarkRunner, scenario_name: str, concurrency: int, duration: float):
    console.log(f"Running {scenario_name} with concurrency {concurrency} for {duration}s...")
    await runner.setup()
    
    op = SCENARIOS[scenario_name]
    start_time = time.perf_counter()
    end_time = start_time + duration
    count = 0
    latencies = []

    async def worker():
        nonlocal count
        while time.perf_counter() < end_time:
            t0 = time.perf_counter()
            await op(runner)
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000) # ms
            count += 1

    tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
    await asyncio.gather(*tasks)
    
    await runner.teardown()
    
    total_time = time.perf_counter() - start_time
    rps = count / total_time
    p50 = statistics.median(latencies) if latencies else 0
    p99 = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else 0
    
    return {
        "rps": rps,
        "p50": p50,
        "p99": p99,
        "count": count
    }

async def main():
    parser = argparse.ArgumentParser(description="Vertebra Benchmark Runner")
    parser.add_argument("--concurrency", "-c", type=int, default=10, help="Number of concurrent tasks")
    parser.add_argument("--duration", "-d", type=int, default=5, help="Duration in seconds per test")
    parser.add_argument("--drivers", choices=["pg", "mysql", "vertebra", "all"], default="all")
    args = parser.parse_args()

    results = []

    runners = []
    if args.drivers in ["pg", "all"]:
        runners.append(("Native PG", NativePostgresRunner(PG_DSN)))
    if args.drivers in ["mysql", "all"]:
        runners.append(("Native MySQL", NativeMySQLRunner(**MYSQL_CONFIG)))
    
    # Try adding Vertebra
    try:
        from benchmarks.runners.vertebra_runner import VertebraRunner
        if args.drivers in ["vertebra", "all"]:
            # Check if we can instantiate it (requires env vars or mocking?)
            # For now, we assume it works if import works
             runners.append(("Vertebra PG", VertebraRunner(PG_DSN, "postgres")))
    except ImportError:
        console.log("[yellow]Vertebra driver not found, skipping Vertebra benchmarks[/]")
    except Exception as e:
        console.log(f"[red]Failed to init Vertebra runner: {e}[/]")

    table = Table(title=f"Benchmark Results (Concurrency: {args.concurrency})")
    table.add_column("Driver", style="cyan")
    table.add_column("Scenario", style="magenta")
    table.add_column("RPS", justify="right", style="green")
    table.add_column("P50 (ms)", justify="right", style="yellow")
    table.add_column("P99 (ms)", justify="right", style="red")

    for name, runner in runners:
        for scenario in SCENARIOS.keys():
            # Skip arrow fetch for native if we want strict apple-to-apple, 
            # but we defined a fallback in base so it's fine.
            
            try:
                stats = await run_benchmark(runner, scenario, args.concurrency, args.duration)
                table.add_row(
                    name,
                    scenario,
                    f"{stats['rps']:.2f}",
                    f"{stats['p50']:.3f}",
                    f"{stats['p99']:.3f}"
                )
            except Exception as e:
                console.log(f"[red]Error running {name}/{scenario}: {e}[/]")
                table.add_row(name, scenario, "ERROR", "-", "-")

    console.print(table)

if __name__ == "__main__":
    asyncio.run(main())
