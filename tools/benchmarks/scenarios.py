from __future__ import annotations

import msgspec


class Scenario(msgspec.Struct, frozen=True, gc=False):
    """A benchmark scenario describing an HTTP or gRPC request to test."""

    path: str
    method: str = "GET"
    body: str | None = None
    description: str = ""
    concurrency_levels: list[int] | None = None
    min_rps: float | None = None
    max_latency_ms: float | None = None
    protocol: str = "http1"
    load_mode: str = "closed_loop"
    driver: str = "oha"
    grpc_method: str | None = None


SCENARIOS: dict[str, Scenario] = {
    "hello": Scenario(
        path="/hello",
        description="Minimal hello-world response",
        min_rps=150000,
        max_latency_ms=1.0,
    ),
    "plaintext": Scenario(
        path="/plaintext",
        description="Plaintext body response",
        min_rps=140000,
        max_latency_ms=1.5,
    ),
    "body_1k": Scenario(
        path="/b1k",
        description="1 KB static body",
        min_rps=120000,
        max_latency_ms=2.0,
    ),
    "body_10k": Scenario(
        path="/b10k",
        description="10 KB static body",
        min_rps=80000,
        max_latency_ms=5.0,
    ),
    "body_100k": Scenario(
        path="/b100k",
        description="100 KB static body",
        min_rps=20000,
        max_latency_ms=20.0,
    ),
    "json": Scenario(
        path="/json",
        description="JSON serialization",
        min_rps=100000,
        max_latency_ms=2.5,
    ),
    "echo": Scenario(
        path="/echo",
        method="POST",
        body="x" * 1024,
        description="Echo 1 KB POST body",
        min_rps=100000,
        max_latency_ms=3.0,
    ),
    "io_10ms": Scenario(
        path="/io10",
        description="Simulated 10 ms I/O delay",
        min_rps=5000,
        max_latency_ms=15.0,
    ),
    "db": Scenario(
        path="/db",
        description="TechEmpower single database query",
    ),
    "queries_1": Scenario(
        path="/queries?queries=1",
        description="TechEmpower 1 database query",
    ),
    "queries_5": Scenario(
        path="/queries?queries=5",
        description="TechEmpower 5 database queries",
    ),
    "queries_10": Scenario(
        path="/queries?queries=10",
        description="TechEmpower 10 database queries",
    ),
    "queries_20": Scenario(
        path="/queries?queries=20",
        description="TechEmpower 20 database queries",
    ),
    "updates_5": Scenario(
        path="/updates?queries=5",
        description="TechEmpower 5 database updates",
    ),
    "updates_20": Scenario(
        path="/updates?queries=20",
        description="TechEmpower 20 database updates",
    ),
    "cached_queries_10": Scenario(
        path="/cached-queries?count=10",
        description="TechEmpower 10 cached queries",
    ),
    "cached_queries_100": Scenario(
        path="/cached-queries?count=100",
        description="TechEmpower 100 cached queries",
    ),
    "object_get_4k": Scenario(
        path="/object/bench-4k",
        description="Object storage GET 4 KB payload",
    ),
    "object_put_4k": Scenario(
        path="/object/bench-4k",
        method="PUT",
        body="x" * 4096,
        description="Object storage PUT 4 KB payload",
    ),
    "object_get_64k": Scenario(
        path="/object/bench-64k",
        description="Object storage GET 64 KB payload",
    ),
    "grpc_db": Scenario(
        path="/vertebra.bench.BenchmarkService/GetDb",
        method="POST",
        description="gRPC ASGI single database query",
        protocol="grpc",
        driver="ghz",
        grpc_method="vertebra.bench.BenchmarkService.GetDb",
    ),
    "grpc_queries_10": Scenario(
        path="/vertebra.bench.BenchmarkService/GetQueries",
        method="POST",
        body='{"count":10}',
        description="gRPC ASGI 10 database queries",
        protocol="grpc",
        driver="ghz",
        grpc_method="vertebra.bench.BenchmarkService.GetQueries",
    ),
    "grpc_native_db": Scenario(
        path="/vertebra.rpc.ProcedureService/get_db",
        method="POST",
        description="gRPC native H2C single database query",
        protocol="grpc",
        driver="ghz",
        grpc_method="vertebra.rpc.ProcedureService.get_db",
    ),
    "grpc_native_queries_10": Scenario(
        path="/vertebra.rpc.ProcedureService/get_queries",
        method="POST",
        body='{"count":10}',
        description="gRPC native H2C 10 database queries",
        protocol="grpc",
        driver="ghz",
        grpc_method="vertebra.rpc.ProcedureService.get_queries",
    ),
}

QUICK_SCENARIOS: list[str] = ["hello", "body_1k", "json", "io_10ms"]
DEFAULT_SCENARIOS: list[str] = ["hello", "plaintext", "body_1k", "body_10k", "json", "echo", "io_10ms"]
FULL_SCENARIOS: list[str] = list(SCENARIOS)
MULTIWORKER_SCENARIOS: list[str] = ["hello", "json", "echo", "io_10ms"]
DATA_SCENARIOS: list[str] = [
    "json",
    "db",
    "queries_1",
    "queries_5",
    "queries_10",
    "queries_20",
    "updates_5",
    "updates_20",
    "cached_queries_10",
    "cached_queries_100",
]
STORAGE_SCENARIOS: list[str] = ["object_put_4k", "object_get_4k", "object_get_64k"]
GRPC_SCENARIOS: list[str] = ["grpc_db", "grpc_queries_10"]
GRPC_NATIVE_SCENARIOS: list[str] = ["grpc_native_db", "grpc_native_queries_10"]
