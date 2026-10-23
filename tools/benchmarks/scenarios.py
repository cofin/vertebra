from __future__ import annotations

import msgspec


class Scenario(msgspec.Struct, frozen=True, gc=False):
    """A benchmark scenario describing an HTTP request to test."""

    path: str
    method: str = "GET"
    body: str | None = None
    description: str = ""
    concurrency_levels: list[int] | None = None
    min_rps: float | None = None
    max_latency_ms: float | None = None


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
}

QUICK_SCENARIOS: list[str] = ["hello", "body_1k", "json", "io_10ms"]
DEFAULT_SCENARIOS: list[str] = ["hello", "plaintext", "body_1k", "body_10k", "json", "echo", "io_10ms"]
FULL_SCENARIOS: list[str] = list(SCENARIOS)
MULTIWORKER_SCENARIOS: list[str] = ["hello", "json", "echo", "io_10ms"]
