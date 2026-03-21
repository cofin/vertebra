from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Scenario:
    """A benchmark scenario describing an HTTP request to test."""

    path: str
    method: str = "GET"
    body: str | None = None
    description: str = ""
    concurrency_levels: list[int] | None = None


SCENARIOS: dict[str, Scenario] = {
    "hello": Scenario(path="/hello", description="Minimal hello-world response"),
    "plaintext": Scenario(path="/plaintext", description="Plaintext body response"),
    "body_1k": Scenario(path="/b1k", description="1 KB static body"),
    "body_10k": Scenario(path="/b10k", description="10 KB static body"),
    "body_100k": Scenario(path="/b100k", description="100 KB static body"),
    "json": Scenario(path="/json", description="JSON serialization"),
    "echo": Scenario(path="/echo", method="POST", body="x" * 1024, description="Echo 1 KB POST body"),
    "io_10ms": Scenario(path="/io10", description="Simulated 10 ms I/O delay"),
}

QUICK_SCENARIOS: list[str] = ["hello", "body_1k", "json", "io_10ms"]
DEFAULT_SCENARIOS: list[str] = ["hello", "plaintext", "body_1k", "body_10k", "json", "echo", "io_10ms"]
FULL_SCENARIOS: list[str] = list(SCENARIOS)

# Multi-worker scenarios: focus on IO-bound (where parallelism matters most)
# and hello (baseline throughput ceiling).
MULTIWORKER_SCENARIOS: list[str] = ["hello", "json", "echo", "io_10ms"]
