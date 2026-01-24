"""Test scenario definitions for ASGI benchmarks.

Each scenario defines an endpoint, HTTP method, and expected behavior
for consistent comparison across servers.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Scenario:
    """A benchmark test scenario."""

    name: str
    endpoint: str
    description: str
    method: str = "GET"
    body: bytes | None = None
    headers: dict[str, str] = field(default_factory=dict)
    expected_response_bytes: int | None = None  # For validation

    def get_url(self, port: int) -> str:
        """Get the full URL for this scenario."""
        return f"http://127.0.0.1:{port}{self.endpoint}"


# =============================================================================
# Standard Scenarios
# =============================================================================

SCENARIOS: dict[str, Scenario] = {
    # Minimal response - pure server overhead
    "hello": Scenario(
        name="hello",
        endpoint="/hello",
        description="12-byte JSON response (minimal overhead)",
        expected_response_bytes=17,  # {"hello":"world"}
    ),
    # Response size scaling
    "body_10b": Scenario(
        name="body_10b", endpoint="/b10", description="10-byte plaintext response", expected_response_bytes=10
    ),
    "body_1k": Scenario(
        name="body_1k", endpoint="/b1k", description="1KB plaintext response", expected_response_bytes=1024
    ),
    "body_10k": Scenario(
        name="body_10k", endpoint="/b10k", description="10KB plaintext response", expected_response_bytes=10240
    ),
    "body_100k": Scenario(
        name="body_100k", endpoint="/b100k", description="100KB plaintext response", expected_response_bytes=102400
    ),
    # POST with body
    "echo": Scenario(
        name="echo",
        endpoint="/echo",
        description="Echo 64-byte POST body",
        method="POST",
        body=b"x" * 64,
        headers={"Content-Type": "text/plain"},
    ),
    # I/O simulation (tests async behavior)
    "io_10ms": Scenario(name="io_10ms", endpoint="/io10", description="10ms async sleep (fast I/O simulation)"),
    "io_100ms": Scenario(name="io_100ms", endpoint="/io100", description="100ms async sleep (slow query simulation)"),
    # Static file serving
    "file": Scenario(
        name="file", endpoint="/file", description="50KB static file response", expected_response_bytes=51200
    ),
    # JSON serialization
    "json": Scenario(name="json", endpoint="/json", description="~1KB JSON response with nested structure"),
}


# =============================================================================
# Scenario Groups
# =============================================================================

# Default scenarios for standard benchmarks
DEFAULT_SCENARIOS = ["hello", "body_1k", "body_10k", "json", "echo", "io_10ms"]

# Quick benchmark scenarios (fastest to run)
QUICK_SCENARIOS = ["hello", "body_1k", "json"]

# Full benchmark scenarios (comprehensive)
FULL_SCENARIOS = list(SCENARIOS.keys())

# Response size scaling scenarios
SIZE_SCENARIOS = ["body_10b", "body_1k", "body_10k", "body_100k"]

# I/O scenarios (for async behavior testing)
IO_SCENARIOS = ["io_10ms", "io_100ms"]


# =============================================================================
# Concurrency Levels
# =============================================================================

# Default concurrency levels for standard benchmarks
DEFAULT_CONCURRENCY = [64, 256]

# Quick benchmark concurrency (single level)
QUICK_CONCURRENCY = [64]

# Full benchmark concurrency (comprehensive)
FULL_CONCURRENCY = [8, 64, 256, 512]


# =============================================================================
# Duration Settings
# =============================================================================

# Three-phase execution timing (seconds)
PRIMER_DURATION = 4  # Initial warmup to fill caches
WARMUP_DURATION = 3  # Server stabilization
MEASURE_DURATION = 10  # Actual measurement

# Quick mode durations
QUICK_PRIMER_DURATION = 2
QUICK_WARMUP_DURATION = 1
QUICK_MEASURE_DURATION = 3


def get_scenarios(names: list[str] | None = None, quick: bool = False) -> dict[str, Scenario]:
    """Get scenarios by name or default set.

    Args:
        names: Specific scenario names, or None for default
        quick: Use quick scenario set if no names specified

    Returns:
        Dictionary of scenario name to Scenario
    """
    if names is not None:
        return {name: SCENARIOS[name] for name in names if name in SCENARIOS}

    scenario_list = QUICK_SCENARIOS if quick else DEFAULT_SCENARIOS
    return {name: SCENARIOS[name] for name in scenario_list}


def get_concurrency(quick: bool = False) -> list[int]:
    """Get concurrency levels.

    Args:
        quick: Use quick (single) concurrency level

    Returns:
        List of concurrency levels
    """
    return QUICK_CONCURRENCY if quick else DEFAULT_CONCURRENCY


def get_durations(quick: bool = False) -> tuple[int, int, int]:
    """Get primer, warmup, and measure durations.

    Args:
        quick: Use quick (shorter) durations

    Returns:
        Tuple of (primer, warmup, measure) seconds
    """
    if quick:
        return (QUICK_PRIMER_DURATION, QUICK_WARMUP_DURATION, QUICK_MEASURE_DURATION)
    return (PRIMER_DURATION, WARMUP_DURATION, MEASURE_DURATION)
