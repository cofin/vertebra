"""Unit tests for tools.benchmarks.reports matrix-aware Rich tables."""

from __future__ import annotations

from typing import Any

import pytest
from rich.console import Console
from tools.benchmarks.reports import (
    _fmt_delta,
    _fmt_gil,
    _fmt_load,
    _has_matrix_dimensions,
    print_comparison_table,
    print_summary_table,
)


def _legacy_summary_row(*, stored_ms: bool = False) -> dict[str, Any]:
    """Build a legacy HTTP/1.1 benchmark summary row without matrix keys."""
    row: dict[str, Any] = {
        "target": "vertebra",
        "topology": "multi-worker",
        "scenario": "json",
        "concurrency": 64,
        "rps": 52300.0,
        "errors": 0,
    }
    if stored_ms:
        row["latency_mean_ms"] = 1.25
        row["latency_p99_ms"] = 3.45
    else:
        row["latency_mean_s"] = 0.00125
        row["latency_p99_s"] = 0.00345
    return row


def _matrix_summary_row(**overrides: Any) -> dict[str, Any]:
    """Build a matrix-enabled benchmark summary row."""
    row: dict[str, Any] = {
        "target": "vertebra",
        "topology": "multi-worker",
        "scenario": "db_read",
        "concurrency": 32,
        "rps": 14800.0,
        "latency_mean_ms": 2.1,
        "latency_p99_ms": 6.4,
        "errors": 0,
        "protocol": "grpc",
        "adapter": "postgres",
        "transport": "native",
        "broker_mode": "shm_broker",
        "load_mode": "open_loop",
        "target_rps": 2500.0,
        "gil_enabled": False,
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([], False),
        ([_legacy_summary_row()], False),
        (
            [
                {
                    "adapter": "none",
                    "protocol": "http1",
                    "transport": "native",
                    "broker_mode": "none",
                    "load_mode": "closed_loop",
                    "target_rps": None,
                    "gil_enabled": True,
                }
            ],
            False,
        ),
        ([{"adapter": "postgres"}], True),
        ([{"protocol": "http2"}], True),
        ([{"transport": "asgi"}], True),
        ([{"broker_mode": "shm_broker"}], True),
        ([{"load_mode": "open_loop"}], True),
        ([{"target_rps": 5000.0}], True),
        ([{"gil_enabled": False}], True),
    ],
)
def test_has_matrix_dimensions(rows: list[dict[str, Any]], expected: bool) -> None:
    """Verify matrix dimension detection across default and non-default row values."""
    assert _has_matrix_dimensions(rows) is expected


@pytest.mark.parametrize(
    ("pct", "expected"),
    [
        (None, "-"),
        (0.0, "+0.0%"),
        (1.5, "+1.5%"),
        (-1.9, "-1.9%"),
        (2.0, "[green]+2.0%[/green]"),
        (12.34, "[green]+12.3%[/green]"),
        (-2.0, "[red]-2.0%[/red]"),
        (-8.76, "[red]-8.8%[/red]"),
    ],
)
def test_fmt_delta(pct: float | None, expected: str) -> None:
    """Verify delta percentage formatting for None, neutral, improvement, and regression."""
    assert _fmt_delta(pct) == expected


@pytest.mark.parametrize(
    ("load_mode", "target_rps", "expected"),
    [
        ("closed_loop", None, "closed"),
        ("closed_loop", 1000.0, "closed"),
        ("open_loop", None, "open"),
        ("open_loop", 2500.0, "open@2,500"),
    ],
)
def test_fmt_load(load_mode: str, target_rps: float | None, expected: str) -> None:
    """Verify load mode formatting for closed-loop and open-loop targets."""
    assert _fmt_load(load_mode, target_rps) == expected


@pytest.mark.parametrize(
    ("gil_enabled", "expected"),
    [
        (False, "off"),
        (True, "on"),
        (None, "-"),
    ],
)
def test_fmt_gil(gil_enabled: bool | None, expected: str) -> None:
    """Verify GIL state formatting for False, True, and None."""
    assert _fmt_gil(gil_enabled) == expected


@pytest.mark.parametrize("stored_ms", [False, True])
def test_print_summary_table_legacy_rows_omit_matrix_columns(stored_ms: bool) -> None:
    """Verify legacy HTTP/1.1 summary rows omit matrix columns and format latencies."""
    recorded = Console(record=True, width=200)
    print_summary_table([_legacy_summary_row(stored_ms=stored_ms)], output_console=recorded)
    text = recorded.export_text()

    assert "Target" in text
    assert "Topology" in text
    assert "Scenario" in text
    assert "52,300" in text
    assert "1.25" in text
    assert "3.45" in text
    for header in ("Protocol", "Adapter", "Broker", "Load", "GIL"):
        assert header not in text


def test_print_summary_table_matrix_rows_auto_enable_columns() -> None:
    """Verify matrix summary rows auto-enable matrix columns and render formatted values."""
    recorded = Console(record=True, width=200)
    print_summary_table([_matrix_summary_row()], output_console=recorded)
    text = recorded.export_text()

    for expected in (
        "Protocol",
        "Adapter",
        "Broker",
        "Load",
        "GIL",
        "grpc",
        "postgres",
        "shm_broker",
        "open@2,500",
        "off",
    ):
        assert expected in text


def test_print_comparison_table_matrix_and_none_values() -> None:
    """Verify comparison table renders matrix columns and formats None values as dashes."""
    rows: list[dict[str, Any]] = [
        {
            "target": "vertebra",
            "scenario": "db_read",
            "concurrency": 32,
            "protocol": "http2",
            "adapter": "postgres",
            "broker_mode": "shm_broker",
            "load_mode": "open_loop",
            "target_rps": 5000.0,
            "current_rps": 48000.0,
            "baseline_rps": 40000.0,
            "rps_delta_pct": 20.0,
            "current_p99_ms": 1.8,
            "baseline_p99_ms": 2.0,
            "p99_delta_pct": -10.0,
        },
        {
            "target": "vertebra",
            "scenario": "db_write",
            "concurrency": 32,
            "protocol": "grpc",
            "adapter": "redis",
            "broker_mode": "per_worker",
            "load_mode": "closed_loop",
            "current_rps": None,
            "baseline_rps": None,
            "rps_delta_pct": None,
            "current_p99_ms": None,
            "baseline_p99_ms": None,
            "p99_delta_pct": None,
        },
    ]

    recorded = Console(record=True, width=220)
    print_comparison_table(rows, "current", "baseline", output_console=recorded)
    text = recorded.export_text()

    for expected in (
        "Protocol",
        "Adapter",
        "Broker",
        "Load",
        "http2",
        "postgres",
        "shm_broker",
        "open@5,000",
        "48,000",
        "40,000",
        "+20.0%",
        "+10.0%",
        "grpc",
        "redis",
        "per_worker",
        "closed",
        "-",
    ):
        assert expected in text


@pytest.mark.parametrize(
    ("show_matrix", "use_matrix_row", "expect_matrix"),
    [
        (True, False, True),
        (False, True, False),
    ],
)
def test_show_matrix_override_on_summary_and_comparison_tables(
    show_matrix: bool,
    use_matrix_row: bool,
    expect_matrix: bool,
) -> None:
    """Verify explicit show_matrix overrides auto-detection on both tables."""
    summary_rows = [_matrix_summary_row()] if use_matrix_row else [_legacy_summary_row()]
    summary_console = Console(record=True, width=220)
    print_summary_table(summary_rows, show_matrix=show_matrix, output_console=summary_console)
    summary_text = summary_console.export_text()

    for header in ("Protocol", "Adapter", "Broker", "Load", "GIL"):
        assert (header in summary_text) is expect_matrix

    comparison_row: dict[str, Any] = {
        "target": "vertebra",
        "scenario": "json",
        "concurrency": 64,
        "current_rps": 50000.0,
        "baseline_rps": 45000.0,
        "rps_delta_pct": 11.1,
        "current_p99_ms": 2.0,
        "baseline_p99_ms": 2.2,
        "p99_delta_pct": -9.1,
    }
    if use_matrix_row:
        comparison_row["protocol"] = "grpc"
        comparison_row["adapter"] = "postgres"
        comparison_row["broker_mode"] = "shm_broker"
        comparison_row["load_mode"] = "open_loop"

    comparison_console = Console(record=True, width=220)
    print_comparison_table(
        [comparison_row],
        "cur",
        "base",
        show_matrix=show_matrix,
        output_console=comparison_console,
    )
    comparison_text = comparison_console.export_text()

    for header in ("Protocol", "Adapter", "Broker", "Load"):
        assert (header in comparison_text) is expect_matrix
