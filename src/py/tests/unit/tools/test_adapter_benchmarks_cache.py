"""Unit tests for Redis and Valkey standalone adapter benchmark comparators."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tools.benchmarks.adapters import base as adapter_base
from tools.benchmarks.adapters import cache as cache_bench
from tools.benchmarks.adapters.base import (
    ADAPTER_SCHEMA_VERSION,
    AdapterBenchmarkCase,
    AdapterBenchmarkReport,
    DriverMode,
    format_adapter_report_bytes,
    redact_target_uri,
)
from tools.benchmarks.adapters.cache import (
    DEFAULT_PIPELINE_SIZES,
    CacheWorkloadSpec,
    build_cache_keys,
    build_pipeline_get_commands,
    capture_cache_report,
    main,
    resolve_redis_py_driver_label,
)


@pytest.mark.parametrize("batch_size", DEFAULT_PIPELINE_SIZES)
def test_cache_workload_builders_and_validation(batch_size: int) -> None:
    """Verify deterministic key/pipeline builders and validation errors for invalid inputs."""
    keys = build_cache_keys("bench:cache", batch_size)
    assert len(keys) == batch_size
    assert keys[0] == "bench:cache:0"
    assert keys[-1] == f"bench:cache:{batch_size - 1}"

    commands = build_pipeline_get_commands("bench:cache", batch_size)
    assert len(commands) == batch_size
    assert commands == tuple(("GET", key) for key in keys)

    with pytest.raises(ValueError, match="count must be >= 1"):
        build_cache_keys("bench:cache", 0)
    with pytest.raises(ValueError, match="key_prefix must be a non-empty string"):
        build_cache_keys("", batch_size)
    with pytest.raises(ValueError, match="key_prefix must be a non-empty string"):
        build_pipeline_get_commands("   ", batch_size)


@pytest.mark.parametrize(
    ("is_async", "hiredis_installed", "expected"),
    [
        (True, True, "redis-py-async-hiredis"),
        (True, False, "redis-py-async"),
        (False, True, "redis-py-sync-hiredis"),
        (False, False, "redis-py-sync"),
    ],
)
def test_resolve_redis_py_driver_label_variants(
    is_async: bool,
    hiredis_installed: bool,
    expected: str,
) -> None:
    """Verify all four sync/async x hiredis redis-py driver label variants."""
    assert resolve_redis_py_driver_label(is_async=is_async, hiredis_installed=hiredis_installed) == expected


@pytest.mark.parametrize(
    ("is_async", "module_obj", "expected"),
    [
        (True, object(), "redis-py-async-hiredis"),
        (True, None, "redis-py-async"),
        (False, object(), "redis-py-sync-hiredis"),
        (False, None, "redis-py-sync"),
    ],
)
def test_resolve_redis_py_driver_label_probes_optional_hiredis(
    monkeypatch: pytest.MonkeyPatch,
    is_async: bool,
    module_obj: object | None,
    expected: str,
) -> None:
    """Verify hiredis auto-detection when hiredis_installed is omitted."""
    monkeypatch.setattr(cache_bench, "import_optional", lambda name: module_obj if name == "hiredis" else None)
    assert resolve_redis_py_driver_label(is_async=is_async) == expected


@pytest.mark.anyio
async def test_capture_cache_report_strict_free_threading_raises_when_gil_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify strict free-threading aborts before any network call when the GIL is active."""
    monkeypatch.setattr(adapter_base, "detect_gil_enabled", lambda: True)

    def _fail_if_called(*_args: object, **_kwargs: object) -> object:
        msg = "Network driver constructor must not be invoked when GIL check fails"
        raise AssertionError(msg)

    monkeypatch.setattr(cache_bench, "_create_sync_driver", _fail_if_called)
    monkeypatch.setattr(cache_bench, "_create_async_driver", _fail_if_called)

    with pytest.raises(RuntimeError, match="Free-threaded benchmark requires GIL disabled"):
        await capture_cache_report("redis://127.0.0.1:6379/0", strict_free_threading=True)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"iterations": 0}, "iterations must be >= 1"),
        ({"warmup": -1}, "warmup must be >= 0"),
        ({"spec": CacheWorkloadSpec(payload_bytes=0)}, "payload_bytes must be >= 1"),
        ({"spec": CacheWorkloadSpec(threads=0)}, "threads must be >= 1"),
        ({"engine": "memcached"}, "engine must be 'redis' or 'valkey'"),
    ],
)
async def test_capture_cache_report_rejects_invalid_inputs(
    kwargs: dict[str, Any],
    match: str,
) -> None:
    """Verify capture_cache_report validates arguments prior to GIL or driver setup."""
    with pytest.raises(ValueError, match=match):
        await capture_cache_report("redis://127.0.0.1:6379/0", **kwargs)


@pytest.mark.parametrize(
    ("driver_mode", "candidate_driver", "baseline_driver", "candidate_ops", "candidate_p99", "expected_rc"),
    [
        ("async_native", "vertebra-valkey-async", "redis-py-async-hiredis", 12000.0, 0.40, 0),
        ("async_native", "vertebra-valkey-async", "redis-py-async-hiredis", 5000.0, 0.90, 1),
        ("sync_blocking", "vertebra-redis-sync", "redis-py-sync", 9500.0, 0.55, 0),
        ("sync_blocking", "vertebra-redis-sync", "redis-py-sync", 4000.0, 1.20, 1),
    ],
)
def test_cache_report_stratified_comparison_and_cli_compare(
    tmp_path: Path,
    driver_mode: DriverMode,
    candidate_driver: str,
    baseline_driver: str,
    candidate_ops: float,
    candidate_p99: float,
    expected_rc: int,
) -> None:
    """Verify stratified CLI comparison exit codes and credential redaction for cache reports."""
    redacted = redact_target_uri("valkey://:test-token-placeholder@127.0.0.1:6379/0")
    assert "test-token-placeholder" not in redacted
    assert redacted == "valkey://127.0.0.1:6379/0"

    secret_redacted = redact_target_uri("valkey://:secret@127.0.0.1:6379/0")
    assert "secret" not in secret_redacted
    assert secret_redacted == "valkey://127.0.0.1:6379/0"

    engine = "valkey" if "valkey" in candidate_driver else "redis"
    candidate_report = AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="cache",
        engine=engine,
        generated_at="2026-11-19T00:00:00+00:00",
        target_redacted=redacted,
        gil_enabled=False,
        cases=(
            AdapterBenchmarkCase(
                case="get_set",
                driver=candidate_driver,
                category="cache",
                driver_mode=driver_mode,
                iterations=100,
                median_ms=0.20,
                p99_ms=candidate_p99,
                ops_per_sec=candidate_ops,
                gil_enabled=False,
            ),
            AdapterBenchmarkCase(
                case="mget_pipeline_10",
                driver=candidate_driver,
                category="cache",
                driver_mode=driver_mode,
                iterations=100,
                median_ms=0.25,
                p99_ms=candidate_p99,
                ops_per_sec=candidate_ops,
                gil_enabled=False,
            ),
        ),
    )
    baseline_report = AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="cache",
        engine=engine,
        generated_at="2026-11-19T00:00:00+00:00",
        target_redacted=redacted,
        gil_enabled=False,
        cases=(
            AdapterBenchmarkCase(
                case="get_set",
                driver=baseline_driver,
                category="cache",
                driver_mode=driver_mode,
                iterations=100,
                median_ms=0.22,
                p99_ms=0.50,
                ops_per_sec=10000.0,
                gil_enabled=False,
            ),
            AdapterBenchmarkCase(
                case="mget_pipeline_10",
                driver=baseline_driver,
                category="cache",
                driver_mode=driver_mode,
                iterations=100,
                median_ms=0.28,
                p99_ms=0.50,
                ops_per_sec=10000.0,
                gil_enabled=False,
            ),
        ),
    )

    candidate_path = tmp_path / "candidate.json"
    baseline_path = tmp_path / "baseline.json"
    candidate_path.write_bytes(format_adapter_report_bytes(candidate_report))
    baseline_path.write_bytes(format_adapter_report_bytes(baseline_report))

    rc = main(
        [
            "compare",
            "--candidate",
            str(candidate_path),
            "--baseline",
            str(baseline_path),
            "--candidate-driver",
            candidate_driver,
            "--baseline-driver",
            baseline_driver,
            "--driver-mode",
            driver_mode,
        ]
    )
    assert rc == expected_rc
