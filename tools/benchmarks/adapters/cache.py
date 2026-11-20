"""Redis and Valkey standalone adapter benchmark comparators."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, Literal, cast

import msgspec

from tools.benchmarks.adapters.base import (
    ADAPTER_SCHEMA_VERSION,
    DEFAULT_MAX_LATENCY_RATIO,
    DEFAULT_MIN_THROUGHPUT_RATIO,
    AdapterBenchmarkCase,
    AdapterBenchmarkReport,
    DriverMode,
    assert_free_threaded_gil_disabled,
    compare_adapter_reports,
    compute_adapter_case_stats,
    format_adapter_report_bytes,
    parse_adapter_report_bytes,
    redact_target_uri,
    sample_async,
    sample_sync,
    sample_threaded,
)
from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig, RedisNotifyConfig, SyncRedisDriver
from vertebra.db.adapters.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig
from vertebra.utils.module_loader import import_optional

DEFAULT_PIPELINE_SIZES: Final[tuple[int, ...]] = (10, 50)

type CacheEngine = Literal["redis", "valkey"]

__all__ = (
    "DEFAULT_PIPELINE_SIZES",
    "CacheEngine",
    "CacheWorkloadSpec",
    "build_cache_keys",
    "build_pipeline_get_commands",
    "capture_cache_report",
    "main",
    "resolve_redis_py_driver_label",
)


class CacheWorkloadSpec(msgspec.Struct, frozen=True, gc=False):
    """Workload parameters for Redis and Valkey standalone adapter benchmarks."""

    key_prefix: str = "bench:cache"
    payload_bytes: int = 128
    pipeline_sizes: tuple[int, ...] = DEFAULT_PIPELINE_SIZES
    pubsub_channel: str = "bench:pubsub"
    threads: int = 1


def build_cache_keys(key_prefix: str, count: int) -> tuple[str, ...]:
    """Build deterministic cache keys for single-key and pipeline workloads."""
    if not key_prefix or not key_prefix.strip():
        msg = "key_prefix must be a non-empty string"
        raise ValueError(msg)
    if count < 1:
        msg = f"count must be >= 1 (got {count})"
        raise ValueError(msg)
    return tuple(f"{key_prefix}:{index}" for index in range(count))


def build_pipeline_get_commands(key_prefix: str, batch_size: int) -> tuple[tuple[str, str], ...]:
    """Build deterministic pipeline GET command tuples for a batch size."""
    return tuple(("GET", key) for key in build_cache_keys(key_prefix, batch_size))


def resolve_redis_py_driver_label(*, is_async: bool, hiredis_installed: bool | None = None) -> str:
    """Resolve the redis-py driver label reflecting sync/async mode and hiredis availability."""
    has_hiredis = import_optional("hiredis") is not None if hiredis_installed is None else hiredis_installed
    mode = "async" if is_async else "sync"
    suffix = "-hiredis" if has_hiredis else ""
    return f"redis-py-{mode}{suffix}"


async def capture_cache_report(
    url: str,
    *,
    engine: CacheEngine = "redis",
    spec: CacheWorkloadSpec | None = None,
    iterations: int = 100,
    warmup: int = 10,
    include_pubsub: bool = True,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Capture standalone Redis or Valkey adapter benchmark metrics and optional redis-py baselines."""
    resolved_spec = spec or CacheWorkloadSpec()
    _validate_capture_inputs(
        engine=engine,
        spec=resolved_spec,
        iterations=iterations,
        warmup=warmup,
        include_pubsub=include_pubsub,
    )
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)

    payload = b"x" * resolved_spec.payload_bytes
    cases: list[AdapterBenchmarkCase] = []

    cases.extend(
        _measure_vertebra_sync(
            url,
            engine=engine,
            spec=resolved_spec,
            payload=payload,
            iterations=iterations,
            warmup=warmup,
            include_pubsub=include_pubsub,
            gil_enabled=gil_enabled,
        )
    )
    cases.extend(
        await _measure_vertebra_async(
            url,
            engine=engine,
            spec=resolved_spec,
            payload=payload,
            iterations=iterations,
            warmup=warmup,
            include_pubsub=include_pubsub,
            gil_enabled=gil_enabled,
        )
    )

    if include_external:
        redis_mod = import_optional("redis")
        redis_asyncio_mod = import_optional("redis.asyncio") if redis_mod is not None else None
        hiredis_installed = import_optional("hiredis") is not None
        gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
        if redis_mod is not None:
            cases.extend(
                _measure_redis_py_sync(
                    redis_mod,
                    url,
                    spec=resolved_spec,
                    payload=payload,
                    iterations=iterations,
                    warmup=warmup,
                    include_pubsub=include_pubsub,
                    hiredis_installed=hiredis_installed,
                    gil_enabled=gil_enabled,
                )
            )
        if redis_asyncio_mod is not None:
            cases.extend(
                await _measure_redis_py_async(
                    redis_asyncio_mod,
                    url,
                    spec=resolved_spec,
                    payload=payload,
                    iterations=iterations,
                    warmup=warmup,
                    include_pubsub=include_pubsub,
                    hiredis_installed=hiredis_installed,
                    gil_enabled=gil_enabled,
                )
            )

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="cache",
        engine=engine,
        generated_at=datetime.now(UTC).isoformat(),
        target_redacted=redact_target_uri(url),
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for capturing and comparing Redis/Valkey standalone adapter benchmarks."""
    parser = argparse.ArgumentParser(
        prog="tools.benchmarks.adapters.cache",
        description="Capture and compare Redis/Valkey standalone adapter benchmark reports.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture_cmd = subparsers.add_parser("capture", help="Capture a live cache adapter benchmark report.")
    capture_cmd.add_argument("--engine", choices=("redis", "valkey"), default="redis", help="Target cache engine.")
    capture_cmd.add_argument("--url", required=True, help="Redis or Valkey connection URL.")
    capture_cmd.add_argument("--iterations", type=int, default=100, help="Measured iterations per workload.")
    capture_cmd.add_argument("--warmup", type=int, default=10, help="Warmup iterations per workload.")
    capture_cmd.add_argument("--threads", type=int, default=1, help="Thread count for free-threaded scaling.")
    capture_cmd.add_argument(
        "--pipeline-sizes",
        default="10,50",
        help="Comma-separated pipeline batch sizes.",
    )
    capture_cmd.add_argument("--no-pubsub", action="store_true", help="Skip Pub/Sub fanout benchmarks.")
    capture_cmd.add_argument("--no-external", action="store_true", help="Skip optional redis-py comparators.")
    capture_cmd.add_argument(
        "--strict-free-threading",
        action="store_true",
        help="Fail immediately if the Python GIL is active or re-enabled.",
    )
    capture_cmd.add_argument("--output", type=Path, default=None, help="Optional output path for JSON report.")

    compare_cmd = subparsers.add_parser("compare", help="Compare candidate and baseline cache benchmark reports.")
    compare_cmd.add_argument("--candidate", type=Path, required=True, help="Candidate JSON report path.")
    compare_cmd.add_argument("--baseline", type=Path, required=True, help="Baseline JSON report path.")
    compare_cmd.add_argument("--candidate-driver", default=None, help="Candidate driver label within report.")
    compare_cmd.add_argument("--baseline-driver", default=None, help="Baseline driver label within report.")
    compare_cmd.add_argument(
        "--driver-mode",
        choices=("async_native", "sync_blocking"),
        default=None,
        help="Optional driver mode filter.",
    )
    compare_cmd.add_argument(
        "--min-throughput-ratio",
        type=float,
        default=DEFAULT_MIN_THROUGHPUT_RATIO,
        help="Minimum allowed candidate/baseline ops/sec ratio.",
    )
    compare_cmd.add_argument(
        "--max-latency-ratio",
        type=float,
        default=DEFAULT_MAX_LATENCY_RATIO,
        help="Maximum allowed candidate/baseline p99 latency ratio.",
    )

    args = parser.parse_args(argv)
    if args.command == "capture":
        pipeline_sizes = _parse_pipeline_sizes(args.pipeline_sizes)
        spec = CacheWorkloadSpec(pipeline_sizes=pipeline_sizes, threads=args.threads)
        report = asyncio.run(
            capture_cache_report(
                args.url,
                engine=cast("CacheEngine", args.engine),
                spec=spec,
                iterations=args.iterations,
                warmup=args.warmup,
                include_pubsub=not args.no_pubsub,
                include_external=not args.no_external,
                strict_free_threading=args.strict_free_threading,
            )
        )
        encoded = format_adapter_report_bytes(report)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_bytes(encoded + b"\n")
        else:
            sys.stdout.write(encoded.decode("utf-8") + "\n")
        return 0

    candidate_report = parse_adapter_report_bytes(args.candidate.read_bytes())
    baseline_report = parse_adapter_report_bytes(args.baseline.read_bytes())
    outcomes = compare_adapter_reports(
        candidate_report,
        baseline_report,
        candidate_driver=args.candidate_driver,
        baseline_driver=args.baseline_driver,
        driver_mode=cast("DriverMode | None", args.driver_mode),
        min_throughput_ratio=args.min_throughput_ratio,
        max_latency_ratio=args.max_latency_ratio,
    )
    failures = [outcome for outcome in outcomes if not outcome.passed]
    for outcome in outcomes:
        status = "PASS" if outcome.passed else "FAIL"
        line = (
            f"[{status}] {outcome.case} ({outcome.candidate_driver} vs {outcome.baseline_driver}): "
            f"throughput_ratio={outcome.throughput_ratio:.3f}, p99_ratio={outcome.p99_ratio:.3f}"
        )
        if outcome.reason:
            line += f" ({outcome.reason})"
        sys.stdout.write(line + "\n")
    return 1 if failures else 0


def _validate_capture_inputs(
    *,
    engine: str,
    spec: CacheWorkloadSpec,
    iterations: int,
    warmup: int,
    include_pubsub: bool,
) -> None:
    if engine not in {"redis", "valkey"}:
        msg = f"engine must be 'redis' or 'valkey' (got {engine!r})"
        raise ValueError(msg)
    if iterations < 1:
        msg = f"iterations must be >= 1 (got {iterations})"
        raise ValueError(msg)
    if warmup < 0:
        msg = f"warmup must be >= 0 (got {warmup})"
        raise ValueError(msg)
    if spec.payload_bytes < 1:
        msg = f"spec.payload_bytes must be >= 1 (got {spec.payload_bytes})"
        raise ValueError(msg)
    if spec.threads < 1:
        msg = f"spec.threads must be >= 1 (got {spec.threads})"
        raise ValueError(msg)
    if not spec.key_prefix or not spec.key_prefix.strip():
        msg = "spec.key_prefix must be a non-empty string"
        raise ValueError(msg)
    if include_pubsub and (not spec.pubsub_channel or not spec.pubsub_channel.strip()):
        msg = "spec.pubsub_channel must be a non-empty string when include_pubsub=True"
        raise ValueError(msg)
    for batch_size in spec.pipeline_sizes:
        if batch_size < 1:
            msg = f"All spec.pipeline_sizes entries must be >= 1 (got {batch_size})"
            raise ValueError(msg)


def _parse_pipeline_sizes(raw: str) -> tuple[int, ...]:
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if not parts:
        msg = "pipeline-sizes must include at least one positive integer"
        raise ValueError(msg)
    sizes = tuple(int(part) for part in parts)
    if any(size < 1 for size in sizes):
        msg = f"All pipeline sizes must be >= 1 (got {sizes})"
        raise ValueError(msg)
    return sizes


def _create_sync_driver(url: str, engine: CacheEngine) -> SyncRedisDriver | SyncValkeyDriver:
    if engine == "redis":
        return SyncRedisDriver(RedisConfig.from_url(url))
    return SyncValkeyDriver(ValkeyConfig.from_url(url))


def _create_async_driver(url: str, engine: CacheEngine) -> AsyncRedisDriver | AsyncValkeyDriver:
    if engine == "redis":
        return AsyncRedisDriver(RedisConfig.from_url(url))
    return AsyncValkeyDriver(ValkeyConfig.from_url(url))


def _measure_vertebra_sync(
    url: str,
    *,
    engine: CacheEngine,
    spec: CacheWorkloadSpec,
    payload: bytes,
    iterations: int,
    warmup: int,
    include_pubsub: bool,
    gil_enabled: bool,
) -> list[AdapterBenchmarkCase]:
    driver_label = f"vertebra-{engine}-sync"
    driver = _create_sync_driver(url, engine)
    results: list[AdapterBenchmarkCase] = []
    try:
        single_key = build_cache_keys(spec.key_prefix, 1)[0]

        def _get_set() -> bytes | None:
            driver.set(single_key, payload)
            return driver.get(single_key)

        results.append(
            compute_adapter_case_stats(
                case="get_set",
                driver=driver_label,
                category="cache",
                driver_mode="sync_blocking",
                latencies_ns=sample_sync(_get_set, iterations=iterations, warmup=warmup),
                gil_enabled=gil_enabled,
            )
        )

        for batch_size in spec.pipeline_sizes:
            keys = build_cache_keys(spec.key_prefix, batch_size)
            for key in keys:
                driver.set(key, payload)
            commands = build_pipeline_get_commands(spec.key_prefix, batch_size)

            def _run_pipe(cmds: tuple[tuple[str, str], ...] = commands) -> list[Any]:
                return driver.execute_pipeline(cmds)

            samples = sample_sync(
                _run_pipe,
                iterations=iterations,
                warmup=warmup,
            )
            results.append(
                compute_adapter_case_stats(
                    case=f"mget_pipeline_{batch_size}",
                    driver=driver_label,
                    category="cache",
                    driver_mode="sync_blocking",
                    latencies_ns=samples,
                    gil_enabled=gil_enabled,
                )
            )

        if include_pubsub:
            driver.subscribe(spec.pubsub_channel)
            try:

                def _pubsub_roundtrip() -> int:
                    driver.publish(spec.pubsub_channel, payload)
                    return len(driver.drain_messages())

                results.append(
                    compute_adapter_case_stats(
                        case="pubsub_fanout",
                        driver=driver_label,
                        category="cache",
                        driver_mode="sync_blocking",
                        latencies_ns=sample_sync(_pubsub_roundtrip, iterations=iterations, warmup=warmup),
                        gil_enabled=gil_enabled,
                    )
                )
            finally:
                with contextlib.suppress(Exception):
                    driver.unsubscribe(spec.pubsub_channel)

        if spec.threads > 1:
            threaded_samples = sample_threaded(
                _get_set,
                iterations=iterations,
                warmup=warmup,
                threads=spec.threads,
            )
            results.append(
                compute_adapter_case_stats(
                    case=f"threaded_get_set_{spec.threads}t",
                    driver=driver_label,
                    category="cache",
                    driver_mode="sync_blocking",
                    latencies_ns=threaded_samples,
                    threads=spec.threads,
                    gil_enabled=gil_enabled,
                )
            )
    finally:
        driver.close()
    return results


async def _measure_vertebra_async(
    url: str,
    *,
    engine: CacheEngine,
    spec: CacheWorkloadSpec,
    payload: bytes,
    iterations: int,
    warmup: int,
    include_pubsub: bool,
    gil_enabled: bool,
) -> list[AdapterBenchmarkCase]:
    driver_label = f"vertebra-{engine}-async"
    driver = _create_async_driver(url, engine)
    results: list[AdapterBenchmarkCase] = []
    try:
        single_key = build_cache_keys(spec.key_prefix, 1)[0]

        async def _get_set() -> bytes | None:
            await driver.set(single_key, payload)
            return await driver.get(single_key)

        results.append(
            compute_adapter_case_stats(
                case="get_set",
                driver=driver_label,
                category="cache",
                driver_mode="async_native",
                latencies_ns=await sample_async(_get_set, iterations=iterations, warmup=warmup),
                gil_enabled=gil_enabled,
            )
        )

        for batch_size in spec.pipeline_sizes:
            keys = build_cache_keys(spec.key_prefix, batch_size)
            for key in keys:
                await driver.set(key, payload)
            commands = build_pipeline_get_commands(spec.key_prefix, batch_size)

            async def _run_pipe(cmds: tuple[tuple[str, str], ...] = commands) -> list[Any]:
                return await driver.execute_pipeline(cmds)

            results.append(
                compute_adapter_case_stats(
                    case=f"mget_pipeline_{batch_size}",
                    driver=driver_label,
                    category="cache",
                    driver_mode="async_native",
                    latencies_ns=await sample_async(_run_pipe, iterations=iterations, warmup=warmup),
                    gil_enabled=gil_enabled,
                )
            )

        if include_pubsub:
            pubsub_samples = await _measure_vertebra_async_pubsub(
                driver,
                channel=spec.pubsub_channel,
                payload=payload,
                iterations=iterations,
                warmup=warmup,
            )
            results.append(
                compute_adapter_case_stats(
                    case="pubsub_fanout",
                    driver=driver_label,
                    category="cache",
                    driver_mode="async_native",
                    latencies_ns=pubsub_samples,
                    gil_enabled=gil_enabled,
                )
            )
    finally:
        await driver.close()
    return results


async def _measure_vertebra_async_pubsub(
    driver: AsyncRedisDriver | AsyncValkeyDriver,
    *,
    channel: str,
    payload: bytes,
    iterations: int,
    warmup: int,
) -> list[int]:
    if isinstance(driver, AsyncRedisDriver):
        received: asyncio.Queue[Any] = asyncio.Queue()
        source = driver.notification_source(RedisNotifyConfig(channels=(channel,), timeout_ms=25))
        async with source:

            async def _consume() -> None:
                async for event in source.listen():
                    received.put_nowait(event)

            consumer = asyncio.create_task(_consume())
            await asyncio.sleep(0.02)
            try:

                async def _redis_pubsub_step() -> Any:
                    await driver.publish(channel, payload)
                    return await asyncio.wait_for(received.get(), timeout=1.0)

                return await sample_async(_redis_pubsub_step, iterations=iterations, warmup=warmup)
            finally:
                consumer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await consumer

    await driver.subscribe(channel)
    try:

        async def _valkey_pubsub_step() -> int:
            await driver.publish(channel, payload)
            return len(driver.drain_messages())

        return await sample_async(_valkey_pubsub_step, iterations=iterations, warmup=warmup)
    finally:
        with contextlib.suppress(Exception):
            await driver.unsubscribe(channel)


def _normalize_redis_py_url(url: str) -> str:
    """Normalize ``valkey://`` and ``valkeys://`` URLs to schemes accepted by ``redis-py``."""
    if url.startswith("valkeys://"):
        return f"rediss://{url[len('valkeys://') :]}"
    if url.startswith("valkey://"):
        return f"redis://{url[len('valkey://') :]}"
    return url


def _measure_redis_py_sync(
    redis_mod: Any,
    url: str,
    *,
    spec: CacheWorkloadSpec,
    payload: bytes,
    iterations: int,
    warmup: int,
    include_pubsub: bool,
    hiredis_installed: bool,
    gil_enabled: bool,
) -> list[AdapterBenchmarkCase]:
    driver_label = resolve_redis_py_driver_label(is_async=False, hiredis_installed=hiredis_installed)
    client = redis_mod.Redis.from_url(_normalize_redis_py_url(url))
    results: list[AdapterBenchmarkCase] = []
    try:
        single_key = build_cache_keys(spec.key_prefix, 1)[0]

        def _get_set() -> Any:
            client.set(single_key, payload)
            return client.get(single_key)

        results.append(
            compute_adapter_case_stats(
                case="get_set",
                driver=driver_label,
                category="cache",
                driver_mode="sync_blocking",
                latencies_ns=sample_sync(_get_set, iterations=iterations, warmup=warmup),
                gil_enabled=gil_enabled,
            )
        )

        for batch_size in spec.pipeline_sizes:
            keys = build_cache_keys(spec.key_prefix, batch_size)
            for key in keys:
                client.set(key, payload)

            def _run_pipe(batch_keys: tuple[str, ...] = keys) -> Any:
                pipe = client.pipeline(transaction=False)
                for key in batch_keys:
                    pipe.get(key)
                return pipe.execute()

            results.append(
                compute_adapter_case_stats(
                    case=f"mget_pipeline_{batch_size}",
                    driver=driver_label,
                    category="cache",
                    driver_mode="sync_blocking",
                    latencies_ns=sample_sync(_run_pipe, iterations=iterations, warmup=warmup),
                    gil_enabled=gil_enabled,
                )
            )

        if include_pubsub:
            pubsub = client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(spec.pubsub_channel)
            try:

                def _pubsub_step() -> Any:
                    client.publish(spec.pubsub_channel, payload)
                    return pubsub.get_message(timeout=0.1)

                results.append(
                    compute_adapter_case_stats(
                        case="pubsub_fanout",
                        driver=driver_label,
                        category="cache",
                        driver_mode="sync_blocking",
                        latencies_ns=sample_sync(_pubsub_step, iterations=iterations, warmup=warmup),
                        gil_enabled=gil_enabled,
                    )
                )
            finally:
                with contextlib.suppress(Exception):
                    pubsub.unsubscribe(spec.pubsub_channel)
                with contextlib.suppress(Exception):
                    pubsub.close()

        if spec.threads > 1:
            results.append(
                compute_adapter_case_stats(
                    case=f"threaded_get_set_{spec.threads}t",
                    driver=driver_label,
                    category="cache",
                    driver_mode="sync_blocking",
                    latencies_ns=sample_threaded(_get_set, iterations=iterations, warmup=warmup, threads=spec.threads),
                    threads=spec.threads,
                    gil_enabled=gil_enabled,
                )
            )
    finally:
        client.close()
    return results


async def _measure_redis_py_async(
    redis_asyncio_mod: Any,
    url: str,
    *,
    spec: CacheWorkloadSpec,
    payload: bytes,
    iterations: int,
    warmup: int,
    include_pubsub: bool,
    hiredis_installed: bool,
    gil_enabled: bool,
) -> list[AdapterBenchmarkCase]:
    driver_label = resolve_redis_py_driver_label(is_async=True, hiredis_installed=hiredis_installed)
    client = redis_asyncio_mod.Redis.from_url(_normalize_redis_py_url(url))
    results: list[AdapterBenchmarkCase] = []
    try:
        single_key = build_cache_keys(spec.key_prefix, 1)[0]

        async def _get_set() -> Any:
            await client.set(single_key, payload)
            return await client.get(single_key)

        results.append(
            compute_adapter_case_stats(
                case="get_set",
                driver=driver_label,
                category="cache",
                driver_mode="async_native",
                latencies_ns=await sample_async(_get_set, iterations=iterations, warmup=warmup),
                gil_enabled=gil_enabled,
            )
        )

        for batch_size in spec.pipeline_sizes:
            keys = build_cache_keys(spec.key_prefix, batch_size)
            for key in keys:
                await client.set(key, payload)

            async def _run_pipe(batch_keys: tuple[str, ...] = keys) -> Any:
                pipe = client.pipeline(transaction=False)
                for key in batch_keys:
                    pipe.get(key)
                return await pipe.execute()

            results.append(
                compute_adapter_case_stats(
                    case=f"mget_pipeline_{batch_size}",
                    driver=driver_label,
                    category="cache",
                    driver_mode="async_native",
                    latencies_ns=await sample_async(_run_pipe, iterations=iterations, warmup=warmup),
                    gil_enabled=gil_enabled,
                )
            )

        if include_pubsub:
            pubsub = client.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe(spec.pubsub_channel)
            try:

                async def _pubsub_step() -> Any:
                    await client.publish(spec.pubsub_channel, payload)
                    return await pubsub.get_message(timeout=0.1)

                results.append(
                    compute_adapter_case_stats(
                        case="pubsub_fanout",
                        driver=driver_label,
                        category="cache",
                        driver_mode="async_native",
                        latencies_ns=await sample_async(_pubsub_step, iterations=iterations, warmup=warmup),
                        gil_enabled=gil_enabled,
                    )
                )
            finally:
                with contextlib.suppress(Exception):
                    await pubsub.unsubscribe(spec.pubsub_channel)
                with contextlib.suppress(Exception):
                    await pubsub.aclose()
    finally:
        await client.aclose()
    return results


if __name__ == "__main__":
    raise SystemExit(main())
