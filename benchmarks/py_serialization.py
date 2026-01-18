from __future__ import annotations

import argparse
import statistics
import time
from typing import Callable

from vertebra import BufferPool
from vertebra.ipc.channel import BufferView


def bench(label: str, fn: Callable[[], None], iterations: int) -> None:
    samples: list[float] = []
    for _ in range(5):
        start = time.perf_counter()
        for _ in range(iterations):
            fn()
        elapsed = time.perf_counter() - start
        samples.append(elapsed)
    best = min(samples)
    mean = statistics.mean(samples)
    ops = iterations / best if best > 0 else 0.0
    print(f"{label:<32} best={best:.6f}s mean={mean:.6f}s ops/s={ops:,.0f}")


def run_msgspec(size: int, iterations: int) -> None:
    try:
        import msgspec
    except Exception:
        print("msgspec not available; skipping msgspec benchmarks")
        return

    payload = {"id": 1, "data": "x" * size}
    encoded = msgspec.json.encode(payload)
    decoder = msgspec.json.Decoder(dict)
    view = memoryview(encoded)

    pool = BufferPool(len(encoded), 1)
    buffer = pool.acquire()
    if buffer is None:
        print("buffer pool exhausted; skipping buffer benchmarks")
        return
    buffer.write(encoded)
    buffer_view = BufferView(buffer, len(encoded))
    buffer_mv = buffer_view.as_memoryview()

    bench("msgspec decode (bytes)", lambda: decoder.decode(encoded), iterations)
    bench("msgspec decode (memoryview)", lambda: decoder.decode(view), iterations)
    bench("msgspec decode (BufferView)", lambda: decoder.decode(buffer_mv), iterations)

    encoder = msgspec.json.Encoder()
    bench("msgspec encode (dict)", lambda: encoder.encode(payload), iterations)

    buffer_view.release()


def run_pydantic(size: int, iterations: int) -> None:
    try:
        from pydantic import BaseModel
    except Exception:
        print("pydantic not available; skipping pydantic benchmarks")
        return

    class Model(BaseModel):
        id: int
        data: str

    payload = {"id": 1, "data": "x" * size}
    instance = Model(**payload)
    encoded = instance.model_dump_json().encode()

    bench(
        "pydantic validate_json",
        lambda: Model.model_validate_json(encoded),
        iterations,
    )
    bench("pydantic dump_json", lambda: instance.model_dump_json(), iterations)


def main() -> None:
    parser = argparse.ArgumentParser(description="Vertebra serialization benchmarks")
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument(
        "--sizes",
        type=int,
        nargs="+",
        default=[1024, 10 * 1024, 100 * 1024],
        help="payload sizes in bytes",
    )
    args = parser.parse_args()

    for size in args.sizes:
        print(f"\nPayload size: {size} bytes")
        run_msgspec(size, args.iterations)
        run_pydantic(size, args.iterations)


if __name__ == "__main__":
    main()
