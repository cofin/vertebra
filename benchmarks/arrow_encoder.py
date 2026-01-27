import argparse
import random
import time
import uuid
from collections.abc import Callable

from vertebra.resources.arrow import _encode_arrow_python, encode_arrow


def generate_data(rows: int, cols: int) -> dict[str, object]:
    columns = [f"col_{i}" for i in range(cols)]
    data_rows: list[list[object]] = []
    for i in range(rows):
        row = []
        for col_index in range(cols):
            if col_index == 0:
                row.append(i)
            elif col_index == 1:
                row.append(f"item-{uuid.uuid4()}")
            elif col_index == 2:
                row.append(random.random() * 1000)
            else:
                row.append(i % 2 == 0)
        data_rows.append(row)
    return {"columns": columns, "rows": data_rows}


def bench(fn: Callable[[dict[str, object]], bytes], payload: dict[str, object], iterations: int) -> float:
    start = time.perf_counter()
    for _ in range(iterations):
        fn(payload)
    return (time.perf_counter() - start) / iterations


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Vertebra Arrow encoders.")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--cols", type=int, default=4)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=0)
    parser.add_argument("--types", action="store_true")
    args = parser.parse_args()

    payload = generate_data(args.rows, args.cols)
    if args.types:
        payload["types"] = ["int64", "string", "float64"] + ["bool"] * (args.cols - 3)
    if args.batch_size > 0:
        payload["arrow_batch_size"] = args.batch_size

    rust_time = bench(encode_arrow, payload, args.iterations)
    python_time = bench(_encode_arrow_python, payload, args.iterations)
    speedup = python_time / rust_time if rust_time else 0.0

    print(f"rows={args.rows} cols={args.cols} iterations={args.iterations}")
    if args.batch_size > 0:
        print(f"batch_size={args.batch_size}")
    if args.types:
        print("types=on")
    print(f"rust={rust_time * 1e6:.2f}us python={python_time * 1e6:.2f}us speedup={speedup:.2f}x")


if __name__ == "__main__":
    main()
