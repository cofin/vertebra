import random
import time
import uuid
from typing import Any

import cbor2
import msgspec
from rich.console import Console
from rich.table import Table

from vertebra.resources.arrow import encode_arrow

# Ensure C implementation of cbor2 is used
try:
    import _cbor2

    if cbor2.dumps is not _cbor2.dumps:
        print("WARNING: cbor2 is using Pure Python implementation (dumps identity mismatch)!")
    else:
        print("INFO: cbor2 is using C implementation.")
except ImportError:
    print("WARNING: _cbor2 module not found. Using Pure Python cbor2!")

console = Console()


class Row(msgspec.Struct):
    id: int
    name: str
    value: float
    active: bool
    tags: list[str]


def generate_data(count: int) -> dict[str, Any]:
    rows = []
    columns = ["id", "name", "value", "active", "tags"]

    # Generate list of dicts (standard DB driver output format)
    rows = []
    for i in range(count):
        {
            "id": i,
            "name": f"item-{uuid.uuid4()}",
            "value": random.random() * 1000,
            "active": i % 2 == 0,
            "tags": ["a", "b", "c"],
        }
        # In vertebra, rows are often tuples/lists, but let's test dicts as it's common
        # Actually vertebra.resources.arrow.encode_arrow expects:
        # data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        # implying rows are indexable (tuples or lists)

        # Let's simulate tuple rows which is faster and what db drivers usually return
        row_tuple = (
            i,
            f"item-{uuid.uuid4()}",
            random.random() * 1000,
            i % 2 == 0,
            "tags",  # nanoarrow string inference might fail on lists, keeping simple for now
        )
        rows.append(row_tuple)

    return {"columns": columns, "rows": rows}


def bench_msgspec(data: dict, iterations: int) -> float:
    # Encode the whole result dict
    start = time.perf_counter()
    for _ in range(iterations):
        _ = msgspec.msgpack.encode(data)
    return (time.perf_counter() - start) / iterations


def bench_cbor2(data: dict, iterations: int) -> float:
    start = time.perf_counter()
    for _ in range(iterations):
        _ = cbor2.dumps(data)
    return (time.perf_counter() - start) / iterations


def bench_nanoarrow(data: dict, iterations: int) -> float:
    start = time.perf_counter()
    for _ in range(iterations):
        _ = encode_arrow(data)
    return (time.perf_counter() - start) / iterations


def run_benchmark():
    sizes = [10, 100, 500, 1000, 2000, 5000]
    table = Table(title="Serialization Benchmark (Time per op)")
    table.add_column("Rows", justify="right")
    table.add_column("MsgPack (us)", justify="right")
    table.add_column("CBOR2 (us)", justify="right")
    table.add_column("Arrow (us)", justify="right")
    table.add_column("Winner", style="bold green")

    for size in sizes:
        data = generate_data(size)
        iterations = max(5, 50000 // size)

        t_msgpack = bench_msgspec(data, iterations) * 1e6
        t_cbor2 = bench_cbor2(data, iterations) * 1e6
        t_arrow = bench_nanoarrow(data, iterations) * 1e6

        times = {"MsgPack": t_msgpack, "CBOR2": t_cbor2, "Arrow": t_arrow}
        winner = min(times, key=times.get)

        table.add_row(str(size), f"{t_msgpack:.2f}", f"{t_cbor2:.2f}", f"{t_arrow:.2f}", winner)

    console.print(table)


if __name__ == "__main__":
    from typing import Any

    run_benchmark()
