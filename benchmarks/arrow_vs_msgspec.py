"""
Benchmark MessagePack vs Arrow serialization for Vertebra.
"""
import time
import msgspec
import pyarrow as pa
from rich.console import Console
from rich.table import Table

console = Console()

class Row(msgspec.Struct):
    id: int
    name: str
    value: float
    active: bool

def generate_data(count: int) -> list[Row]:
    return [
        Row(id=i, name=f"item-{i}", value=i * 1.5, active=i % 2 == 0)
        for i in range(count)
    ]

def bench_msgspec(data: list[Row], iterations: int) -> float:
    # Warmup
    _ = msgspec.msgpack.encode(data)
    
    start = time.perf_counter()
    for _ in range(iterations):
        _ = msgspec.msgpack.encode(data)
    return (time.perf_counter() - start) / iterations

def bench_arrow(data: list[Row], iterations: int) -> float:
    # Pre-convert to Arrow RecordBatch to measure PURE serialization speed
    # This assumes the application can maintain data in Arrow-friendly format
    ids = [r.id for r in data]
    names = [r.name for r in data]
    values = [r.value for r in data]
    actives = [r.active for r in data]
    
    arrays = [
        pa.array(ids, type=pa.int64()),
        pa.array(names, type=pa.string()),
        pa.array(values, type=pa.float64()),
        pa.array(actives, type=pa.bool_()),
    ]
    batch = pa.RecordBatch.from_arrays(arrays, names=["id", "name", "value", "active"])
    
    # Warmup
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    
    start = time.perf_counter()
    for _ in range(iterations):
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        _ = sink.getvalue()
    return (time.perf_counter() - start) / iterations

def run_benchmark(sizes=[100, 1000, 10000, 100000]):
    table = Table(title="Serialization Benchmark (Time per op)")
    table.add_column("Rows", justify="right")
    table.add_column("MsgPack (us)", justify="right")
    table.add_column("Arrow (us)", justify="right")
    table.add_column("Speedup", justify="right")

    for size in sizes:
        data = generate_data(size)
        iterations = max(10, 100000 // size) # Adjust iterations based on size
        
        t_msgpack = bench_msgspec(data, iterations) * 1e6
        t_arrow = bench_arrow(data, iterations) * 1e6
        
        speedup = t_msgpack / t_arrow if t_arrow > 0 else 0
        
        table.add_row(
            str(size), 
            f"{t_msgpack:.2f}", 
            f"{t_arrow:.2f}", 
            f"{speedup:.2f}x"
        )

    console.print(table)

if __name__ == "__main__":
    run_benchmark()
