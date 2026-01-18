from __future__ import annotations

import argparse
import statistics
import threading
import time

from vertebra.ipc.channel import Channel


def main() -> None:
    parser = argparse.ArgumentParser(description="Vertebra IPC latency benchmark")
    parser.add_argument("--iterations", type=int, default=1000)
    args = parser.parse_args()

    client, server = Channel.create_pair(slot_size=1024, slots=64)
    latencies: list[float] = []

    def server_loop() -> None:
        for _ in range(args.iterations):
            proc_id, params = server.receive_sync()
            server.respond_sync(proc_id, {"ok": params["value"]})

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()

    for i in range(args.iterations):
        start = time.perf_counter()
        client.send_sync("ping", {"value": i})
        latencies.append(time.perf_counter() - start)

    thread.join(timeout=5)

    p50 = statistics.median(latencies)
    p95 = statistics.quantiles(latencies, n=20)[18]
    p99 = statistics.quantiles(latencies, n=100)[98]

    print(f"iterations: {args.iterations}")
    print(f"p50: {p50 * 1e6:.2f} µs")
    print(f"p95: {p95 * 1e6:.2f} µs")
    print(f"p99: {p99 * 1e6:.2f} µs")


if __name__ == "__main__":
    main()
