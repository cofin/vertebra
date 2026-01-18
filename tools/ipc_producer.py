from __future__ import annotations

import argparse
import os
import time

from vertebra import _vertebra


def main() -> None:
    parser = argparse.ArgumentParser(description="Vertebra IPC producer example")
    parser.add_argument("--name", default=f"/vertebra-example-{os.getpid()}")
    parser.add_argument("--messages", type=int, default=1000)
    args = parser.parse_args()

    shm_name = f"{args.name}-shm"
    ring_name = f"{args.name}-ring"

    region = _vertebra.ShmRegion.create_named(shm_name, 4096)
    region.write(0, b"producer-ready")

    ring = _vertebra.SpscRing.create_named(ring_name, 64, 256)
    payload = b"ping"

    start = time.perf_counter_ns()
    for _ in range(args.messages):
        ring.send(payload, 0)
        _ = ring.recv(64)
    time.perf_counter_ns() - start

    pool = _vertebra.BufferPool(128, 2)
    buf = pool.acquire()
    if buf is not None:
        buf.write(b"buffer-pool")


if __name__ == "__main__":
    main()
