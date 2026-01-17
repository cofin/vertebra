from __future__ import annotations

import argparse

from vertebra import _vertebra


def main() -> None:
    parser = argparse.ArgumentParser(description="Vertebra IPC consumer example")
    parser.add_argument("--name", required=True)
    parser.add_argument("--messages", type=int, default=1000)
    args = parser.parse_args()

    shm_name = f"{args.name}-shm"
    ring_name = f"{args.name}-ring"

    region = _vertebra.ShmRegion.open_named(shm_name, 4096)
    print("Shared memory says:", region.read(0, 14))

    ring = _vertebra.SpscRing.open_named(ring_name, 64, 256)
    for _ in range(args.messages):
        payload = ring.recv(64)
        ring.send(payload, 0)


if __name__ == "__main__":
    main()
