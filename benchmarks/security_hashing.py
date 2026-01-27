import asyncio
import os
import sys
import time

try:
    from vertebra.security.client import SecurityClient
except ImportError:
    # Allow running from root without install if PYTHONPATH set
    sys.path.append(os.path.join(os.getcwd(), "src", "py"))
    from vertebra.security.client import SecurityClient

try:
    import argon2

    HAS_ARGON2 = True
    ph = argon2.PasswordHasher()
except ImportError:
    HAS_ARGON2 = False
    print("argon2-cffi not installed, skipping native benchmark")


async def benchmark_ipc(n=10):
    client = SecurityClient()

    print("Warmup IPC...")
    try:
        client.hash_password("warmup")
    except Exception as e:
        print(f"Failed to connect to broker: {e}")
        return

    print("Running IPC benchmark...")
    start = time.time()
    for _ in range(n):
        client.hash_password("secret")
    end = time.time()
    print(f"IPC Hashing (Bcrypt): {n} iterations in {end - start:.4f}s ({n / (end - start):.2f} ops/s)")


def benchmark_native(n=10):
    if not HAS_ARGON2:
        return
    print("Running Native Argon2 benchmark...")
    start = time.time()
    for _ in range(n):
        ph.hash("secret")
    end = time.time()
    print(f"Native Argon2: {n} iterations in {end - start:.4f}s ({n / (end - start):.2f} ops/s)")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10

    benchmark_native(n)
    asyncio.run(benchmark_ipc(n))
