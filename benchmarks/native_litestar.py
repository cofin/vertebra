import asyncio
import multiprocessing
import time

import uvicorn
from httpx import AsyncClient
from litestar import Litestar, get


# --- Application ---
@get("/hello", sync_to_thread=False)
def hello() -> dict:
    return {"hello": "world"}


app = Litestar(route_handlers=[hello])


# --- Uvicorn Runner ---
def run_uvicorn():
    uvicorn.run(app, port=8001, log_level="error")


# --- Vertebra Runner ---
def run_vertebra():
    # Simulate Vertebra worker loop directly for benchmark
    # Real benchmark requires full broker setup which is complex to script in single file
    # So we'll benchmark the "Worker Logic" throughput isolated from IO
    pass


async def benchmark_endpoint(url: str, count: int = 10000):
    async with AsyncClient() as client:
        start = time.time()
        for _ in range(count):
            await client.get(url)
        end = time.time()
    return count / (end - start)


def main():
    # Start Uvicorn
    p = multiprocessing.Process(target=run_uvicorn)
    p.start()
    time.sleep(2)  # Warmup

    try:
        print("Benchmarking Uvicorn...")
        loop = asyncio.new_event_loop()
        rps = loop.run_until_complete(benchmark_endpoint("http://127.0.0.1:8001/hello"))
        print(f"Uvicorn RPS: {rps:.2f}")
    finally:
        p.terminate()
        p.join()


if __name__ == "__main__":
    main()
