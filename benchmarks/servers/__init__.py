"""ASGI Server Benchmarking Suite.

Provides comprehensive benchmarks for comparing ASGI servers, integrated runtimes,
and web frameworks. Uses oha as the load generator for accurate, low-overhead
measurement.

Categories:
- Category 1: Raw ASGI server performance (Uvicorn, Granian, Hypercorn, etc.)
- Category 2: Integrated runtimes (Vertebra, Robyn, Sanic standalone, Spikard)
- Category 3: Framework overhead (Starlette, FastAPI, BlackSheep, Litestar)

Usage:
    uv run python -m benchmarks.servers.runner
    uv run python -m benchmarks.servers.runner --quick
    uv run python -m benchmarks.servers.runner --servers vertebra,granian
"""
