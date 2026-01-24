import asyncpg
from .base import BenchmarkRunner

class NativePostgresRunner(BenchmarkRunner):
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def setup(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=10, max_size=20)

    async def teardown(self):
        if self.pool:
            await self.pool.close()

    async def select_1(self):
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT 1")

    async def pk_lookup(self, pk: int):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM users WHERE id = $1", pk)

    async def large_fetch(self, limit: int):
        async with self.pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM users LIMIT $1", limit)

    async def fetch_arrow(self, limit: int):
        # Native asyncpg doesn't support Arrow natively, so we just fetch standard
        # and maybe convert, but for benchmark comparison we measure the fetch.
        # Alternatively, we just return the list of records.
        return await self.large_fetch(limit)
