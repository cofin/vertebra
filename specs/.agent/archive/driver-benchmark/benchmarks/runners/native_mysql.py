import aiomysql
from .base import BenchmarkRunner

class NativeMySQLRunner(BenchmarkRunner):
    def __init__(self, **kwargs):
        self.config = kwargs
        self.pool = None

    async def setup(self):
        self.pool = await aiomysql.create_pool(minsize=10, maxsize=20, **self.config)

    async def teardown(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def select_1(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                return await cur.fetchone()

    async def pk_lookup(self, pk: int):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM users WHERE id = %s", (pk,))
                return await cur.fetchone()

    async def large_fetch(self, limit: int):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM users LIMIT %s", (limit,))
                return await cur.fetchall()
                
    async def fetch_arrow(self, limit: int):
        # Not supported natively
        return await self.large_fetch(limit)
