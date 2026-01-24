import asyncio
import os
import random
import string
import time

import asyncpg
import aiomysql
from rich.console import Console

console = Console()

PG_DSN = "postgresql://benchmark:password@localhost:5432/benchmark_db"
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "benchmark",
    "password": "password",
    "db": "benchmark_db",
    "autocommit": True,
}

TABLE_SCHEMA_PG = """
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    age INT NOT NULL,
    data BYTEA,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

TABLE_SCHEMA_MYSQL = """
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    age INT NOT NULL,
    data BLOB,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

ROW_COUNT = 100_000
BATCH_SIZE = 1000

def generate_row():
    name = ''.join(random.choices(string.ascii_letters, k=10))
    email = f"{name}@example.com"
    age = random.randint(18, 90)
    data = os.urandom(32)
    return (name, email, age, data)

async def seed_postgres():
    console.log("[blue]Seeding PostgreSQL...[/]")
    conn = await asyncpg.connect(PG_DSN)
    try:
        await conn.execute(TABLE_SCHEMA_PG)
        
        rows = [generate_row() for _ in range(ROW_COUNT)]
        
        start = time.time()
        await conn.copy_records_to_table(
            'users',
            records=rows,
            columns=['name', 'email', 'age', 'data']
        )
        duration = time.time() - start
        console.log(f"[green]PostgreSQL seeded {ROW_COUNT} rows in {duration:.2f}s[/]")
    finally:
        await conn.close()

async def seed_mysql():
    console.log("[yellow]Seeding MySQL...[/]")
    pool = await aiomysql.create_pool(**MYSQL_CONFIG)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(TABLE_SCHEMA_MYSQL)
            
            rows = [generate_row() for _ in range(ROW_COUNT)]
            
            start = time.time()
            query = "INSERT INTO users (name, email, age, data) VALUES (%s, %s, %s, %s)"
            
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i+BATCH_SIZE]
                await cur.executemany(query, batch)
                
            duration = time.time() - start
            console.log(f"[green]MySQL seeded {ROW_COUNT} rows in {duration:.2f}s[/]")
    
    pool.close()
    await pool.wait_closed()

async def main():
    await asyncio.gather(
        seed_postgres(),
        seed_mysql()
    )

if __name__ == "__main__":
    asyncio.run(main())
