# Vertebra Driver Benchmarks

This suite validates the performance of Vertebra's drivers against native implementations (`asyncpg` for PostgreSQL, `aiomysql` for MySQL).

## "The Driver Bet"
We aim to prove that Vertebra's shared-memory IPC architecture introduces negligible overhead for small queries while offering massive throughput gains for large result sets via zero-copy Arrow transport, compared to traditional connection pooling.

## Setup

1.  **Environment**
    ```bash
    cd benchmarks
    pip install -r requirements.txt
    docker-compose up -d
    ```

2.  **Seed Data**
    Wait for databases to be healthy, then run:
    ```bash
    python seed.py
    ```
    This creates `users` tables and inserts 100k rows with random data.

## Running Benchmarks

Run the main runner script:

```bash
python run.py --concurrency 10 --duration 5
```

### Options
*   `--concurrency`, `-c`: Number of concurrent tasks (default: 10).
*   `--duration`, `-d`: Seconds to run each test (default: 5).
*   `--drivers`: Filter drivers (`pg`, `mysql`, `vertebra`, `all`).

## Scenarios
1.  `select_1`: Minimal latency check (`SELECT 1`).
2.  `pk_lookup`: Typical indexed read (`SELECT * FROM users WHERE id = 1`).
3.  `large_fetch`: Throughput check (`SELECT * FROM users LIMIT 1000`).
4.  `arrow_fetch`: Zero-copy serialization check (Vertebra Arrow vs Native Objects).

## Project Structure
*   `runners/`: implementations for each driver.
*   `docker-compose.yml`: Database services.
*   `seed.py`: Data generation.
*   `run.py`: Main harness using `asyncio` and `rich`.
