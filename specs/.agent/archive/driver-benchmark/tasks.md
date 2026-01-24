# Implementation Tasks: Vertebra Driver Benchmark Suite

## Phase 1: Environment Setup
- [ ] Create `benchmarks/` directory structure.
- [ ] Create `benchmarks/docker-compose.yml` with Postgres and MySQL services.
- [ ] Create `benchmarks/requirements.txt` (asyncpg, aiomysql, pandas, pyarrow, rich, etc.).
- [ ] Implement `benchmarks/conftest.py` or setup script to seed databases with test data.

## Phase 2: Native Driver Benchmarks
- [ ] Implement `benchmarks/runners/postgres_native.py` using `asyncpg`.
    - [ ] `measure_select_1`
    - [ ] `measure_pk_lookup`
    - [ ] `measure_large_fetch`
- [ ] Implement `benchmarks/runners/mysql_native.py` using `aiomysql`.
    - [ ] `measure_select_1`
    - [ ] `measure_pk_lookup`
    - [ ] `measure_large_fetch`

## Phase 3: Vertebra Driver Benchmarks
- [ ] Implement `benchmarks/runners/vertebra_postgres.py`.
- [ ] Implement `benchmarks/runners/vertebra_mysql.py`.
- [ ] Ensure IPC/Shared Memory configuration is tuned.

## Phase 4: The Benchmark Runner
- [ ] Create `benchmarks/run.py` CLI entrypoint.
- [ ] Implement concurrency harness (running `N` coroutines).
- [ ] Implement timing and stats collection (p50/p99/RPS).
- [ ] Add "The Driver Bet" specific report mode (comparing overheads).

## Phase 5: Documentation & Polish
- [ ] Add `benchmarks/README.md` with usage instructions.
- [ ] Validate results against "The Driver Bet" hypothesis.
