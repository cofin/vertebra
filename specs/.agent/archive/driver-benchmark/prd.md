# Product Requirements Document: Vertebra Driver Benchmark Suite

## 1. Overview
This project aims to create a comprehensive benchmark suite to validate the performance of Vertebra's database drivers against established native drivers like `asyncpg` (PostgreSQL) and `aiomysql` (MySQL). The primary goal is to settle "The Driver Bet": proving that the overhead of Vertebra's shared-memory IPC is negligible compared to the overhead of traditional connection pooling and that it delivers superior performance for large result sets via zero-copy Arrow transport.

## 2. Goals & Success Metrics
*   **Primary Goal:** Quantify the performance delta between Vertebra and native drivers.
*   **The Driver Bet:** Demonstrate that Vertebra achieves:
    *   Comparable latency (within 5-10% or better) for small queries.
    *   Significantly higher throughput (2x+) for large result sets (Arrow serialization).
    *   Stable performance under high concurrency (100+ concurrent tasks).
*   **Deliverables:**
    *   A reproducible benchmark suite runner.
    *   Docker Compose setup for the test environment.
    *   Comparative reports (CLI tables and JSON).

## 3. Scope
### 3.1 Drivers to Benchmark
*   **Vertebra:** Vertebra's Python driver (using shared memory IPC).
*   **Native Postgres:** `asyncpg` (known for high performance).
*   **Native MySQL:** `aiomysql` (standard async MySQL driver).

### 3.2 Test Scenarios
1.  **Small Queries (Latency Focus):**
    *   `SELECT 1`
    *   Simple PK lookups (`SELECT * FROM users WHERE id = ?`).
2.  **Large Result Sets (Throughput/Serialization Focus):**
    *   Fetching 10,000+ rows.
    *   Comparing Vertebra's Arrow transport vs Native driver's Python object creation overhead.
3.  **Concurrent Load (Scalability Focus):**
    *   Mixed workload with 10, 50, 100, 500 concurrent async tasks.

### 3.3 Metrics
*   **Latency:** p50, p95, p99 (microseconds/milliseconds).
*   **Throughput:** Requests Per Second (RPS).
*   **Data Rate:** Rows/sec or MB/sec for large results.

## 4. Technical Architecture
*   **Language:** Python 3.10+
*   **Runner:** Custom async benchmark runner or `pytest-benchmark`.
*   **Infrastructure:**
    *   `docker-compose.yml` defining `postgres`, `mysql`, and `vertebra-daemon` (if applicable).
*   **Data:** Pre-seeded datasets (e.g., generated via a fixture).

## 5. Constraints & Assumptions
*   Vertebra driver and daemon are available or buildable in the environment.
*   Benchmarks run on a single machine (local IPC focus).
