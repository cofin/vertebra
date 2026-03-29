-- name: create_bench_result_seq!
CREATE SEQUENCE IF NOT EXISTS bench_result_seq START 1;

-- name: create_benchmark_runs!
CREATE TABLE IF NOT EXISTS benchmark_runs (
    run_id       VARCHAR PRIMARY KEY,
    git_sha      VARCHAR,
    git_branch   VARCHAR,
    machine_os   VARCHAR,
    machine_arch VARCHAR,
    machine_cpu  VARCHAR,
    python_version VARCHAR,
    started_at   TIMESTAMP,
    note         VARCHAR DEFAULT ''
);

-- name: create_benchmark_results!
CREATE TABLE IF NOT EXISTS benchmark_results (
    id              INTEGER PRIMARY KEY DEFAULT nextval('bench_result_seq'),
    run_id          VARCHAR NOT NULL REFERENCES benchmark_runs(run_id),
    target          VARCHAR,
    topology        VARCHAR,
    scenario        VARCHAR,
    concurrency     INTEGER,
    duration_s      INTEGER,
    rps             DOUBLE,
    latency_mean_ms DOUBLE,
    latency_p50_ms  DOUBLE,
    latency_p99_ms  DOUBLE,
    latency_max_ms  DOUBLE,
    total_requests  INTEGER,
    errors          INTEGER DEFAULT 0,
    url             VARCHAR,
    timestamp       TIMESTAMP
);
