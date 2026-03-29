-- name: insert_run!
INSERT INTO benchmark_runs (
    run_id, git_sha, git_branch, machine_os, machine_arch,
    machine_cpu, python_version, started_at, note
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: insert_result!
INSERT INTO benchmark_results (
    run_id, target, topology, scenario, concurrency, duration_s,
    rps, latency_mean_ms, latency_p50_ms, latency_p99_ms, latency_max_ms,
    total_requests, errors, url, timestamp
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);
