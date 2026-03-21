-- name: select_runs
SELECT
    r.run_id,
    r.git_sha,
    r.git_branch,
    r.machine_os,
    r.machine_arch,
    r.machine_cpu,
    r.python_version,
    r.started_at,
    r.note,
    (SELECT COUNT(*) FROM benchmark_results br WHERE br.run_id = r.run_id) AS result_count
FROM benchmark_runs r
ORDER BY r.started_at DESC
LIMIT $1;

-- name: select_run_results
SELECT
    target, scenario, concurrency, duration_s,
    rps, latency_mean_ms, latency_p50_ms, latency_p99_ms, latency_max_ms,
    total_requests, errors, url, timestamp
FROM benchmark_results
WHERE run_id = $1
ORDER BY target, scenario, concurrency;

-- name: compare_runs
SELECT
    COALESCE(c.target, b.target) AS target,
    COALESCE(c.scenario, b.scenario) AS scenario,
    COALESCE(c.concurrency, b.concurrency) AS concurrency,
    c.rps AS current_rps,
    b.rps AS baseline_rps,
    CASE WHEN b.rps > 0 THEN ((c.rps - b.rps) / b.rps) * 100 ELSE NULL END AS rps_delta_pct,
    c.latency_p99_ms AS current_p99_ms,
    b.latency_p99_ms AS baseline_p99_ms,
    CASE WHEN b.latency_p99_ms > 0 THEN ((c.latency_p99_ms - b.latency_p99_ms) / b.latency_p99_ms) * 100 ELSE NULL END AS p99_delta_pct
FROM benchmark_results c
FULL OUTER JOIN benchmark_results b
    ON c.target = b.target
    AND c.scenario = b.scenario
    AND c.concurrency = b.concurrency
    AND b.run_id = $2
WHERE c.run_id = $1
ORDER BY target, scenario, concurrency;

-- name: select_latest_run_id
SELECT run_id FROM benchmark_runs ORDER BY started_at DESC LIMIT 1;

-- name: select_previous_run_id
SELECT run_id
FROM benchmark_runs
WHERE started_at < (SELECT started_at FROM benchmark_runs WHERE run_id = $1)
ORDER BY started_at DESC
LIMIT 1;

-- name: select_trend
SELECT
    r.run_id,
    r.git_sha,
    r.started_at,
    br.rps,
    br.latency_p99_ms
FROM benchmark_results br
JOIN benchmark_runs r ON br.run_id = r.run_id
WHERE br.target = $1
ORDER BY r.started_at DESC
LIMIT $2;
