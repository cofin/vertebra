# Benchmark Protocol and Regression Limits

This document specifies the benchmark measurement protocol and regression failure thresholds for Vertebra.

---

## Benchmark Execution Rules

1. **Isolation**:
   - Close non-essential background processes and ensure CPU frequency governor is set to performance where accessible.
   - Run warmup iterations prior to recording timed samples.

2. **Metric Collection**:
   - Record median operations per second and 99th percentile latencies across at least 5 rounds.
   - Collect machine hardware specifications, kernel version, and commit SHA in output metadata.

3. **Regression Thresholds**:
   - Throughput metrics must not degrade by more than 3.0% against the committed baseline.
   - Latency (p99) and cold import times must not increase by more than 5.0%.
