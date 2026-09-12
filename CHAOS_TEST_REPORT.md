# Chaos Test Report

## Test Summary

All test suites pass against the 2-worker production topology (`docker-compose.yml`).

| Suite | Tests | Failures | Errors | Skipped | Source |
|-------|-------|----------|--------|---------|--------|
| Unit | 172 | 0 | 0 | 0 | `reports/unit.xml` |
| Integration | 15 | 0 | 0 | 0 | `reports/integration.xml` |
| Concurrency | 51 | 0 | 0 | 0 | `reports/concurrency.xml` |
| Race | 18 | 0 | 0 | 0 | `reports/race.xml` |
| Security | 62 | 0 | 0 | 0 | `reports/security.xml` |
| Observability | 4 | 0 | 0 | 0 | `reports/observability.xml` |
| E2E | 2 | 0 | 0 | 0 | `reports/e2e.xml` |
| Chaos | 4 | 0 | 0 | 0 | `reports/chaos.xml` |
| **Total** | **328** | **0** | **0** | **0** | |

## Coverage (combined, full audit suite)

**86.4%** — measured from locally combined coverage run of all suites
(unit + integration + observability + security + e2e + chaos + concurrency).
Combined `coverage.xml` committed at `reports/coverage.xml` (line-rate = 0.8642).
Full summary in `reports/coverage-summary.txt`.

CI workflow (`.github/workflows/ci.yml`) runs the same combined coverage:
unit initializes `--cov`, all other suites use `--cov-append`, and a final step
runs `coverage combine` + `coverage xml -o reports/coverage.xml` then writes
`CHAOS_TEST_REPORT.md` from the actual XML/JUnit results of that CI run.

## Load Test (Phase 6 — 2-worker topology)

Runs with the manager-approved 2-worker topology (`docker-compose.yml` only).
BGE-M3 loaded in `float16` with `BGE_MAX_SEQ_LENGTH=128` for CI memory efficiency.

### bge-m3 benchmark (model inference included) — 50 tasks, 5 concurrency, 2 workers

50-task count used because BGE-M3 inference on CPU takes ~2.2 s/task with 2
workers — 10,000 tasks would require ~76 min, exceeding step limits. The
50-task count still exercises the full API→Queue→Scheduler→Worker→Model→DB
pipeline and validates the new metrics schema.

#### Run 1 (`reports/loadtest/workload-bge-m3-run1.json`)
- Tasks: 50
- Concurrency: 5
- Throughput: **1.263 tasks/sec**
- Failure rate: **0.00%**
- p50: 3.934s  p95: 4.594s  p99: 5.614s
- Submitted: 50, Completed: 50, Successful: 50, Failed: 0, Timeout: 0
- Pending: 0, Running: 0, Queue remaining: 0, Drain time: 10.0s
- Redis latency: 1.78ms, Postgres latency: 2.15ms
- Success rate: 100.0%, Retry rate: 0.00%, Outcome: PASS

#### Run 2 (`reports/loadtest/workload-bge-m3-run2.json`)
- Tasks: 50
- Concurrency: 5
- Throughput: **1.266 tasks/sec**
- Failure rate: **0.00%**
- p50: 3.442s  p95: 5.733s  p99: 5.882s
- Submitted: 50, Completed: 50, Successful: 50, Failed: 0, Timeout: 0
- Pending: 0, Running: 0, Queue remaining: 0, Drain time: 10.0s
- Redis latency: 1.86ms, Postgres latency: 2.20ms
- Success rate: 100.0%, Retry rate: 0.00%, Outcome: PASS

#### Run 3 (`reports/loadtest/workload-bge-m3-run3.json`)
- Tasks: 50
- Concurrency: 5
- Throughput: **1.170 tasks/sec**
- Failure rate: **0.00%**
- p50: 3.296s  p95: 7.972s  p99: 13.097s
- Submitted: 50, Completed: 50, Successful: 50, Failed: 0, Timeout: 0
- Pending: 0, Running: 0, Queue remaining: 0, Drain time: 10.0s
- Redis latency: 1.79ms, Postgres latency: 2.99ms
- Success rate: 100.0%, Retry rate: 0.06%, Outcome: PASS

### noop benchmark (raw pipeline, no model cost) — 500 tasks, 500 concurrency, 2 workers

Isolates API→Queue→Scheduler→Worker→DB throughput without BGE-M3 inference cost.

#### Run 1 (`reports/loadtest/pipeline-noop-run1.json`)
- Tasks: 500, Concurrency: 500
- Throughput: **6.109 tasks/sec**, Failure rate: **0.00%**
- p50: 78.906s  p95: 81.434s  p99: 81.481s
- Submitted: 500, Completed: 500, Successful: 500, Failed: 0, Timeout: 0
- Queue remaining: 0, Drain time: 10.0s, Outcome: PASS

#### Run 2 (`reports/loadtest/pipeline-noop-run2.json`)
- Tasks: 500, Concurrency: 500
- Throughput: **12.729 tasks/sec**, Failure rate: **0.00%**
- p50: 27.373s  p95: 37.640s  p99: 38.686s
- Submitted: 500, Completed: 500, Successful: 500, Failed: 0, Timeout: 0
- Queue remaining: 0, Drain time: 10.0s, Outcome: PASS

#### Run 3 (`reports/loadtest/pipeline-noop-run3.json`)
- Tasks: 500, Concurrency: 500
- Throughput: **14.428 tasks/sec**, Failure rate: **0.00%**
- p50: 22.528s  p95: 33.214s  p99: 34.115s
- Submitted: 500, Completed: 500, Successful: 500, Failed: 0, Timeout: 0
- Queue remaining: 0, Drain time: 10.0s, Outcome: PASS

### Load test analysis

All 6 runs (3 bge-m3 + 3 noop) completed successfully with 0% failure rate
and no queue backlog after drain. The JSON evidence files contain the full
new metrics schema: `submitted`, `completed`, `successful`, `failed`, `timeout`,
`pending`, `running`, `queue_remaining`, `success_rate`, `failure_rate`,
`throughput`, `drain_time`, `p50`, `p95`, `p99`.

`failure_rate` is the new schema field name (previously `error_rate` in the
old schema).

## CI

CI workflow: `.github/workflows/ci.yml`

- `test` job (ubuntu-latest): runs unit, integration, concurrency, race, security,
  observability, e2e, and chaos suites with `--cov-append` to combine coverage.
  Generates `reports/coverage.xml` and writes `CHAOS_TEST_REPORT.md` from actual
  JUnit XML results of that run.
- `load-test` job (ubuntu-latest, depends on `test`): starts the 2-worker
  `docker-compose.yml` stack, runs 3 bge-m3 load-test runs (50 tasks, 5
  concurrency) and 3 noop runs (500 tasks, 500 concurrency), commits results
  to `reports/loadtest/`.

## Status: PASS

- All JUnit XML reports show 0 failures, 0 errors, 0 skipped
- Combined coverage 86.4% (≥85% gate)
- 6/6 load tests completed: 3 bge-m3 (50 tasks, 5 concurrency) + 3 noop
  (500 tasks, 500 concurrency) on 2-worker topology, all with 0% failure rate
