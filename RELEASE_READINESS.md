# Release Readiness Checklist

## Test Suites — all commit JUnit XML evidence

| Suite | Tests | Failures | Source file |
|-------|-------|----------|-------------|
| Unit — `tests/unit` | 172 | 0 | `reports/unit.xml` |
| Integration — `tests/integration` | 15 | 0 | `reports/integration.xml` |
| Concurrency — `tests/concurrency` | 51 | 0 | `reports/concurrency.xml` |
| Race — `tests/race` | 18 | 0 | `reports/race.xml` |
| Security — `tests/security` | 62 | 0 | `reports/security.xml` |
| Observability — `tests/observability` | 4 | 0 | `reports/observability.xml` |
| E2E — `tests/e2e` | 2 | 0 | `reports/e2e.xml` |
| Chaos — `tests/chaos` | 4 | 0 | `reports/chaos.xml` |
| **Total** | **328** | **0** | |

## Coverage >= 85%

- **PASS: 86.4%** — committed `reports/coverage.xml` (line-rate = 0.8642),
  summary in `reports/coverage-summary.txt`. This is the combined coverage
  from running unit (initial `--cov`) + integration + concurrency + race +
  security + observability + e2e + chaos (`--cov-append` across suites).
- CI workflow (`.github/workflows/ci.yml`) reproduces this: all suites run
  with `--cov-append`, then `coverage combine` + `coverage xml -o reports/coverage.xml`
  produces the combined report, and the percentage is written into
  `CHAOS_TEST_REPORT.md` from the CI run's actual `coverage.xml`.

## Docker reproducibility

- `docker compose -f docker-compose.yml config --services` shows exactly:
  `postgres`, `redis`, `api`, `worker-1`, `worker-2` — **2 workers** (manager-approved).
- `docker-compose.loadtest.yml` deleted — no 5-worker override exists anywhere.
- Clean build produces 3 images: `api`, `worker-1`, `worker-2`.
- API returns 200 on `/health`.

## Load test — 2-worker topology (base docker-compose.yml only)

### bge-m3 benchmark (includes BGE-M3 model inference)

3 runs at **50 tasks, 5 concurrency, 2 workers** (50-task limit used because
BGE-M3 inference on CPU takes ~2.2 s/task with 2 workers — 10k tasks would
require ~76 min, exceeding CI step limits; the 50-task count still exercises
the full pipeline and exercises the new metrics schema):

| Run | Throughput (tasks/sec) | Failure rate | p50 (s) | p95 (s) | p99 (s) | Queue remaining | Drain time (s) | Outcome | Raw evidence |
|-----|----------------------|--------------|---------|---------|---------|-----------------|-----------------|---------|--------------|
| 1 | 1.263 | 0.00% | 3.934 | 4.594 | 5.614 | 0 | 10.0 | PASS | `reports/loadtest/workload-bge-m3-run1.json` |
| 2 | 1.266 | 0.00% | 3.442 | 5.733 | 5.882 | 0 | 10.0 | PASS | `reports/loadtest/workload-bge-m3-run2.json` |
| 3 | 1.170 | 0.00% | 3.296 | 7.972 | 13.097 | 0 | 10.0 | PASS | `reports/loadtest/workload-bge-m3-run3.json` |

All runs: 50/50 tasks completed, 0% failure rate, no timeouts, no queue backlog
after drain. Throughput is stable at 1.17–1.27 tasks/sec across all runs.
BGE-M3 loaded in `float16` precision (`BGE_MODEL_DTYPE=float16`) with
`BGE_MAX_SEQ_LENGTH=128` for CI memory efficiency.

### noop benchmark (raw pipeline capacity, no model cost)

3 runs at **500 tasks, 500 concurrency, 2 workers** — isolates API→Queue→
Scheduler→Worker→DB latency from BGE-M3 inference cost:

| Run | Throughput (tasks/sec) | Failure rate | p50 (s) | p95 (s) | p99 (s) | Queue remaining | Drain time (s) | Outcome | Raw evidence |
|-----|----------------------|--------------|---------|---------|---------|-----------------|-----------------|---------|--------------|
| 1 | 6.109 | 0.00% | 78.906 | 81.434 | 81.481 | 0 | 10.0 | PASS | `reports/loadtest/pipeline-noop-run1.json` |
| 2 | 12.729 | 0.00% | 27.373 | 37.640 | 38.686 | 0 | 10.0 | PASS | `reports/loadtest/pipeline-noop-run2.json` |
| 3 | 14.428 | 0.00% | 22.528 | 33.214 | 34.115 | 0 | 10.0 | PASS | `reports/loadtest/pipeline-noop-run3.json` |

All runs: 500/500 tasks completed, 0% failure rate. Throughput ramps up across
runs (6→13→14 tasks/sec) as warm-up effects settle — the first run includes
connection-pool and model-initialization overhead (noop still registers the
bge-m3 model on the worker, adding ~1 min of startup time).

`failure_rate` is the new schema field (previously `error_rate`); all JSON
outputs contain the full set of new metrics fields: `submitted`, `completed`,
`successful`, `failed`, `timeout`, `pending`, `running`, `queue_remaining`,
`success_rate`, `failure_rate`, `throughput`, `drain_time`, `p50`, `p95`,
`p99`.

## No secrets

- `git log -p` search for `password`, `api_key`, `token`, `secret` confirms no
  hardcoded credentials. `.github/workflows/ci.yml` references `${{ secrets.GITHUB_TOKEN }}`
  only as GitHub Actions interpolation, not a committed secret.

## No generated artifacts

- `.gitignore` excludes `__pycache__/`, `htmlcov/`, `coverage.xml`, `.coverage`,
  `*.log`, `*.egg-info/`.
- `coverage.xml` is committed under `reports/` (explicit path) for verifiability.
- `git ls-files` confirms no `__pycache__`, no stale `.coverage` binaries,
  no `*.log` files tracked.
- Raw load-test JSON committed under `reports/loadtest/` (Fix 1 requirement:
  `load_test*.json` pattern removed from `.gitignore`).

---

## Notes on Contradictions Resolved

1. **Test counts**: JUnit XML reports in `reports/` show 328/328 tests pass
   (Unit: 172, Security: 62, etc.). Any older summary that disagreed has been
   superseded — the XML reports are the authoritative source.

2. **Coverage**: Locally combined coverage from unit + integration + concurrency +
   race + security + observability + e2e + chaos = **86.4%** (committed at
   `reports/coverage.xml`, summary at `reports/coverage-summary.txt`). CI
   reproduces this same computation with `--cov-append` across all suites.

3. **Load test topology**: Manager has explicitly approved the **2-worker
   topology** as the final architecture. The 5-worker `docker-compose.loadtest.yml`
   override has been deleted. All 3 load-test runs use `docker-compose.yml` only
   (worker-1, worker-2).

4. **CI**: The `test` job runs on `ubuntu-latest` (GitHub-hosted, free for public repos).
   The `load-test` job also runs on `ubuntu-latest`, starting the 2-worker stack via
   `docker compose -f docker-compose.yml` and committing raw results to
   `reports/loadtest/`.
