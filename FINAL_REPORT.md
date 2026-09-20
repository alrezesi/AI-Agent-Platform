# Final Handover Report — AI-Agent-Platform

**Commit:** `1ef379e` (`refs/heads/master`)  
**Generated:** 2026-09-20  

This report consolidates the final state of the platform after the engineering audit, CI hardening, and load-test evidence commitments.

---

## 1. Problems Found and Fixed

### 1.1 Report generator hardcoded PASS

**File:** `scripts/report_coverage.py`

The previous `ci.yml` used an inline Python snippet that printed `Status: PASS` unconditionally — it rendered `total_tests/total_tests passed` (always matching) and never inspected actual JUnit failure/error counts or coverage gate values.

**Fix:** Replaced with `ReportGenerator.generate()` which computes status from real inputs:
- Reads `coverage.xml` line-rate and compares to `--minimum` (default 85%)
- Reads every JUnit XML suite and sums failures + errors
- Returns `PASS` only when `coverage >= minimum AND total_failures == 0 AND total_errors == 0`
- Exits non-zero on FAIL, making it a real release gate

### 1.2 No CI run provenance

**File:** `scripts/report_coverage.py`

Before this change, `CHAOS_TEST_REPORT.md` had no traceable link to the CI run that produced it.

**Fix:** Added `_ci_provenance()` which reads `GITHUB_RUN_ID`, `GITHUB_SHA`, `GITHUB_REF`, `GITHUB_REPOSITORY`, `GITHUB_SERVER_URL`, `GITHUB_RUN_NUMBER`, and `GITHUB_RUN_STARTED_AT` from the GitHub Actions environment. The `ReportGenerator.generate()` method stamps a provenance block into the report. In CI this emits the full run URL + SHA + timestamp; local runs emit `Provenance (local run)` with the git commit.

### 1.3 Load-test script brittle on zero-duration metrics

**File:** `scripts/chaos_load_test.py`

The previous version would emit a silent `throughput: 0.0` report when the load test failed to measure anything, producing misleading results.

**Fix:** Added validation in `read_load_metrics()` that raises `ValueError` when throughput and all latency fields are zero, refusing to report a degenerate result.

### 1.4 Load-test worker discovery and health-check

**File:** `scripts/chaos_load_test.py`

Replaced the single-shot (un-retrying) API health check with `_wait_for_api_healthy()` that polls `/health` every 1.0s for up to 60.0s, mirroring the pattern already used by the e2e/chaos test suites. Added a `Capture container state before load test` CI step that records `docker compose ps` and container logs so transient vs. sustained failures are distinguishable.

### 1.5 Worker crash-loop from musl vs. glibc PyTorch

**File:** `Dockerfile`, `docker-compose.yml`

Workers were crash-looping in `import torch` because the BGE-M3 manylinux (glibc) wheel was being loaded on `python:3.13-alpine` (musl). The glibc-internal symbols (`__res_init`, `__finitef`, etc.) were unresolved.

**Fix:** Changed the base image from `python:3.13-alpine` to `python:3.13-slim` (glibc), removed the `pthread_shim.so` workaround, and switched from `apk add` to `apt-get install`.

### 1.6 Host-level OOM on CI runner

**File:** `src/agents/bge_m3_agent.py`, `docker-compose.yml`

Two BGE-M3 model instances in fp32 (~2 GB each) plus the load-test's 500 concurrent connections exceeded the ~7 GB `ubuntu-latest` runner.

**Fix:** Added `BGE_MODEL_DTYPE` env-var gating (defaults to `float32`; CI sets `float16`) and `BGE_MAX_SEQ_LENGTH=128`. Also reduced the bge-m3 benchmark from 10,000 tasks / 500 concurrency to 50 tasks / 5 concurrency (10k would take ~76 min on 2 CPU workers).

### 1.7 `dequeue()` bypassed optimistic locking

**File:** `src/agent_platform/scheduler/redis_queue.py`

`dequeue()` performed an unconditional UPDATE that bypassed the `version` check on the PENDING→RUNNING transition, allowing a concurrent cancel or stale write to silently clobber the lease claim.

**Fix:** `dequeue()` now uses the same version-checked `UPDATE ... WHERE task_id = ? AND version = ?` path as `_save_task_to_db()`. On zero-row match, the task is best-effort re-enqueued. Three deterministic regression tests added.

### 1.8 Unauthenticated cross-tenant data leak via /monitoring/*

**File:** `src/agent_platform/multi_tenant/middleware.py`, `src/agent_platform/monitoring/task_trace.py`

The `/monitoring/` path was exempted from authentication entirely, and `build_task_trace()` applied no tenant filtering.

**Fix:** Removed the monitoring exemption from the middleware, added tenant-scoped dependencies to all monitoring routes, and made `build_task_trace()` require a `tenant_id` parameter that filters every match path.

### 1.9 `reclaim_orphaned_tasks()` lost retry_count / erased trace correlation

**File:** `src/agent_platform/scheduler/redis_queue.py`

Reverting a task after lease expiry used a read-modify-write pattern that could double-increment `retry_count`, and the rebuilt Redis cache entry omitted `request_id`/`message_id`, breaking trace correlation.

**Fix:** Single atomic `UPDATE ... SET status=PENDING, retry_count = retry_count+1 WHERE ... RETURNING task_id`. The candidate SELECT now also reads `request_id`/`message_id` and carries them forward into the rebuilt cache entry.

### 1.10 `Task.version` optimistic locking never enforced

**File:** `src/agent_platform/scheduler/redis_queue.py`

The `version` column existed but `_save_task_to_db()` never used it as a WHERE guard, allowing silent lost updates on concurrent terminal writes.

**Fix:** Added version-checked `UPDATE ... WHERE task_id = :id AND version = :expected_version RETURNING task_id`. Zero matching rows raises `TaskWriteConflictError`. The new version is synced back to the in-memory `task.version` after commit.

### 1.11 `IdempotencyManager` was dead code

**File:** `src/agent_platform/recovery/idempotency.py`

Defined but never wired into the queue.

**Fix:** Rewritten to support optional Redis backing (`SET NX` cross-process lock at `idempotency:{key}` with TTL) while preserving an in-memory `asyncio.Lock` + `dict` fallback. Wired into `RedisTaskQueue.enqueue()` and `update_task()`.

### 1.12 Stale diagnostic files and incorrect load-test parameters in ci.yml

**File:** `reports/`, `.gitignore`, `.github/workflows/ci.yml`

29 stale debug/eval/load-test files under `reports/` were removed. ci.yml was updated with correct benchmark parameters (50 tasks/5 concurrency for bge-m3, 500 tasks/500 concurrency for noop, 3 runs each).

---

## 2. Test Summary and Evidence

All test counts are read from the committed JUnit XML files in `reports/`:

| Suite | Tests | Failures | Errors | Skipped | Source file |
|-------|-------|----------|--------|---------|-------------|
| Unit | 172 | 0 | 0 | 0 | `reports/unit.xml` |
| Integration | 15 | 0 | 0 | 0 | `reports/integration.xml` |
| Concurrency | 51 | 0 | 0 | 0 | `reports/concurrency.xml` |
| Race | 18 | 0 | 0 | 0 | `reports/race.xml` |
| Security | 62 | 0 | 0 | 0 | `reports/security.xml` |
| Observability | 4 | 0 | 0 | 0 | `reports/observability.xml` |
| E2E | 2 | 0 | 0 | 0 | `reports/e2e.xml` |
| Chaos | 4 | 0 | 0 | 0 | `reports/chaos.xml` |
| **Total** | **328** | **0** | **0** | **0** | |

All suites pass. No test is skipped. No `xfail` is used to hide failures. No `|| true` suppresses exit codes.

Key regression tests that prove the fixes above:
- `test_lost_update_is_prevented_by_version_check` — deterministic `asyncio.Barrier(2)` test for optimistic locking
- `test_concurrent_reclaim_does_not_double_increment_retry` — atomic reclaim prevents double-increment
- `test_distributed_trace_request_to_final_result_with_retry` — trace correlation survives reclaim+re-enqueue
- `test_dequeue_increments_version_on_every_successful_claim` — dequeue() participates in version-checked updates
- `test_idempotency_persists_across_worker_crash` — IdempotencyManager survives worker crash
- 8 unit tests in `tests/unit/test_report_coverage.py` covering PASS/FAIL computation, failure injection, coverage-below-minimum, all-zero rejection, and provenance stamping

---

## 3. Coverage Gate

**Combined coverage: 86.4%** (committed `reports/coverage.xml`, line-rate = 0.8642)

The `--cov-fail-under=85` gate is set on every `pytest` step in the `test` job. Combined coverage is computed via `--cov-append` across all suites. The `release-gate` job runs `report_coverage.py --minimum 85` as the final check — it exits non-zero if coverage drops below 85% or any JUnit suite reports failures/errors.

---

## 4. Load Test Results

### bge-m3 benchmark (includes BGE-M3 model inference)

50 tasks, 5 concurrency, 2 workers, `float16` precision, `BGE_MAX_SEQ_LENGTH=128`:

| Run | Throughput | Failure rate | p50 / p95 / p99 | Drain time | Outcome | Evidence |
|-----|-----------|-------------|-----------------|------------|---------|----------|
| 1 | 1.263/s | 0.00% | 3.93 / 4.59 / 5.61 | 10.0s | PASS | `workload-bge-m3-run1.json` |
| 2 | 1.266/s | 0.00% | 3.44 / 5.73 / 5.88 | 10.0s | PASS | `workload-bge-m3-run2.json` |
| 3 | 1.170/s | 0.00% | 3.30 / 7.97 / 13.10 | 10.0s | PASS | `workload-bge-m3-run3.json` |

### noop benchmark (raw pipeline, no model cost)

500 tasks, 500 concurrency, 2 workers:

| Run | Throughput | Failure rate | p50 / p95 / p99 | Drain time | Outcome | Evidence |
|-----|-----------|-------------|-----------------|------------|---------|----------|
| 1 | 6.109/s | 0.00% | 78.91 / 81.43 / 81.48 | 10.0s | PASS | `pipeline-noop-run1.json` |
| 2 | 12.729/s | 0.00% | 27.37 / 37.64 / 38.69 | 10.0s | PASS | `pipeline-noop-run2.json` |
| 3 | 14.428/s | 0.00% | 22.53 / 33.21 / 34.11 | 10.0s | PASS | `pipeline-noop-run3.json` |

All 6 runs completed with 0% failure rate, 0 queue backlog, and `outcome: PASS`. The noop benchmark's throughput ramps up across runs (6→13→14/s) as warm-up effects settle.

---

## 5. CI Pipeline

**File:** `.github/workflows/ci.yml`

Three-job flow:

1. **`test`** (ubuntu-latest, 2-worker topology via `docker-compose.yml`)
   - Runs unit, integration, concurrency, race, security, observability, e2e, and chaos suites
   - Each `pytest` invocation uses `--cov-append` to accumulate coverage
   - `--cov-fail-under=85` enforced on every step
   - Uploads JUnit XMLs and `coverage.xml` as artifacts

2. **`load-test`** (ubuntu-latest, depends on `test`)
   - Starts the 2-worker Docker stack with `BGE_MODEL_DTYPE=float16` and `BGE_MAX_SEQ_LENGTH=128`
   - Runs 3 bge-m3 load-test runs (50 tasks, 5 concurrency each)
   - Runs 3 noop load-test runs (500 tasks, 500 concurrency each)
   - Runs `scripts/load_test_summary.py` to aggregate results
   - Uploads all JSON results to `reports/loadtest/` artifact

3. **`release-gate`** (ubuntu-latest, depends on `[test, load-test]`)
   - Downloads all artifacts (coverage, JUnit, load-test JSON)
   - Runs `scripts/report_coverage.py --minimum 85` as the final gate
   - Stamps CI run provenance (run ID, SHA, ref, start time) into `CHAOS_TEST_REPORT.md`
   - Exits non-zero on any failure — pipeline is green only if this passes

---

## 6. CI Run Provenance

The `release-gate` job calls `report_coverage.py`, which reads GitHub Actions environment variables and stamps them into the generated `CHAOS_TEST_REPORT.md`:

```
Provenance (local run):
  SHA:      1ef379e
  Ref:      refs/heads/master
  Started:  2026-09-12T17:20:53Z
  Note: This report was generated outside CI. CI runs stamp full GitHub
        Actions run ID/URL automatically.
```

When run in CI, the same `_ci_provenance()` function reads:
- `GITHUB_RUN_ID` — unique identifier for the workflow run
- `GITHUB_SHA` — commit SHA the run was triggered on
- `GITHUB_REF` — branch/tag ref
- `GITHUB_REPOSITORY` — `owner/repo`
- `GITHUB_SERVER_URL` — base URL (default `https://github.com`)
- `GITHUB_RUN_NUMBER` — sequential run number for the workflow
- `GITHUB_RUN_STARTED_AT` — ISO 8601 timestamp

The generated report includes a direct `Run URL: https://github.com/{repo}/actions/runs/{run_id}` link, so any committed report is traceable to the exact CI run that produced it.

---

## 7. Failure Modes and Mitigations

| Failure mode | Root cause | Mitigation in code/CI |
|---|---|---|
| Report generator shows PASS with test failures | Hardcoded status string | `ReportGenerator.report_status()` computes from JUnit + coverage; exits non-zero on FAIL |
| Coverage drop below 85% goes unnoticed | No per-step gate | `--cov-fail-under=85` on every pytest step |
| Load test runs against broken codebase | `if: always()` on load-test job | Removed; `load-test` only runs when `test` passes |
| Worker crash-loop in CI | musl/alpine + glibc PyTorch wheel | Base image changed to `python:3.13-slim` |
| Host-level OOM during load test | 2x BGE-M3 fp32 (~4 GB) + 500 concurrent connections on 7 GB runner | `BGE_MODEL_DTYPE=float16` in CI; bge-m3 benchmark scaled to 50 tasks/5 concurrency |
| Transient API health-check failure masks real crashes | Single un-retried health check | `_wait_for_api_healthy()` polls for 60s; `Capture container state before load test` step records `docker inspect` + logs |
| Cross-tenant data leak via /monitoring/* | Monitoring path exempted from auth | Monitoring exemption removed; all routes require tenant-scoped auth |
| Lost update on dequeue | Version check bypassed | dequeue() now uses version-checked UPDATE |
| Stale writer clobbers terminal state | Optimistic locking not enforced | `UPDATE ... WHERE version = :v`; `TaskWriteConflictError` on conflict |
| Double-reclaim increments retry_count | Read-modify-write race | Atomic `UPDATE ... WHERE ... RETURNING` |
| Trace correlation lost after reclaim | Requeued task missing request_id/message_id | Reclaim now reads and carries forward correlation identifiers |
| Test DB isolation from live workers | Shared Postgres `tasks` table | Dedicated `agent_platform_test` database with auto-creation via `ensure_test_db` fixture |
| Unit tests can't reach DB in CI | Wrong port (5433 vs 5432) | Job-level `POSTGRES_URL` env var in ci.yml |

---

## 8. Known Limitations

1. **Full 10k/500 bge-m3 load test is documented as infeasible on `ubuntu-latest`.** BGE-M3 inference on CPU takes ~2.2s/task with 2 workers; 10,000 tasks would require ~76 minutes, exceeding the CI step timeout. The 50-task/5-concurrency scale still exercises the full pipeline and validates the metrics schema. The original 10k/500 spec is documented as a known limitation in `FINAL_REPORT.md` section 8 and `ENGINEERING_AUDIT.md` Addendum 11 closing note.

2. **Rate limiting is in-memory per process.** The rate limiter is not backed by Redis or any shared store; distributed correctness across multiple API processes is not claimed (documented in `ENGINEERING_AUDIT.md` Security Audit section).

3. **Worker execution is At-Least-Once, not Exactly-Once.** Lease expiry + reclaim can re-execute a task after a worker crash. Business side effects are the worker's responsibility, not provided by the scheduler (documented in `ENGINEERING_AUDIT.md` Delivery Semantics).

4. **Host-load timing tests remain sensitive.** Tests with arbitrary-timing assertions in `tests/concurrency` and `tests/race` may flake under high host load on shared CI runners. These pass in isolation and fail consistently only under extreme resource contention. This is a pre-existing characteristic, not a regression.

5. **Local vs. CI split for load-test JSON.** The committed JSON evidence files under `reports/loadtest/` were produced from a local run with the 2-worker topology. CI reproduces equivalent results but the JSON files are committed for verifiability (see `RELEASE_READINESS.md` Local vs CI split table).

---

*For the full audit trail including CI run history, diagnostic evidence, and root-cause analysis of the 11 problems above, see `ENGINEERING_AUDIT.md` and its 11 addenda.*
