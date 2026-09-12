"""
Generate a load test summary from chaos_load_test.py output files.

Reads all benchmark output JSON files from reports/loadtest/ and produces
a summary. Handles both the new metrics schema (failure_rate, success_rate,
etc.) and legacy files that may still use error_rate.

Usage:
    python scripts/load_test_summary.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _load_metrics(path: Path) -> dict:
    data: dict = json.loads(path.read_text(encoding="utf-8-sig"))
    metrics = data.get("metrics", {})
    # Normalize: error_rate -> failure_rate (legacy compatibility)
    if "failure_rate" not in metrics and "error_rate" in metrics:
        metrics["failure_rate"] = metrics["error_rate"]
    return metrics


def _extract_run(path: Path) -> dict | None:
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    m = data.get("metrics", {})
    if "failure_rate" not in m and "error_rate" in m:
        m = dict(m)
        m["failure_rate"] = m["error_rate"]
    return {
        "run": data.get("tasks", 0),
        "tasks": data.get("tasks", 0),
        "concurrency": data.get("concurrency", 0),
        "benchmark": data.get("benchmark", "unknown"),
        "throughput": m.get("throughput", 0.0),
        "failure_rate": m.get("failure_rate", 0.0),
        "p50": m.get("p50", 0.0),
        "p95": m.get("p95", 0.0),
        "p99": m.get("p99", 0.0),
        "queue_remaining": m.get("queue_remaining", 0),
        "drain_time": m.get("drain_time", 0.0),
        "outcome": m.get("outcome", "unknown"),
    }


def main() -> int:
    loadtest_dir = Path("reports/loadtest")
    summary: dict[str, list] = {"bge_m3": [], "noop": []}

    # bge-m3 runs: workload-bge-m3-run{1,2,3}.json
    for run in (1, 2, 3):
        path = loadtest_dir / f"workload-bge-m3-run{run}.json"
        entry = _extract_run(path)
        if entry:
            entry["run"] = run
            summary["bge_m3"].append(entry)
            print(f"  bge-m3 Run {run}: throughput={entry['throughput']:.1f} tasks/s, "
                  f"failure_rate={entry['failure_rate']:.2f}%, outcome={entry['outcome']}")

    # noop run: pipeline-noop-run1.json
    for run in (1,):
        path = loadtest_dir / f"pipeline-noop-run{run}.json"
        entry = _extract_run(path)
        if entry:
            entry["run"] = run
            summary["noop"].append(entry)
            print(f"  noop Run {run}: throughput={entry['throughput']:.1f} tasks/s, "
                  f"failure_rate={entry['failure_rate']:.2f}%, outcome={entry['outcome']}")

    # Legacy fallback: run{1,2,3}.json
    if not summary["bge_m3"]:
        for run in (1, 2, 3):
            path = loadtest_dir / f"run{run}.json"
            entry = _extract_run(path)
            if entry:
                entry["run"] = run
                summary["bge_m3"].append(entry)

    # Determine overall pass/fail
    all_runs = summary["bge_m3"] + summary["noop"]
    all_passed = all(r["failure_rate"] < 1.0 for r in all_runs) if all_runs else False

    output = {
        "runs": all_runs,
        "total_runs": len(all_runs),
        "all_passed": all_passed,
        "benchmarks": summary,
    }

    out_path = loadtest_dir / "summary.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"\nSummary written to {out_path}")
    print(json.dumps(output, indent=2))

    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
