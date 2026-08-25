from __future__ import annotations

import argparse
import json
from pathlib import Path
from xml.etree import ElementTree as ET


def read_coverage_percentage(path: Path) -> float:
    root = ET.parse(path).getroot()
    return float(root.attrib["line-rate"]) * 100.0


def read_junit_summary(path: Path) -> dict[str, int]:
    root = ET.parse(path).getroot()
    # Pytest produces <testsuites><testsuite .../></testsuites>.
    # Some older callers may pass a bare <testsuite .../> as the root.
    suites = root.findall("testsuite")
    if suites:
        tests = sum(int(float(s.attrib.get("tests", 0))) for s in suites)
        failures = sum(int(float(s.attrib.get("failures", 0))) for s in suites)
        errors = sum(int(float(s.attrib.get("errors", 0))) for s in suites)
        skipped = sum(int(float(s.attrib.get("skipped", 0))) for s in suites)
    else:
        tests = int(float(root.attrib.get("tests", 0)))
        failures = int(float(root.attrib.get("failures", 0)))
        errors = int(float(root.attrib.get("errors", 0)))
        skipped = int(float(root.attrib.get("skipped", 0)))
    return {
        "tests": tests,
        "failures": failures,
        "errors": errors,
        "skipped": skipped,
        "passed": tests - failures - errors - skipped,
    }


def read_load_metrics(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))["metrics"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minimum", type=float, default=85.0)
    parser.add_argument("--coverage-xml", type=Path, default=Path("coverage.xml"))
    parser.add_argument("--unit-junit", type=Path, default=Path("reports/unit.xml"))
    parser.add_argument("--integration-junit", type=Path, default=Path("reports/integration.xml"))
    parser.add_argument("--e2e-junit", type=Path, default=Path("reports/e2e.xml"))
    parser.add_argument("--chaos-junit", type=Path, default=Path("reports/chaos.xml"))
    parser.add_argument("--concurrency-junit", type=Path, default=Path("reports/concurrency.xml"))
    parser.add_argument("--race-junit", type=Path, default=Path("reports/race.xml"))
    parser.add_argument("--security-junit", type=Path, default=Path("reports/security.xml"))
    parser.add_argument("--observability-junit", type=Path, default=Path("reports/observability.xml"))
    parser.add_argument("--load-json", type=Path, default=Path("load_test_results.json"))
    parser.add_argument("--output", type=Path, default=Path("CHAOS_TEST_REPORT.md"))
    args = parser.parse_args()

    coverage = read_coverage_percentage(args.coverage_xml)
    unit = read_junit_summary(args.unit_junit)
    integration = read_junit_summary(args.integration_junit)
    e2e = read_junit_summary(args.e2e_junit)
    chaos = read_junit_summary(args.chaos_junit)
    concurrency = read_junit_summary(args.concurrency_junit)
    race = read_junit_summary(args.race_junit)
    security = read_junit_summary(args.security_junit)
    observability = read_junit_summary(args.observability_junit)
    load = read_load_metrics(args.load_json)

    total_passed = (
        unit["passed"]
        + integration["passed"]
        + e2e["passed"]
        + chaos["passed"]
        + concurrency["passed"]
        + race["passed"]
        + security["passed"]
        + observability["passed"]
    )
    total_tests = (
        unit["tests"]
        + integration["tests"]
        + e2e["tests"]
        + chaos["tests"]
        + concurrency["tests"]
        + race["tests"]
        + security["tests"]
        + observability["tests"]
    )
    status = "PASS" if coverage >= args.minimum else "FAIL"

    report = "\n".join(
        [
            "Test Summary",
            "------------",
            f"Unit:          {unit['passed']} passed",
            f"Integration:   {integration['passed']} passed",
            f"E2E:           {e2e['passed']} passed",
            f"Chaos:         {chaos['passed']} passed",
            f"Concurrency:   {concurrency['passed']} passed",
            f"Race:          {race['passed']} passed",
            f"Security:      {security['passed']} passed",
            f"Observability: {observability['passed']} passed",
            "",
            f"Coverage:     {coverage:.1f}%",
            f"Throughput:   {load['throughput']:.1f} tasks/sec",
            f"p50:          {load['p50']:.2f}s",
            f"p95:          {load['p95']:.2f}s",
            f"p99:          {load['p99']:.2f}s",
            f"Error rate:   {load['error_rate']:.2%}",
            f"Retry rate:   {load['retry_rate']:.2%}",
            f"Queue depth:  {load['queue_depth']}",
            f"CPU:          {load.get('cpu', {})}",
            f"Memory:       {load.get('memory', {})}",
            f"Redis latency: {load.get('redis_latency_ms', 0.0):.2f} ms",
            f"Postgres latency: {load.get('postgres_latency_ms', 0.0):.2f} ms",
            "",
            f"Total tests:  {total_passed}/{total_tests} passed",
            f"Status:       {status}",
            "",
        ]
    )
    args.output.write_text(report, encoding="utf-8")
    print(report)
    return 0 if coverage >= args.minimum else 1


if __name__ == "__main__":
    raise SystemExit(main())
