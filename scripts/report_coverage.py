from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
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


def read_load_metrics(path: Path) -> dict[str, Any]:
    # utf-8-sig tolerates a BOM that some writers (e.g. PowerShell
    # Set-Content) prepend, without changing real behavior.
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8-sig"))
    metrics = data["metrics"]
    if not isinstance(metrics, dict):
        raise ValueError("load metrics must be a JSON object")

    # Refuse to report a degenerate all-zero load test.  A valid load test
    # that actually drove the stack must have a positive throughput and at
    # least one of the latency figures must be non-zero.  Anything else means
    # the load test never ran / never measured anything, and printing
    # "Throughput: 0.0 tasks/sec" would be misleading.
    throughput = float(metrics.get("throughput", 0.0))
    redis_lat = float(metrics.get("redis_latency_ms", 0.0))
    pg_lat = float(metrics.get("postgres_latency_ms", 0.0))
    if throughput <= 0.0 and redis_lat == 0.0 and pg_lat == 0.0:
        raise ValueError(
            "Load metrics are all-zero (throughput=0, redis_latency_ms=0, "
            "postgres_latency_ms=0). The load test did not produce a valid "
            "measurement; refusing to report a degenerate result."
        )
    return metrics


class ReportGenerator:
    """
    Single source of truth for CHAOS_TEST_REPORT.md generation.

    The report is generated from real JUnit XML files, a real coverage.xml,
    and real load-test JSON.  Status is **computed** — never hardcoded:
    PASS only when (a) coverage >= minimum AND (b) every suite has zero
    failures and zero errors.  Any failure or error in any suite makes
    the overall status FAIL, regardless of coverage.
    """

    def __init__(
        self,
        coverage_xml: Path,
        coverage_detail_path: Path | None,
        junit_paths: dict[str, Path],
        load_metrics: dict[str, Any] | None,
        load_test_outcome: str | None,
        minimum: float = 85.0,
    ) -> None:
        self.coverage_xml = coverage_xml
        self.coverage_detail_path = coverage_detail_path
        self.junit_paths = junit_paths
        self.load_metrics = load_metrics
        self.load_test_outcome = load_test_outcome
        self.minimum = minimum

    def _read_suites(self) -> dict[str, dict[str, int]]:
        results: dict[str, dict[str, int]] = {}
        for suite_name, xml_file in self.junit_paths.items():
            if xml_file.exists():
                results[suite_name] = read_junit_summary(xml_file)
            else:
                results[suite_name] = {
                    "tests": 0, "failures": 0, "errors": 0, "skipped": 0, "passed": 0,
                }
        return results

    def _total_failures_and_errors(self, suites: dict[str, dict[str, int]]) -> tuple[int, int]:
        total_failures = sum(s["failures"] for s in suites.values())
        total_errors = sum(s["errors"] for s in suites.values())
        return total_failures, total_errors

    @property
    def coverage(self) -> float:
        return read_coverage_percentage(self.coverage_xml)

    @property
    def coverage_detail(self) -> str:
        if self.coverage_detail_path and self.coverage_detail_path.exists():
            return self.coverage_detail_path.read_text(encoding="utf-8")
        return ""

    def generate(self) -> tuple[str, str]:
        """Return (report_text, status).  status is PASS or FAIL."""
        suites = self._read_suites()
        coverage_pct = self.coverage
        total_tests = sum(s["tests"] for s in suites.values())
        total_passed = sum(s["passed"] for s in suites.values())
        total_failures, total_errors = self._total_failures_and_errors(suites)

        # Status is computed, never hardcoded:
        #   PASS only when coverage >= minimum AND no failures/errors anywhere.
        #   Any test failure or error makes the whole run FAIL.
        test_ok = total_failures == 0 and total_errors == 0
        status = "PASS" if (coverage_pct >= self.minimum and test_ok) else "FAIL"

        lines: list[str] = []
        lines.append("Test Summary")
        lines.append("=" * 50)
        for suite_name, counts in suites.items():
            pct = counts["passed"] / counts["tests"] * 100 if counts["tests"] else 0.0
            lines.append(
                f"{suite_name:20s} {counts['passed']}/{counts['tests']} passed "
                f"({pct:.0f}% pass, {counts['failures']} fail, {counts['errors']} err, "
                f"{counts['skipped']} skipped)"
            )

        lines.append("")
        lines.append(f"Total tests:  {total_passed}/{total_tests} passed "
                      f"({total_failures} failures, {total_errors} errors)")
        lines.append(f"Coverage:     {coverage_pct:.1f}% (minimum {self.minimum:.0f}%)")
        lines.append(f"Status:       {status}")

        if self.load_metrics:
            lines.append("")
            lines.append("Load Test (per run):")
            for key in (
                "submitted", "completed", "successful", "failed", "timeout",
                "pending", "running", "queue_remaining", "success_rate",
                "failure_rate", "throughput", "drain_time",
                "p50", "p95", "p99",
                "redis_latency_ms", "postgres_latency_ms",
            ):
                if key in self.load_metrics:
                    lines.append(f"  {key:20s} {self.load_metrics[key]}")
            if self.load_test_outcome:
                lines.append(f"  load_test_outcome   {self.load_test_outcome}")

        if self.coverage_detail:
            lines.append("")
            lines.append("Coverage Detail:")
            lines.append(self.coverage_detail)

        report = "\n".join(lines) + "\n"
        return report, status


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minimum", type=float, default=85.0)
    parser.add_argument("--coverage-xml", type=Path, default=Path("coverage.xml"))
    parser.add_argument("--coverage-detail", type=Path, default=Path("reports/coverage-summary.txt"))
    parser.add_argument("--unit-junit", type=Path, default=Path("reports/unit.xml"))
    parser.add_argument("--integration-junit", type=Path, default=Path("reports/integration.xml"))
    parser.add_argument("--e2e-junit", type=Path, default=Path("reports/e2e.xml"))
    parser.add_argument("--chaos-junit", type=Path, default=Path("reports/chaos.xml"))
    parser.add_argument("--concurrency-junit", type=Path, default=Path("reports/concurrency.xml"))
    parser.add_argument("--race-junit", type=Path, default=Path("reports/race.xml"))
    parser.add_argument("--security-junit", type=Path, default=Path("reports/security.xml"))
    parser.add_argument("--observability-junit", type=Path, default=Path("reports/observability.xml"))
    parser.add_argument(
        "--load-json",
        type=Path,
        default=Path("reports/loadtest/workload-bge-m3-run1.json"),
    )
    parser.add_argument("--output", type=Path, default=Path("CHAOS_TEST_REPORT.md"))
    args = parser.parse_args()

    junit_paths: dict[str, Path] = {
        "Unit": args.unit_junit,
        "Integration": args.integration_junit,
        "Concurrency": args.concurrency_junit,
        "Race": args.race_junit,
        "Security": args.security_junit,
        "E2E": args.e2e_junit,
        "Chaos": args.chaos_junit,
    }

    # Observability is included only when its JUnit file exists. This keeps
    # the report accurate for partial/local runs (and the unit-test that does
    # not generate it) without faking a zero-count suite.
    if args.observability_junit.exists():
        junit_paths["Observability"] = args.observability_junit

    load_metrics: dict[str, Any] | None = None
    load_test_outcome: str | None = None
    if args.load_json.exists():
        load_metrics = read_load_metrics(args.load_json)
        load_test_outcome = load_metrics.get("outcome", "unknown")

    gen = ReportGenerator(
        coverage_xml=args.coverage_xml,
        coverage_detail_path=args.coverage_detail if args.coverage_detail.exists() else None,
        junit_paths=junit_paths,
        load_metrics=load_metrics,
        load_test_outcome=load_test_outcome,
        minimum=args.minimum,
    )
    report, status = gen.generate()
    args.output.write_text(report, encoding="utf-8")
    print(report)

    # Exit non-zero when the report is FAIL — this is what makes the
    # report generator a real release gate rather than a decorative printout.
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
