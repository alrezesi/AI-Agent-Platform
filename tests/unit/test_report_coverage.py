from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.report_coverage import (
    ReportGenerator,
    main,
    read_coverage_percentage,
    read_junit_summary,
    read_load_metrics,
)


def test_read_coverage_percentage(tmp_path: Path) -> None:
    coverage_xml = tmp_path / "coverage.xml"
    coverage_xml.write_text('<coverage line-rate="0.873"><packages /></coverage>', encoding="utf-8")

    assert read_coverage_percentage(coverage_xml) == 87.3


def test_read_junit_summary(tmp_path: Path) -> None:
    junit_xml = tmp_path / "unit.xml"
    junit_xml.write_text('<testsuite tests="10" failures="2" errors="1" skipped="3" />', encoding="utf-8")

    assert read_junit_summary(junit_xml) == {
        "tests": 10,
        "failures": 2,
        "errors": 1,
        "skipped": 3,
        "passed": 4,
    }


def test_read_load_metrics(tmp_path: Path) -> None:
    load_json = tmp_path / "load.json"
    payload = {"metrics": {"throughput": 241.8, "p95": 3.21}}
    load_json.write_text(json.dumps(payload), encoding="utf-8")

    assert read_load_metrics(load_json) == payload["metrics"]


# ---------------------------------------------------------------------------
# ReportGenerator — the single source of truth for CHAOS_TEST_REPORT.md
# ---------------------------------------------------------------------------


def _make_junit(path: Path, tests: int, failures: int = 0, errors: int = 0, skipped: int = 0) -> None:
    path.write_text(
        f'<testsuite tests="{tests}" failures="{failures}" errors="{errors}" skipped="{skipped}" />',
        encoding="utf-8",
    )


def _write_all_junit(tmp_path: Path, fail: bool = False) -> dict[str, Path]:
    """Write a complete set of JUnit XML files for all suites.

    When *fail* is True the Unit suite has 3 real failures, which must make
    the overall status FAIL regardless of coverage.
    """
    paths: dict[str, Path] = {}
    if fail:
        _make_junit(tmp_path / "unit.xml", tests=10, failures=3, errors=0, skipped=0)
    else:
        _make_junit(tmp_path / "unit.xml", tests=10, failures=0, errors=0, skipped=0)
    _make_junit(tmp_path / "integration.xml", tests=5, failures=0, errors=0, skipped=0)
    _make_junit(tmp_path / "e2e.xml", tests=3, failures=0, errors=0, skipped=0)
    _make_junit(tmp_path / "chaos.xml", tests=2, failures=0, errors=0, skipped=0)
    _make_junit(tmp_path / "concurrency.xml", tests=5, failures=0, errors=0, skipped=0)
    _make_junit(tmp_path / "race.xml", tests=3, failures=0, errors=0, skipped=0)
    _make_junit(tmp_path / "security.xml", tests=4, failures=0, errors=0, skipped=0)
    paths["Unit"] = tmp_path / "unit.xml"
    paths["Integration"] = tmp_path / "integration.xml"
    paths["E2E"] = tmp_path / "e2e.xml"
    paths["Chaos"] = tmp_path / "chaos.xml"
    paths["Concurrency"] = tmp_path / "concurrency.xml"
    paths["Race"] = tmp_path / "race.xml"
    paths["Security"] = tmp_path / "security.xml"
    return paths


def test_report_generator_passes_when_all_green(tmp_path: Path) -> None:
    """All suites pass and coverage >= minimum → Status: PASS."""
    (tmp_path / "coverage.xml").write_text(
        '<coverage line-rate="0.90"><packages /></coverage>', encoding="utf-8"
    )
    (tmp_path / "coverage-summary.txt").write_text("TOTAL 85.0%", encoding="utf-8")
    junit_paths = _write_all_junit(tmp_path, fail=False)

    gen = ReportGenerator(
        coverage_xml=tmp_path / "coverage.xml",
        coverage_detail_path=tmp_path / "coverage-summary.txt",
        junit_paths=junit_paths,
        load_metrics=None,
        load_test_outcome=None,
        minimum=85.0,
    )
    report, status = gen.generate()
    assert status == "PASS"
    assert "Status:       PASS" in report
    assert "Total tests:  32/32 passed" in report


def test_report_generator_fails_when_suite_has_failures(tmp_path: Path) -> None:
    """A JUnit XML with real failures must produce Status: FAIL with
    correct, non-matching numbers — not a hardcoded PASS."""
    (tmp_path / "coverage.xml").write_text(
        '<coverage line-rate="0.95"><packages /></coverage>', encoding="utf-8"
    )
    (tmp_path / "coverage-summary.txt").write_text("TOTAL 95.0%", encoding="utf-8")
    junit_paths = _write_all_junit(tmp_path, fail=True)

    gen = ReportGenerator(
        coverage_xml=tmp_path / "coverage.xml",
        coverage_detail_path=tmp_path / "coverage-summary.txt",
        junit_paths=junit_paths,
        load_metrics=None,
        load_test_outcome=None,
        minimum=85.0,
    )
    report, status = gen.generate()

    # Coverage is 95 % — well above the 85 % gate — yet the presence of
    # 3 failures must flip the status to FAIL.  This is the exact
    # regression test for the hardcoded-PASS bug.
    assert status == "FAIL"
    assert "Status:       FAIL" in report
    # The summary line must show the REAL pass count (not total_tests twice).
    assert "Total tests:  29/32 passed" in report
    # The failure/error counts must be surfaced, not hidden.
    assert "3 failures" in report
    assert "0 errors" in report


def test_report_generator_fails_when_coverage_below_minimum(tmp_path: Path) -> None:
    """Coverage below the gate must produce Status: FAIL even with 0 failures."""
    (tmp_path / "coverage.xml").write_text(
        '<coverage line-rate="0.70"><packages /></coverage>', encoding="utf-8"
    )
    junit_paths = _write_all_junit(tmp_path, fail=False)

    gen = ReportGenerator(
        coverage_xml=tmp_path / "coverage.xml",
        coverage_detail_path=None,
        junit_paths=junit_paths,
        load_metrics=None,
        load_test_outcome=None,
        minimum=85.0,
    )
    report, status = gen.generate()
    assert status == "FAIL"
    assert "Status:       FAIL" in report
    assert "Coverage:     70.0%" in report


def test_report_coverage_main_writes_summary(tmp_path: Path, monkeypatch) -> None:
    """End-to-end test of main(): writes a report and exits 0 on PASS."""
    coverage_xml = tmp_path / "coverage.xml"
    coverage_xml.write_text('<coverage line-rate="0.90"><packages /></coverage>', encoding="utf-8")
    (tmp_path / "coverage-summary.txt").write_text("TOTAL 90.0%", encoding="utf-8")

    junit_paths = _write_all_junit(tmp_path, fail=False)

    load_json = tmp_path / "load.json"
    load_json.write_text(
        json.dumps({
            "metrics": {
                "throughput": 241.8,
                "p50": 0.72,
                "p95": 3.21,
                "p99": 7.11,
                "failure_rate": 0.02,
                "retry_rate": 0.05,
                "queue_depth": 7,
            }
        }),
        encoding="utf-8",
    )

    output = tmp_path / "CHAOS_TEST_REPORT.md"
    monkeypatch.setattr(
        "sys.argv",
        [
            "report_coverage.py",
            "--minimum", "85",
            "--coverage-xml", str(coverage_xml),
            "--coverage-detail", str(tmp_path / "coverage-summary.txt"),
            "--unit-junit", str(junit_paths["Unit"]),
            "--integration-junit", str(junit_paths["Integration"]),
            "--e2e-junit", str(junit_paths["E2E"]),
            "--chaos-junit", str(junit_paths["Chaos"]),
            "--concurrency-junit", str(junit_paths["Concurrency"]),
            "--race-junit", str(junit_paths["Race"]),
            "--security-junit", str(junit_paths["Security"]),
            "--observability-junit", str(tmp_path / "does-not-exist.xml"),
            "--load-json", str(load_json),
            "--output", str(output),
        ],
    )

    exit_code = main()

    assert exit_code == 0
    report = output.read_text(encoding="utf-8")
    assert "Coverage:     90.0%" in report
    assert "Status:       PASS" in report
    assert "Throughput" in report or "throughput" in report
    assert "Total tests:  32/32 passed" in report


def test_report_coverage_main_exits_nonzero_on_failure(tmp_path: Path, monkeypatch) -> None:
    """Feed a fake JUnit XML with a real failure; main() must exit 1 and
    the report must say FAIL with correct non-matching numbers."""
    coverage_xml = tmp_path / "coverage.xml"
    coverage_xml.write_text('<coverage line-rate="0.95"><packages /></coverage>', encoding="utf-8")
    (tmp_path / "coverage-summary.txt").write_text("TOTAL 95.0%", encoding="utf-8")

    junit_paths = _write_all_junit(tmp_path, fail=True)

    load_json = tmp_path / "load.json"
    load_json.write_text(
        json.dumps({
            "metrics": {
                "throughput": 241.8,
                "p50": 0.72,
                "p95": 3.21,
                "p99": 7.11,
                "failure_rate": 0.02,
                "retry_rate": 0.05,
                "queue_depth": 7,
            }
        }),
        encoding="utf-8",
    )

    output = tmp_path / "CHAOS_TEST_REPORT.md"
    monkeypatch.setattr(
        "sys.argv",
        [
            "report_coverage.py",
            "--minimum", "85",
            "--coverage-xml", str(coverage_xml),
            "--coverage-detail", str(tmp_path / "coverage-summary.txt"),
            "--unit-junit", str(junit_paths["Unit"]),
            "--integration-junit", str(junit_paths["Integration"]),
            "--e2e-junit", str(junit_paths["E2E"]),
            "--chaos-junit", str(junit_paths["Chaos"]),
            "--concurrency-junit", str(junit_paths["Concurrency"]),
            "--race-junit", str(junit_paths["Race"]),
            "--security-junit", str(junit_paths["Security"]),
            "--observability-junit", str(tmp_path / "does-not-exist.xml"),
            "--load-json", str(load_json),
            "--output", str(output),
        ],
    )

    exit_code = main()

    assert exit_code == 1
    report = output.read_text(encoding="utf-8")
    assert "Status:       FAIL" in report
    # Must show the real numbers, not total_tests/total_tests.
    assert "Total tests:  29/32 passed" in report
    assert "3 failures" in report
