from __future__ import annotations

import json
from pathlib import Path

from scripts.report_coverage import (
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


def test_report_coverage_main_writes_summary(tmp_path: Path, monkeypatch) -> None:
    coverage_xml = tmp_path / "coverage.xml"
    coverage_xml.write_text('<coverage line-rate="0.873"><packages /></coverage>', encoding="utf-8")

    unit_xml = tmp_path / "unit.xml"
    unit_xml.write_text('<testsuite tests="10" failures="0" errors="0" skipped="0" />', encoding="utf-8")
    integration_xml = tmp_path / "integration.xml"
    integration_xml.write_text('<testsuite tests="5" failures="0" errors="0" skipped="0" />', encoding="utf-8")
    e2e_xml = tmp_path / "e2e.xml"
    e2e_xml.write_text('<testsuite tests="3" failures="0" errors="0" skipped="0" />', encoding="utf-8")
    chaos_xml = tmp_path / "chaos.xml"
    chaos_xml.write_text('<testsuite tests="2" failures="0" errors="0" skipped="0" />', encoding="utf-8")

    load_json = tmp_path / "load.json"
    load_json.write_text(
        json.dumps(
            {
                "metrics": {
                    "throughput": 241.8,
                    "p50": 0.72,
                    "p95": 3.21,
                    "p99": 7.11,
                    "error_rate": 0.02,
                    "retry_rate": 0.05,
                    "queue_depth": 7,
                }
            }
        ),
        encoding="utf-8",
    )

    output = tmp_path / "CHAOS_TEST_REPORT.md"
    monkeypatch.setattr(
        "sys.argv",
        [
            "report_coverage.py",
            "--minimum",
            "85",
            "--coverage-xml",
            str(coverage_xml),
            "--unit-junit",
            str(unit_xml),
            "--integration-junit",
            str(integration_xml),
            "--e2e-junit",
            str(e2e_xml),
            "--chaos-junit",
            str(chaos_xml),
            "--load-json",
            str(load_json),
            "--output",
            str(output),
        ],
    )

    exit_code = main()

    assert exit_code == 0
    report = output.read_text(encoding="utf-8")
    assert "Coverage:     87.3%" in report
    assert "Unit:        10 passed" in report
    assert "Throughput:   241.8 tasks/sec" in report
