from __future__ import annotations

import argparse
import sys
from pathlib import Path
from xml.etree import ElementTree as ET


def read_coverage_percentage(path: Path) -> float:
    tree = ET.parse(path)
    root = tree.getroot()
    line_rate = float(root.attrib["line-rate"])
    return line_rate * 100.0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minimum", type=float, default=85.0)
    parser.add_argument("--xml", type=Path, default=Path("coverage.xml"))
    parser.add_argument("--output", type=Path, default=Path("CHAOS_TEST_REPORT.md"))
    args = parser.parse_args()

    coverage = read_coverage_percentage(args.xml)
    status = "PASS" if coverage >= args.minimum else "FAIL"
    args.output.write_text(
        "\n".join(
            [
                "Test Summary",
                "------------",
                f"Coverage:     {coverage:.1f}%",
                f"Threshold:    {args.minimum:.1f}%",
                f"Status:       {status}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(f"Coverage: {coverage:.1f}%")
    if coverage < args.minimum:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

