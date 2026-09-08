#!/usr/bin/env python3
# Sync from cursor-workspaces/agent-test-workflow/scripts/summarize_junit.py
"""JUnit XML → agent-friendly summary.md + failed.list (pytest + vitest)."""

from __future__ import annotations

import argparse
import html
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class TestFailure:
    identifier: str
    file_path: str
    test_name: str
    classname: str
    message: str
    traceback: str
    rerun_command: str


def _text(elem: ET.Element | None) -> str:
    if elem is None:
        return ""
    return (elem.text or "").strip()


def _attr(elem: ET.Element, name: str, default: str = "") -> str:
    return elem.get(name, default) or default


def _parse_counts(root: ET.Element) -> dict[str, int]:
    counts = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    if root.tag == "testsuites":
        for suite in root.findall("testsuite"):
            for key in counts:
                counts[key] += int(_attr(suite, key, "0") or "0")
    elif root.tag == "testsuite":
        for key in counts:
            counts[key] = int(_attr(root, key, "0") or "0")
    return counts


def _pytest_node_id(classname: str, name: str) -> str:
    parts = classname.split(".")
    class_name = None
    if len(parts) >= 2 and parts[-1][:1].isupper() and not parts[-1].isupper():
        class_name = parts[-1]
        module_parts = parts[:-1]
    else:
        module_parts = parts
    file_path = "/".join(module_parts) + ".py"
    if class_name:
        return f"{file_path}::{class_name}::{name}"
    return f"{file_path}::{name}"


def _pytest_rerun(node_id: str) -> str:
    return f"./run_pytest_docker.sh {node_id} -q --tb=short"


def _vitest_rerun(file_path: str, test_name: str) -> str:
    escaped = test_name.replace('"', '\\"')
    return f'yarn vitest run {file_path} -t "{escaped}"'


def _extract_traceback_from_log(log_text: str, needle: str, limit: int = 30) -> str:
    if not log_text or not needle:
        return ""
    idx = log_text.find(needle)
    if idx == -1:
        # Try last 80 chars of test name for partial match
        short = needle[-80:] if len(needle) > 80 else needle
        idx = log_text.find(short)
    if idx == -1:
        return ""
    snippet = log_text[idx : idx + 8000]
    lines = snippet.splitlines()[:limit]
    return "\n".join(lines).strip()


def _failure_details(
    testcase: ET.Element, runner: str, log_text: str
) -> TestFailure | None:
    failure_elem = testcase.find("failure")
    error_elem = testcase.find("error")
    problem = failure_elem if failure_elem is not None else error_elem
    if problem is None:
        return None

    classname = _attr(testcase, "classname")
    name = _attr(testcase, "name")
    message = _attr(problem, "message") or _text(problem)
    body = _text(problem)
    traceback = body or _extract_traceback_from_log(log_text, name)

    if runner == "pytest":
        node_id = _pytest_node_id(classname, name)
        file_path = node_id.split("::")[0]
        identifier = node_id
        rerun = _pytest_rerun(node_id)
    else:
        file_path = classname or name.split(" > ")[0] if " > " in name else classname
        test_name = html.unescape(name)
        identifier = f"{file_path}::{test_name}" if file_path else test_name
        rerun = _vitest_rerun(file_path, test_name)

    return TestFailure(
        identifier=identifier,
        file_path=file_path,
        test_name=name,
        classname=classname,
        message=message.splitlines()[0][:500] if message else "(no message)",
        traceback=traceback[:4000],
        rerun_command=rerun,
    )


def parse_failures(
    junit_path: Path, runner: str, log_text: str
) -> tuple[dict[str, int], list[TestFailure]]:
    tree = ET.parse(junit_path)
    root = tree.getroot()
    counts = _parse_counts(root)

    failures: list[TestFailure] = []
    suites: list[ET.Element]
    if root.tag == "testsuites":
        suites = root.findall("testsuite")
    else:
        suites = [root]

    for suite in suites:
        for testcase in suite.findall("testcase"):
            detail = _failure_details(testcase, runner, log_text)
            if detail is not None:
                failures.append(detail)

    return counts, failures


def render_summary(
    runner: str, counts: dict[str, int], failures: list[TestFailure]
) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    passed = counts["tests"] - counts["failures"] - counts["errors"] - counts["skipped"]
    lines = [
        f"# {runner} summary — {ts}",
        "",
        f"**Result:** {counts['failures']} failed, {counts['errors']} errors, "
        f"{passed} passed, {counts['skipped']} skipped ({counts['tests']} total)",
        "",
    ]

    if not failures:
        lines.append("All tests passed.")
        return "\n".join(lines) + "\n"

    lines.extend(["## Failures", ""])
    for i, failure in enumerate(failures, 1):
        lines.append(f"### {i}. `{failure.identifier}`")
        lines.append("")
        if failure.message:
            lines.append(f"**Message:** {failure.message}")
            lines.append("")
        if failure.traceback:
            lines.append("```")
            lines.append(failure.traceback)
            lines.append("```")
            lines.append("")
        lines.append(f"**Re-run:** `{failure.rerun_command}`")
        lines.append("")

    lines.extend(["## Batch re-run", ""])
    if runner == "pytest" and failures:
        lines.append("```")
        lines.append("./run_pytest_docker.sh --lf -q --tb=short")
        lines.append("```")
        lines.append("")
        lines.append("Or individually from `test-results/failed.list`.")
    elif runner == "vitest" and failures:
        lines.append(
            "Re-run each file/test from `test-results/failed.list` or use the commands above."
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize JUnit XML for agent workflows.")
    parser.add_argument("junit_xml", type=Path, help="Path to junit.xml")
    parser.add_argument("--runner", choices=["pytest", "vitest"], required=True)
    parser.add_argument("--log", type=Path, default=None, help="Full test log for traceback extraction")
    parser.add_argument("--out-dir", type=Path, default=None, help="Write summary.md and failed.list here")
    args = parser.parse_args()

    if not args.junit_xml.is_file():
        print(f"error: junit xml not found: {args.junit_xml}", file=sys.stderr)
        return 2

    log_text = args.log.read_text(encoding="utf-8", errors="replace") if args.log and args.log.is_file() else ""
    counts, failures = parse_failures(args.junit_xml, args.runner, log_text)
    summary = render_summary(args.runner, counts, failures)

    out_dir = args.out_dir or args.junit_xml.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = out_dir / "summary.md"
    failed_path = out_dir / "failed.list"

    summary_path.write_text(summary, encoding="utf-8")
    failed_path.write_text(
        "\n".join(f.rerun_command for f in failures) + ("\n" if failures else ""),
        encoding="utf-8",
    )

    failed_count = counts["failures"] + counts["errors"]
    print(f"wrote {summary_path} ({failed_count} failed/error)", file=sys.stderr)
    return 1 if failed_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
