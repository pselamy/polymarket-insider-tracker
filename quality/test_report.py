"""Require a fresh, complete native pytest JSON report with only passed tests."""

import json
from pathlib import Path

from quality.report_data import array, record, text

PASSED = "passed"
TEST_ROOTS = {"tests"}
TEST_ITEM_TYPES = {"Function", "TestCaseFunction"}
SUMMARY_COUNTS = {"collected", "total", "passed"}


def report_path(root: Path) -> Path:
    return root / ".quality-results" / "tests.json"


def prepare_tests(root: Path) -> None:
    report_path(root).parent.mkdir(exist_ok=True)
    report_path(root).unlink(missing_ok=True)


def integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("test counts and exit status must be integers")
    return value


def passed(value: object) -> dict[str, object]:
    item = record(value)
    if item["outcome"] != PASSED:
        raise ValueError("every test stage and collector must pass")
    return item


def identifiers(tests: list[object]) -> set[str]:
    result: set[str] = set()
    for value in tests:
        item = passed(value)
        node = text(item["nodeid"])
        if node in result:
            raise ValueError("duplicate test identifiers")
        result.add(node)
        for stage in ("setup", "call", "teardown"):
            passed(item[stage])
    return result


def summary(value: object, count: int) -> None:
    item = record(value)
    for field in ("collected", "total", "passed"):
        if integer(item[field]) != count:
            raise ValueError("test summary disagrees with completed results")
    extra_counts(item)


def extra_counts(item: dict[str, object]) -> None:
    for field, number in item.items():
        if field not in SUMMARY_COUNTS and integer(number) != 0:
            raise ValueError("unsuccessful or deselected tests are forbidden")


def session(item: dict[str, object], root: Path) -> None:
    if integer(item["exitcode"]) != 0:
        raise ValueError("test session must exit successfully")
    if Path(text(item["root"])).resolve() != root.resolve():
        raise ValueError("test report root differs from requested repository")
    if array(item.get("warnings", [])):
        raise ValueError("test warnings are forbidden")


def verify_files(nodes: set[str], expected: list[Path], root: Path) -> None:
    actual = {(root / node.split("::")[0]).resolve() for node in nodes}
    wanted = expected_paths(expected)
    if actual != wanted:
        raise ValueError("test file inventory differs from discovered sources")


def expected_paths(paths: list[Path]) -> set[Path]:
    result: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved in result:
            raise ValueError("test file inventory differs from discovered sources")
        result.add(resolved)
    return result


def collected_nodes(value: object) -> list[str]:
    children = array(passed(value)["result"])
    result: list[str] = []
    for child in children:
        item = record(child)
        if text(item["type"]) in TEST_ITEM_TYPES:
            result.append(text(item["nodeid"]))
    return result


def verify_collectors(value: object, completed: set[str]) -> None:
    collectors = array(value)
    if not collectors:
        raise ValueError("test collector inventory is empty")
    nodes = [node for collector in collectors for node in collected_nodes(collector)]
    verify_collected_ids(nodes, completed)


def verify_collected_ids(nodes: list[str], completed: set[str]) -> None:
    collected: set[str] = set()
    for node in nodes:
        if node in collected:
            raise ValueError("duplicate collected test identifiers")
        collected.add(node)
    if collected != completed:
        raise ValueError("collected test identifiers differ from completed results")


def verify_test_report(value: object, expected: list[Path], root: Path) -> None:
    item = record(value)
    session(item, root)
    tests = array(item["tests"])
    if not tests:
        raise ValueError("test inventory is empty")
    nodes = identifiers(tests)
    verify_files(nodes, expected, root)
    summary(item["summary"], len(tests))
    verify_collectors(item["collectors"], nodes)


def verify_tests(root: Path, sources: list[Path]) -> None:
    expected = [
        path
        for path in sources
        if path.relative_to(root).parts[0] in TEST_ROOTS
        and (path.name.startswith("test_") or path.name.endswith("_test.py"))
    ]
    value: object = json.loads(report_path(root).read_text(encoding="utf-8"))
    verify_test_report(value, expected, root)
