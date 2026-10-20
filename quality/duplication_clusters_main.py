"""Print the generated duplication report; with ``--check`` fail on stale cluster files."""

import argparse
import subprocess
import tempfile
import time
from pathlib import Path

from quality.duplication import VERSION, native, scan
from quality.duplication_clusters import clones_from_report, load_clusters, problems
from quality.duplication_clusters_report import render
from quality.duplication_inputs import stage_inputs
from quality.report_data import record
from quality.security_reports import read_report
from quality.source_scope import discover, validate_path

CLUSTER_DIRECTORY = Path("docs/duplication/clusters")
THRESHOLD_EXIT_CODE = 1
MAXIMUM_SECONDS = 300.0


def authored(root: Path) -> dict[str, bytes]:
    """The authored scope without the 399-line size hook, as docs/duplication/README.md says."""
    paths = sorted(path for path in discover(root) if path.suffix == ".py")
    for path in paths:
        validate_path(root, path)
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in paths}


def measure(root: Path, stage: Path, deadline: float) -> object:
    """jscpd exits 1 under the threshold-0 policy whenever a clone exists; read the report."""
    config = stage / "empty-config.json"
    config.write_text("{}\n", encoding="utf-8")
    paths = stage_inputs(authored(root), stage)
    output = stage / "report"
    try:
        return scan(root, config, paths, output, 50, deadline)
    except subprocess.CalledProcessError as error:
        if error.returncode != THRESHOLD_EXIT_CODE:
            raise
        return read_report(output / "jscpd-report.json")


def run(root: Path, check: bool) -> int:
    deadline = time.monotonic() + MAXIMUM_SECONDS
    if native(root, ["--version"], deadline).strip() != VERSION:
        raise ValueError("required jscpd version receipt is missing or wrong")
    with tempfile.TemporaryDirectory(prefix="duplication-report-") as directory:
        report = measure(root, Path(directory), deadline)
        clones = clones_from_report(report, f"{directory}/")
    clusters = load_clusters(root / CLUSTER_DIRECTORY)
    statistics = record(record(record(report)["statistics"])["total"])
    print(render(clusters, clones, statistics))
    found = problems(clusters, clones) if check else []
    for line in found:
        print(f"duplication cluster check failed: {line}")
    return 1 if found else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail on stale cluster files")
    raise SystemExit(run(Path.cwd(), parser.parse_args().check))


if __name__ in {"__main__"}:
    main()
