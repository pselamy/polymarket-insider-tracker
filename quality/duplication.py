"""Run the pinned native clone detector with complete source/report receipts."""

import hashlib
import json
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

from quality.duplication_inputs import duplication_inputs, stage_inputs, verify_inputs
from quality.duplication_inventory import Eligible, duplication_inventory
from quality.duplication_report import verify_duplication_report
from quality.security_reports import read_report

VERSION = "jscpd 5.3.3"
MAXIMUM_SECONDS = 300.0
PER_SCAN_SECONDS = 30.0
SCAN_POLICY = {1: ("1", "100"), 50: ("4", "0")}


def native(root: Path, args: list[str], deadline: float) -> str:
    """Every tool error and timeout fails without producing a passing receipt."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("duplication scan exceeded the whole-scan deadline")
    command = ["mise", "--yes", "--locked", "exec", "--", "jscpd", *args]
    result = subprocess.run(
        command,
        cwd=root,
        capture_output=True,
        text=True,
        timeout=min(PER_SCAN_SECONDS, remaining),
    )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, command, result.stdout, result.stderr
        )
    return result.stdout


def scan(
    root: Path,
    config: Path,
    paths: list[Path],
    output: Path,
    minimum: int,
    deadline: float,
) -> object:
    """Use an empty config, explicit format, fixed thresholds and fresh output."""
    output.mkdir(parents=True)
    minimum_lines, threshold = SCAN_POLICY[minimum]
    args = [
        "--config",
        str(config),
        "--mode",
        "mild",
        "--format",
        "python",
        "--min-tokens",
        str(minimum),
        "--min-lines",
        minimum_lines,
        "--threshold",
        threshold,
        "--max-size",
        "9007199254740991",
        "--no-gitignore",
        "--absolute",
        "--reporters",
        "json",
        "--summary",
        "--summary-top",
        str(len(paths)),
        "--workers",
        "1",
        "--no-colors",
        "--no-tips",
        "--output",
        str(output),
        *(str(path) for path in paths),
    ]
    native(root, args, deadline)
    return read_report(output / "jscpd-report.json")


def eligible_inputs(
    root: Path,
    config: Path,
    inputs: dict[str, bytes],
    stage: Path,
    output: Path,
    deadline: float,
) -> dict[str, Eligible]:
    """Ask the native parser whether each enrolled file meets clone minimums."""
    eligible: dict[str, Eligible] = {}
    for index, (name, content) in enumerate(inputs.items()):
        path = stage / name
        report = scan(root, config, [path], output / "inventory" / str(index), 1, deadline)
        item = duplication_inventory(report, path, content)
        if item is not None and item.lines >= 4 and item.tokens >= 50:
            eligible[str(path)] = item
    return eligible


def prepare_output(root: Path) -> Path:
    """Old or redirected reports cannot satisfy this invocation."""
    parent = root / ".quality-results"
    output = parent / "duplication"
    if parent.is_symlink() or output.is_symlink():
        raise ValueError("duplication report directory may not be a symlink")
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)
    return output


def receipt(inputs: dict[str, bytes], eligible: dict[str, Eligible]) -> dict[str, object]:
    """Record source hashes and validated eligible identities for later audit."""
    return {
        "tool": VERSION,
        "sources": {name: hashlib.sha256(content).hexdigest() for name, content in inputs.items()},
        "eligible": sorted(eligible),
    }


def verify_duplication(root: Path) -> None:
    """Fail closed unless the pinned full source scan has a clean native report."""
    output = prepare_output(root)
    inputs = duplication_inputs(root)
    deadline = time.monotonic() + MAXIMUM_SECONDS
    if native(root, ["--version"], deadline).strip() != VERSION:
        raise ValueError("required jscpd version receipt is missing or wrong")
    with tempfile.TemporaryDirectory(prefix="relentless-duplication-") as directory:
        stage = Path(directory)
        config = stage / "empty-config.json"
        config.write_text("{}\n", encoding="utf-8")
        paths = stage_inputs(inputs, stage)
        eligible = eligible_inputs(root, config, inputs, stage, output, deadline)
        report = scan(root, config, paths, output / "combined", 50, deadline)
        verify_duplication_report(report, eligible)
        verify_inputs(inputs, duplication_inputs(root))
        (output / "verified.json").write_text(json.dumps(receipt(inputs, eligible)) + "\n")
