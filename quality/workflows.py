"""Run pinned native workflow and shell audits over explicit authored paths."""

import json
import subprocess
from pathlib import Path

from quality.pipeline import command
from quality.report_data import record, text
from quality.security_reports import verify_empty


def workflow_paths(root: Path) -> list[str]:
    """Git ignores cannot omit a workflow; unsupported file kinds fail."""
    directory = root / ".github" / "workflows"
    if (root / ".github").is_symlink() or directory.is_symlink():
        raise ValueError("workflow directory cannot be a symlink")
    paths = [workflow_path(path) for path in sorted(directory.iterdir())]
    if not paths:
        raise ValueError("workflow inventory must be nonempty")
    return paths


def workflow_path(path: Path) -> str:
    """Only regular YAML workflows are supported in the native collection."""
    if path.is_symlink() or not path.is_file() or path.suffix not in {".yml", ".yaml"}:
        raise ValueError("workflow directory must contain regular YAML workflows")
    return str(path.resolve())


def policy(root: Path) -> dict[str, object]:
    """Require exact policy keys; native commands use argument-array validation."""
    value: object = json.loads(
        (root / "quality" / "workflow-commands.json").read_text(encoding="utf-8")
    )
    result = record(value)
    if set(result) != {"shellcheck", "shellcheckVersion", "actionlint", "zizmor"}:
        raise ValueError("workflow command policy keys are incomplete or unknown")
    return result


def tool(root: Path, args: list[str]) -> str:
    """Native failures/timeouts propagate; invalid UTF-8 is never repaired."""
    return subprocess.check_output(
        ["mise", "--yes", "--locked", "exec", "--", *args],
        cwd=root,
        timeout=30,
        stdin=subprocess.DEVNULL,
    ).decode("utf-8")


def verify_workflows(root: Path) -> None:
    """A successful report needs current full-scope native checks and no findings."""
    output = root / ".quality-results" / "workflows-security.json"
    output.unlink(missing_ok=True)
    paths = workflow_paths(root)
    commands = policy(root)
    version = tool(root, command(commands["shellcheck"]))
    if f'version: {text(commands["shellcheckVersion"])}' not in version.splitlines():
        raise ValueError("required ShellCheck version receipt is missing or wrong")
    tool(root, [*command(commands["actionlint"]), *paths])
    report = tool(root, [*command(commands["zizmor"]), *paths])
    value: object = json.loads(report)
    verify_empty(value)
    output.parent.mkdir(exist_ok=True)
    output.write_text(report, encoding="utf-8")
