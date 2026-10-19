"""Bind the public README example to safe structured native commands."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from quality import package_contents

BUILD = ["mise", "--yes", "--locked", "exec", "--", "uv", "build"]
RUN = [
    "mise",
    "--yes",
    "--locked",
    "exec",
    "--",
    "uv",
    "run",
    "--locked",
]
INPUT = "[[5,8],[1,3],[2,6]]"
FILES = ("pyproject.toml", "uv.lock", "mise.toml", "mise.lock", "README.md", "LICENSE")


def reviewed_script() -> str:
    """Use the enrolled package entry point, including in renamed templates."""
    entry = package_contents.ENTRY_POINTS.decode("ascii")
    match = re.fullmatch(
        r"\[console_scripts\]\n([a-z][a-z0-9-]*) = ([a-z_][a-z0-9_]*)\.cli:main\n",
        entry,
    )
    if match is None or match.group(2) != package_contents.MODULE:
        raise ValueError("reviewed console script policy is malformed")
    return match.group(1)


def example_command(script: str) -> str:
    """Keep the documented command fixed except for its reviewed script name."""
    return (
        "mise --yes --locked exec -- uv build\n"
        "printf '[[5,8],[1,3],[2,6]]' | "
        f"mise --yes --locked exec -- uv run --locked {script}"
    )


def example_section(text: str) -> str:
    """The public example must have one unambiguous Markdown section."""
    heading = "## Example behavior\n"
    if text.count(heading) != 1:
        raise ValueError("README example heading is missing or duplicated")
    return text.partition(heading)[2].partition("\n## ")[0]


def expected_example(text: str, script: str) -> str:
    """Refuse changed commands before running any documentation code."""
    section = example_section(text)
    blocks = re.findall(r"(?ms)^```sh\n(.*?)^```$", section)
    if blocks != [example_command(script) + "\n"]:
        raise ValueError("README example commands differ from reviewed arguments")
    outputs: list[str] = re.findall(r"Output is `([^`]+)` followed by a newline\.", section)
    if len(outputs) != 1:
        raise ValueError("README example output claim is missing or duplicated")
    return next(iter(outputs)) + "\n"


def stage_example(root: Path, stage: Path) -> None:
    """The example runs from copied package inputs, never the checkout."""
    for name in FILES:
        shutil.copy2(root / name, stage / name)
    shutil.copytree(root / "src", stage / "src")


def command(
    root: Path, args: list[str], input_text: str | None
) -> subprocess.CompletedProcess[str]:
    """Run fixed argv without a shell and without runtime downloads."""
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", UV_PYTHON_DOWNLOADS="never")
    return subprocess.run(
        args,
        cwd=root,
        env=env,
        input=input_text,
        capture_output=True,
        text=True,
        timeout=120,
    )


def verify_readme_example(root: Path) -> None:
    """Build and run the validated README example in an isolated copy."""
    script = reviewed_script()
    expected = expected_example((root / "README.md").read_text(encoding="utf-8"), script)
    with tempfile.TemporaryDirectory(prefix="relentless-readme-") as directory:
        stage = Path(directory)
        stage_example(root, stage)
        built = command(stage, BUILD, None)
        if built.returncode != 0:
            raise subprocess.CalledProcessError(built.returncode, BUILD, built.stdout, built.stderr)
        actual = command(stage, [*RUN, script], INPUT)
        if actual.returncode != 0 or actual.stdout != expected:
            raise ValueError("README example exit status or output differs")
