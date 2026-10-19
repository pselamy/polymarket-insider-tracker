"""Build fresh public archives and validate both independently installed forms."""

import shutil
import sys
import tomllib
from pathlib import Path
from tempfile import TemporaryDirectory

from quality.commands import run
from quality.package_consumer import installed_consumer
from quality.package_contents import verify_sdist, verify_wheel
from quality.package_metadata import identity
from quality.package_process import TIMEOUT, environment
from quality.report_data import record


def public_stage(root: Path, stage: Path) -> None:
    shutil.copytree(root / "src", stage / "src", ignore=shutil.ignore_patterns("__pycache__"))
    for name in ("README.md", "LICENSE", "pyproject.toml", "uv.lock"):
        shutil.copyfile(root / name, stage / name)


def native_build(source: Path, output: Path, constraints: Path, root: Path) -> None:
    run(
        [
            "uv",
            "build",
            str(source),
            *(["--wheel"] if source.is_file() else []),
            "--build-constraints",
            str(constraints),
            "--require-hashes",
            "--python",
            sys.executable,
            "--no-python-downloads",
            "--no-create-gitignore",
            "--out-dir",
            str(output),
        ],
        root,
        TIMEOUT,
        environment(),
    )


def verify_packages(root: Path) -> None:
    output = root / ".quality-results/dist"
    shutil.rmtree(output, ignore_errors=True)
    project = record(tomllib.loads((root / "pyproject.toml").read_text())["project"])
    name = identity(project)
    with TemporaryDirectory(prefix="relentless-python-build-") as temporary:
        stage = Path(temporary)
        public_stage(root, stage)
        dist = stage / "dist"
        constraints = root / "quality/build-constraints.txt"
        native_build(stage, dist, constraints, stage)
        wheel = dist / (name + "-py3-none-any.whl")
        source = dist / (name + ".tar.gz")
        verify_wheel(wheel, stage, project)
        verify_sdist(source, stage, project)
        installed_consumer(wheel)
        rebuilt = stage / "rebuilt"
        native_build(source, rebuilt, constraints, stage)
        rebuilt_wheel = rebuilt / wheel.name
        verify_wheel(rebuilt_wheel, stage, project)
        if wheel.read_bytes() != rebuilt_wheel.read_bytes():
            raise ValueError("source-built wheel differs from the original artifact")
        installed_consumer(rebuilt_wheel)
        shutil.copytree(dist, output)
