"""Exercise a wheel from an isolated installation, outside the source checkout."""

import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from quality.commands import run
from quality.package_process import TIMEOUT, environment, expect, install_guard

INPUT = "[[3,5],[0,2],[2,4],[8,10]]"
OUTPUT = "[[0,5],[8,10]]\n"
API_PROGRAM = (
    "import json,sys; from relentless_example import normalize; "
    "print(json.dumps(normalize(json.load(sys.stdin)),separators=(',',':')))"
)
TYPE_PROGRAM = (
    "from typing import assert_type\n"
    "from relentless_example import normalize\n"
    "assert_type(normalize([[0, 1]]), list[list[int]])\n"
)


def executable(root: Path, name: str, platform: str) -> Path:
    relative = {"posix": f"bin/{name}", "nt": f"Scripts/{name}.exe"}
    return root / relative[platform]


def installed_consumer(wheel: Path) -> None:
    with TemporaryDirectory(prefix="relentless-python-consumer-") as temporary:
        root = Path(temporary)
        env = environment()
        virtual = root / "environment"
        run(
            [
                "uv",
                "venv",
                str(virtual),
                "--python",
                sys.executable,
                "--no-python-downloads",
            ],
            root,
            TIMEOUT,
            env,
        )
        python = executable(virtual, "python", os.name)
        run(
            [
                "uv",
                "pip",
                "install",
                "--offline",
                "--no-deps",
                "--python",
                str(python),
                str(wheel),
            ],
            root,
            TIMEOUT,
            env,
        )
        install_guard(virtual)
        expect(
            [str(python), "-I", "-W", "error", "-c", API_PROGRAM],
            root,
            INPUT,
            (0, OUTPUT, ""),
        )
        cli = [str(executable(virtual, "relentless-example", os.name))]
        expect(cli, root, INPUT, (0, OUTPUT, ""))
        expect(cli, root, "{", (2, "", "error: invalid JSON\n"))
        expect(
            cli,
            root,
            "[[0,1000001]]",
            (2, "", "error: endpoints must be between -1000000 and 1000000\n"),
        )
        consumer = root / "consumer.py"
        consumer.write_text(TYPE_PROGRAM, encoding="utf-8")
        run(
            [
                sys.executable,
                "-m",
                "mypy",
                "--strict",
                "--no-incremental",
                "--python-executable",
                str(python),
                str(consumer),
            ],
            root,
            TIMEOUT,
            env,
        )
