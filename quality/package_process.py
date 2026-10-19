"""Bound consumer commands and keep checkout import overrides out of children."""

import os
import subprocess
import sys
from pathlib import Path

TIMEOUT = 120


PLATFORM = {
    "PATH",
    "PATHEXT",
    "SystemRoot",
    "WINDIR",
    "ComSpec",
    "LD_LIBRARY_PATH",
    "MISE_DATA_DIR",
    "MISE_CACHE_DIR",
    "MISE_STATE_DIR",
    "UV_CACHE_DIR",
    "UV_PYTHON_INSTALL_DIR",
    "UV_PYTHON",
}

GUARD = """\
import os
import pathlib
import socket
import subprocess
import sys

_root = pathlib.Path(os.environ["RLCI_CONSUMER_ROOT"]).resolve()

def _deny(*_args, **_kwargs):
    raise PermissionError("installed consumer network or child process denied")

socket.socket = _deny
socket.create_connection = _deny
subprocess.Popen = _deny
os.system = _deny

def _inside(value):
    path = pathlib.Path(value).resolve()
    if path != _root and _root not in path.parents:
        raise PermissionError("installed consumer write outside temporary root denied")

def _audit(event, args):
    if event == "open":
        path, mode, flags = args
        if isinstance(path, (str, bytes, os.PathLike)) and (
            (isinstance(mode, str) and any(mark in mode for mark in "wax+"))
            or flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)
        ):
            _inside(path)
    elif event in {
        "os.remove", "os.mkdir", "os.rmdir", "os.chmod", "os.chown",
        "os.truncate", "os.utime",
    }:
        _inside(args[0])
    elif event in {"os.rename", "os.link", "os.symlink"}:
        _inside(args[0])
        _inside(args[1])

sys.addaudithook(_audit)
"""


def site_packages(virtual: Path, platform: str) -> Path:
    version = f"python{sys.version_info.major}.{sys.version_info.minor}"
    locations = {
        "nt": "Lib/site-packages",
        "posix": f"lib/{version}/site-packages",
    }
    return virtual / locations[platform]


def install_guard(virtual: Path) -> None:
    site = site_packages(virtual, os.name)
    if not site.is_dir():
        raise ValueError("isolated consumer site-packages is missing")
    guard = site / "sitecustomize.py"
    if guard.exists():
        raise ValueError("installed consumer already owns sitecustomize")
    guard.write_text(GUARD, encoding="utf-8")


def environment(root: "Path | None" = None) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key in PLATFORM}
    if root is not None:
        for key in (
            "HOME",
            "USERPROFILE",
            "APPDATA",
            "LOCALAPPDATA",
            "TMPDIR",
            "TEMP",
            "TMP",
        ):
            env[key] = str(root)
        env["RLCI_CONSUMER_ROOT"] = str(root)
    env["PYTHONNOUSERSITE"] = "1"
    env["PYTHONWARNINGS"] = "error"
    return env


def expect(command: list[str], root: Path, document: str, wanted: tuple[int, str, str]) -> None:
    result = subprocess.run(
        command,
        cwd=root,
        env=environment(root),
        input=document,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=TIMEOUT,
        check=False,
    )
    if (result.returncode, result.stdout, result.stderr) != wanted:
        raise ValueError("installed consumer exit status or output differs")
