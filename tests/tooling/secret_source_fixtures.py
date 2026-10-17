"""Owned, disposable native Git objects; no network, hooks or user configuration."""

from __future__ import annotations

import os
import shutil
import subprocess
import zlib
from dataclasses import dataclass
from pathlib import Path

from scripts._secret_identity import SourceScope
from scripts._secret_models import NativeFinding, ScanMode
from scripts._secret_process import GitLocation


@dataclass(frozen=True)
class ObjectRepo:
    directory: Path
    executable: Path

    def git(self, *arguments: str, payload: bytes = b"") -> bytes:
        return subprocess.run(
            [str(self.executable), "--git-dir", str(self.directory), *arguments],
            input=payload,
            capture_output=True,
            check=True,
            timeout=10,
            env={
                "PATH": os.defpath,
                "LC_ALL": "C",
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_CONFIG_SYSTEM": os.devnull,
            },
        ).stdout

    def write(self, kind: str, body: bytes) -> str:
        return self.git("hash-object", "-w", "-t", kind, "--stdin", payload=body).decode().strip()

    def commit(self, tree: str, parent: str = "") -> str:
        ancestry = f"parent {parent}\n" if parent else ""
        body = (
            f"tree {tree}\n{ancestry}author Fixture <fixture@example.invalid> 1 +0000\n"
            "committer Fixture <fixture@example.invalid> 1 +0000\n\nfixture\n"
        )
        return self.write("commit", body.encode())

    def corrupt(self, oid: str, kind: str, body: bytes) -> None:
        target = self.directory / "objects" / oid[:2] / oid[2:]
        target.chmod(0o600)
        target.write_bytes(zlib.compress(f"{kind} {len(body)}\0".encode() + body))

    @property
    def location(self) -> GitLocation:
        return GitLocation(self.executable, self.directory)


def repository(directory: Path, object_format: str = "sha1") -> ObjectRepo:
    executable = Path(shutil.which("git") or "/usr/bin/git").resolve()
    repo = ObjectRepo(directory, executable)
    repo.git("init", "--bare", f"--object-format={object_format}", str(directory))
    return repo


def entry(name: bytes, oid: str, mode: bytes = b"100644") -> bytes:
    return mode + b" " + name + b"\0" + bytes.fromhex(oid)


def single_file(repo: ObjectRepo, body: bytes = b"fixture\n", name: bytes = b"note.py") -> str:
    blob = repo.write("blob", body)
    tree = repo.write("tree", entry(name, blob))
    return repo.commit(tree)


def source_scope(head: str, *retained: str) -> SourceScope:
    return SourceScope(17, "a" * 64, head, frozenset((head, *retained)))


def finding(head: str, mode: ScanMode = ScanMode.HISTORY, path: str = "note.py") -> NativeFinding:
    commit = head if mode is ScanMode.HISTORY else ""
    prefix = f"{commit}:" if commit else ""
    return NativeFinding(
        mode,
        commit,
        path,
        "fixture-rule",
        f"{prefix}{path}:fixture-rule:1",
        1,
        1,
        1,
        9,
        ("fixture",),
    )
