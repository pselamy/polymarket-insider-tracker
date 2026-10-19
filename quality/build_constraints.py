"""Bind the protected build hash file to a fresh native export of the lock."""

from pathlib import Path


def verify_build_constraints(root: Path) -> None:
    expected = (root / "quality" / "build-constraints.txt").read_bytes()
    actual = (root / ".quality-results" / "build-constraints.txt").read_bytes()
    if not expected or actual.replace(b"\r\n", b"\n") != expected.replace(b"\r\n", b"\n"):
        raise ValueError("build constraints differ from the fresh locked export")
