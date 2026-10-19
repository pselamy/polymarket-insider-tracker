"""Bind package metadata and wheel RECORD to the public project and file bytes."""

import base64
import csv
import hashlib
import io
from email.parser import Parser

from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import Version

from quality.report_data import text


def identity(project: dict[str, object]) -> str:
    name = canonicalize_name(text(project["name"])).replace("-", "_")
    return f"{name}-{Version(text(project['version']))}"


def metadata(data: bytes, project: dict[str, object]) -> None:
    message = Parser().parsestr(data.decode("utf-8"))
    for field, key in (
        ("Name", "name"),
        ("Version", "version"),
        ("Summary", "description"),
        ("License-Expression", "license"),
    ):
        if message.get_all(field, []) != [text(project[key])]:
            raise ValueError("package metadata differs from public project")
    if message.get_all("License-File", []) != ["LICENSE"]:
        raise ValueError("package metadata must identify its license file")
    if message.get_all("Requires-Dist", []):
        raise ValueError("initial sample package must remain dependency-free")
    python_support(message.get_all("Requires-Python", []), project)


def python_support(requires: list[str], project: dict[str, object]) -> None:
    try:
        (requirement,) = requires
    except ValueError as error:
        raise ValueError("package requires one Python support declaration") from error
    if SpecifierSet(requirement) != SpecifierSet(text(project["requires-python"])):
        raise ValueError("package Python support differs from public project")


def wheel_metadata(data: bytes) -> None:
    message = Parser().parsestr(data.decode("utf-8"))
    for field, value in (
        ("Wheel-Version", "1.0"),
        ("Root-Is-Purelib", "true"),
        ("Tag", "py3-none-any"),
    ):
        if message.get_all(field, []) != [value]:
            raise ValueError("wheel format or compatibility tags are unsupported")


def record_bytes(files: dict[str, bytes], record_name: str) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    for name, data in files.items():
        if name != record_name:
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest())
            writer.writerow([name, "sha256=" + digest.decode().rstrip("="), len(data)])
    writer.writerow([record_name, "", ""])
    return buffer.getvalue().encode()


def verify_record(files: dict[str, bytes], record_name: str) -> None:
    expected = list(csv.reader(io.StringIO(record_bytes(files, record_name).decode())))
    actual = list(csv.reader(io.StringIO(files[record_name].decode("utf-8"))))
    if sorted(actual) != sorted(expected):
        raise ValueError("wheel RECORD differs from actual archived file hashes")
