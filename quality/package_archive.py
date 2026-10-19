"""Read bounded native archives without extracting paths into the filesystem."""

import gzip
import io
import stat
import tarfile
import zipfile
from copy import copy
from pathlib import Path

COMPRESSED_BUDGET = 65536
COMPRESSED_READ_LIMIT = 65537
CONTENT_BUDGET = 262144
TAR_BUDGET = 524288
TAR_READ_LIMIT = 524289
FILE_MODE = 0o644
REGULAR_TYPES = {0, stat.S_IFREG}


def compressed(path: Path) -> bytes:
    with path.open("rb") as source:
        data = source.read(COMPRESSED_READ_LIMIT)
    if len(data) > COMPRESSED_BUDGET:
        raise ValueError("compressed package exceeds size budget")
    return data


def insert(files: dict[str, bytes], name: str, data: bytes) -> None:
    if name in files:
        raise ValueError("duplicate archive members")
    files[name] = data


def content_size(sizes: list[int]) -> None:
    if sum(sizes) > CONTENT_BUDGET:
        raise ValueError("unpacked package exceeds size budget")


def wheel_files(path: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    data = compressed(path)
    canonical = io.BytesIO()
    with (
        zipfile.ZipFile(io.BytesIO(data)) as archive,
        zipfile.ZipFile(canonical, "w") as rebuilt,
    ):
        members = archive.infolist()
        content_size([member.file_size for member in members])
        for member in members:
            wheel_mode(member)
            insert(files, member.filename, archive.read(member))
            rebuilt.writestr(copy(member), files[member.filename])
    if canonical.getvalue() != data:
        raise ValueError("wheel container has noncanonical or unaccounted bytes")
    return files


def wheel_mode(member: zipfile.ZipInfo) -> None:
    if member.extra or member.comment:
        raise ValueError("wheel member metadata is outside public policy")
    mode = member.external_attr >> 16
    if member.is_dir() or stat.S_IFMT(mode) not in REGULAR_TYPES:
        raise ValueError("wheel members must be regular files")
    if stat.S_IMODE(mode) != FILE_MODE:
        raise ValueError("archive file permissions differ from public policy")


def tar_bytes(path: Path) -> bytes:
    with gzip.GzipFile(fileobj=io.BytesIO(compressed(path))) as source:
        data = source.read(TAR_READ_LIMIT)
    if len(data) > TAR_BUDGET:
        raise ValueError("raw TAR exceeds size budget")
    return data


def sdist_files(path: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    data = tar_bytes(path)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        members = archive.getmembers()
        content_size([member.size for member in members])
        for member in members:
            sdist_member(archive, member, files)
        tar_container(data, members)
    return files


def tar_container(data: bytes, members: list[tarfile.TarInfo]) -> None:
    end = 0
    for member in members:
        end = tar_member_end(data, member, end)
    if len(data) % 512:
        raise ValueError("source TAR termination contains unaccounted bytes")
    tail = io.BytesIO(data[end:])
    if tail.read(1024) != bytes(1024) or any(tail.read()):
        raise ValueError("source TAR termination contains unaccounted bytes")


def tar_member_end(data: bytes, member: tarfile.TarInfo, previous: int) -> int:
    if member.offset != previous:
        raise ValueError("source TAR has unaccounted header bytes")
    if member.offset_data - member.offset != 512:
        raise ValueError("source TAR has unaccounted extended header bytes")
    end = member.offset_data + ((member.size + 511) // 512) * 512
    if any(data[member.offset_data + member.size : end]):
        raise ValueError("source TAR file padding contains unaccounted bytes")
    return end


def sdist_member(
    archive: tarfile.TarFile, member: tarfile.TarInfo, files: dict[str, bytes]
) -> None:
    if member.type not in {tarfile.REGTYPE, tarfile.AREGTYPE} or member.pax_headers:
        raise ValueError("source archive members must be plain regular files")
    if member.mode != FILE_MODE:
        raise ValueError("archive file permissions differ from public policy")
    source = archive.extractfile(member)
    if source is None:
        raise ValueError("native TAR reader omitted a regular file")
    with source:
        insert(files, member.name, source.read())
