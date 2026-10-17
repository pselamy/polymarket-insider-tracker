"""Strict parsing of complete, already hashed native SHA1 tree bytes."""

from __future__ import annotations

from dataclasses import dataclass, field

from scripts._secret_models import NativeReportError

TREE_ENTRIES = 100_000
MODES = frozenset({b"100644", b"100755", b"120000", b"40000", b"160000"})


@dataclass(frozen=True, slots=True)
class TreeEntry:
    mode: bytes
    name: bytes = field(repr=False)
    oid: str

    @property
    def order_key(self) -> bytes:
        return self.name + (b"/" if self.mode == b"40000" else b"\0")


def _name(value: bytes) -> None:
    if not value or value in (b".", b"..") or b"/" in value:
        raise NativeReportError("native tree name") from None


def _entry(payload: bytes, offset: int) -> tuple[TreeEntry, int]:
    space = payload.find(b" ", offset)
    nul = payload.find(b"\0", space + 1)
    if space < offset or nul < space or nul + 21 > len(payload):
        raise NativeReportError("native tree framing") from None
    mode = payload[offset:space]
    if mode not in MODES:
        raise NativeReportError("native tree mode") from None
    name = payload[space + 1 : nul]
    _name(name)
    return TreeEntry(mode, name, payload[nul + 1 : nul + 21].hex()), nul + 21


def _ordered(entry: TreeEntry, previous: bytes, names: set[bytes]) -> None:
    if entry.order_key <= previous or entry.name in names:
        raise NativeReportError("native tree order or duplicate") from None
    names.add(entry.name)


def parse_tree(payload: bytes) -> tuple[TreeEntry, ...]:
    """Consume every entry without decoding or normalizing unrelated sibling names."""
    result: list[TreeEntry] = []
    names: set[bytes] = set()
    offset = 0
    previous = b""
    while offset < len(payload):
        if len(result) == TREE_ENTRIES:
            raise NativeReportError("native tree entry limit") from None
        entry, offset = _entry(payload, offset)
        _ordered(entry, previous, names)
        previous = entry.order_key
        result.append(entry)
    return tuple(result)


def select_entry(payload: bytes, name: bytes) -> TreeEntry:
    for entry in parse_tree(payload):
        if entry.name == name:
            return entry
    raise NativeReportError("native source path absent") from None
