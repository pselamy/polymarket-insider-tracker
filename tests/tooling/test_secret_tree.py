"""Canonical raw trees are parsed completely, without native normalization."""

from __future__ import annotations

import pytest
from scripts._secret_models import NativeReportError
from scripts._secret_tree import parse_tree
from tests.tooling.secret_source_fixtures import entry

OID = "1" * 40


@pytest.mark.parametrize("mode", [b"100644", b"100755", b"120000", b"40000", b"160000"])
def test_canonical_modes(mode: bytes) -> None:
    (row,) = parse_tree(entry(b"name", OID, mode))
    assert (row.mode, row.name, row.oid) == (mode, b"name", OID)
    assert "name" not in repr(row)


@pytest.mark.parametrize(
    "body",
    [
        b"x",
        b"100644 name",
        b"100644 name\0",
        b"100644 name\0" + b"x" * 19,
        b"100644 name\0" + b"x" * 21,
        b" ",
    ],
)
def test_bad_framing(body: bytes) -> None:
    with pytest.raises(NativeReportError):
        parse_tree(body)


@pytest.mark.parametrize("name", [b"", b".", b"..", b"a/b"])
def test_bad_names(name: bytes) -> None:
    with pytest.raises(NativeReportError):
        parse_tree(entry(name, OID))


@pytest.mark.parametrize("mode", [b"040000", b"0100644", b"100664", b"100600", b"0", b"blob"])
def test_unsupported_or_normalizable_mode_fails(mode: bytes) -> None:
    with pytest.raises(NativeReportError):
        parse_tree(entry(b"name", OID, mode))


@pytest.mark.parametrize(
    "body",
    [
        entry(b"b", OID) + entry(b"a", OID),
        entry(b"a", OID) * 2,
        entry(b"a", OID) + entry(b"a.c", OID) + entry(b"a", OID, b"40000"),
    ],
)
def test_order_and_duplicates_fail(body: bytes) -> None:
    with pytest.raises(NativeReportError):
        parse_tree(body)


def test_directory_order_is_not_plain_name_order() -> None:
    body = entry(b"a.c", OID) + entry(b"a", OID, b"40000") + entry(b"a0", OID)
    assert [row.name for row in parse_tree(body)] == [b"a.c", b"a", b"a0"]
    assert parse_tree(b"") == ()
