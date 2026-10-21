"""Shape validation of the redis-py ``XREADGROUP`` reply."""

from __future__ import annotations

import pytest

from polymarket_insider_tracker.ingestor._stream_reply import reply_batches, stream_row


def test_reply_batches_returns_each_streams_entries() -> None:
    reply = [["trades", [("1-0", {"a": "b"})]], ("other", list[object]())]

    assert reply_batches(reply) == [[("1-0", {"a": "b"})], []]


@pytest.mark.parametrize(
    "reply",
    [
        "trades",
        {"trades": list[object]()},
        [["trades"]],
        [["trades", list[object](), "extra"]],
        [["trades", "not-a-list"]],
        ["not-a-pair"],
        [["trades", list[object]()], 7],
    ],
)
def test_reply_batches_rejects_malformed_replies(reply: object) -> None:
    assert reply_batches(reply) is None


def test_stream_row_keeps_bytes_and_str_content() -> None:
    assert stream_row((b"1-0", {b"k": b"v", "s": "t"})) == (b"1-0", {b"k": b"v", "s": "t"})


def test_stream_row_reports_a_missing_field_mapping_as_a_tombstone() -> None:
    assert stream_row(("1-0", None)) == ("1-0", None)


def test_stream_row_keeps_an_empty_field_mapping() -> None:
    assert stream_row(("1-0", dict[str, str]())) == ("1-0", dict[str, str]())


@pytest.mark.parametrize(
    "entry",
    [
        "1-0",
        ("1-0",),
        ("1-0", dict[str, str](), "extra"),
        (1, {"k": "v"}),
        (None, {"k": "v"}),
        ("1-0", ["k", "v"]),
        ("1-0", {1: "v"}),
        ("1-0", {"k": 1}),
        ("1-0", {"k": None}),
    ],
)
def test_stream_row_rejects_malformed_rows(entry: object) -> None:
    assert stream_row(entry) is None
