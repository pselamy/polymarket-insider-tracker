"""Envelope shape checks of ``parse_v2_page`` for values that never come from decoded JSON."""

from __future__ import annotations

import pytest

from polymarket_insider_tracker.ingestor.v2_page import (
    V2EnvelopeError,
    V2PageRequest,
    parse_v2_page,
)

REQUEST = V2PageRequest(taker_only=False, start=0, end=10)
PAGINATION = {"next_cursor": None, "has_more": False}


def test_rows_are_returned_as_an_immutable_tuple_in_order() -> None:
    rows: list[object] = [{"a": 1}, "x", 3]

    page = parse_v2_page(REQUEST, {"data": rows, "pagination": PAGINATION}, received_at=1.0)

    assert page.rows == ({"a": 1}, "x", 3)


@pytest.mark.parametrize("data", [None, "rows", {"a": 1}, ({"a": 1},), 3])
def test_data_that_is_not_a_list_is_an_envelope_error(data: object) -> None:
    with pytest.raises(V2EnvelopeError, match="data is not a list"):
        parse_v2_page(REQUEST, {"data": data, "pagination": PAGINATION}, received_at=1.0)


NON_STRING_KEYED: dict[object, object] = {1: "x", "data": [], "pagination": PAGINATION}


@pytest.mark.parametrize("body", [NON_STRING_KEYED, [], "x", None])
def test_a_body_that_is_not_a_string_keyed_object_is_an_envelope_error(body: object) -> None:
    with pytest.raises(V2EnvelopeError, match="response is not an object"):
        parse_v2_page(REQUEST, body, received_at=1.0)


def test_pagination_with_a_non_string_key_is_an_envelope_error() -> None:
    pagination: dict[object, object] = {1: "x", "next_cursor": None, "has_more": False}
    body: dict[str, object] = {"data": list[object](), "pagination": pagination}

    with pytest.raises(V2EnvelopeError, match="pagination is not an object"):
        parse_v2_page(REQUEST, body, received_at=1.0)
