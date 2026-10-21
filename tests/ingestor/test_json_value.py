"""Re-typing decoded values as JSON values."""

import pytest

from polymarket_insider_tracker.ingestor._json_value import json_value


def test_json_value_keeps_decoded_json_and_rejects_everything_else() -> None:
    decoded: object = {"a": [1, 2.5, "x", None, True], "b": {"c": {}}}

    assert json_value(decoded) == decoded
    with pytest.raises(TypeError, match="not a JSON value: tuple"):
        json_value((1, 2))
    with pytest.raises(TypeError, match="not a JSON value: dict"):
        json_value({1: "a"})
    with pytest.raises(TypeError, match="not a JSON value: object"):
        json_value({"a": [object()]})
