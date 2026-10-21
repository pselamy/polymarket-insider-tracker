"""Checked shapes for the decoded dictionaries behind the ingestor models."""

import pytest

from polymarket_insider_tracker.ingestor._model_shapes import iso_text


def test_iso_text_returns_text_and_rejects_other_kinds() -> None:
    assert iso_text("2024-01-01T00:00:00") == "2024-01-01T00:00:00"
    with pytest.raises(TypeError, match="ISO timestamp string, not int"):
        iso_text(17)
