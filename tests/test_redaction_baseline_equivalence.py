"""The simplified redaction code behaves exactly as the pre-simplification code did.

``redaction_baseline_main.json`` holds 900 URL-shaped and URL-like strings (all schemes,
userinfo, hosts, ports, bracket, nested-scheme, query and fragment shapes, plus random
punctuation soup) with the outputs the module produced on ``main`` before the mutation
step 2 simplification: ``redact_url``, ``redact_text`` and the known-secret echo masking.
Redaction output must never change silently, so any difference is a failure to explain.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from polymarket_insider_tracker.redaction import (
    redact_text,
    redact_text_with_secrets,
    redact_url,
    url_credential_components,
)

_BASELINE = json.loads(
    (Path(__file__).parent / "data" / "redaction_baseline_main.json").read_text("utf-8")
)


@pytest.mark.parametrize("row", _BASELINE, ids=[str(index) for index in range(len(_BASELINE))])
def test_outputs_match_the_pre_simplification_baseline(row: dict[str, str]) -> None:
    raw = row["input"]
    echo = f"echo {raw} and {raw.replace('/', chr(92) + '/')}"
    assert redact_url(raw) == row["redact_url"]
    assert redact_text(f"echo {raw} end.") == row["redact_text"]
    assert redact_text_with_secrets(echo, url_credential_components(raw)) == row["with_secrets"]
