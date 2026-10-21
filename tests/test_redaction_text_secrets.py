"""Text scanning and known-secret echo masking, pinned by exact output.

``url_credential_components`` exists so a server echoing a credential in a partial or
re-encoded spelling is still masked; each case echoes one spelling and requires the mask.
"""

from __future__ import annotations

import pytest

from polymarket_insider_tracker.redaction import (
    MASK,
    is_scannable_url_text,
    redact_failure_with_secrets,
    redact_text,
    redact_text_with_secrets,
    url_credential_components,
)


@pytest.mark.parametrize(
    ("text", "masked"),
    [
        ("see (https://h/key)", "see (https://h/***path***)"),
        ("see https://h/key.", "see https://h/***path***."),
        ("see 'https://h/key'", "see 'https://h/***path***'"),
        ("see [https://h/key];", "see [https://h/***path***];"),
        ("see {https://h/key}!?", "see {https://h/***path***}!?"),
        ("see https://h:80,", "see https://h:80,"),
        ("no url here", "no url here"),
    ],
)
def test_redact_text_keeps_trailing_prose_delimiters(text: str, masked: str) -> None:
    assert redact_text(text) == masked


@pytest.mark.parametrize("char", [" ", "\t", "\n", "\x01", '"', "<", ">"])
def test_scan_boundary_characters_are_not_scannable(char: str) -> None:
    assert not is_scannable_url_text(f"https://h/a{char}b")


@pytest.mark.parametrize(
    "value", ["https://h/a'b", "https://h/aXb", "https://h/a%20b", "https://h/é"]
)
def test_other_printable_characters_stay_scannable(value: str) -> None:
    assert is_scannable_url_text(value)


def _echo(url: str, echoed: str) -> str:
    return redact_text_with_secrets(f"server said: {echoed} end", url_credential_components(url))


@pytest.mark.parametrize(
    ("url", "echoed"),
    [
        ("https://user:p%2Fw@h/", "p/w"),
        ("https://user:p%2Fw@h/", "p%2Fw"),
        ("https://h/?token=a/b", "a%2Fb"),
        ("https://h/?token=a/b", "a\\/b"),
        ("https://h/seg%20ret/x", "seg ret"),
        ("https://h/?token=SECRETVALUE&mode=fast", "token=SECRETVALUE"),
        ("https://h/?fl+ag=", "fl ag"),
        ("https://h/?token=a+b", "a b"),
        ("https://h/?first=second=third", "second=third"),
        ("https://h/?flag=&key=value1", "flag"),
        ("https://h/#frag", "frag"),
        ("https://h/?key=ab%2fcd", "ab%2fcd"),
    ],
)
def test_credential_spellings_are_masked(url: str, echoed: str) -> None:
    assert _echo(url, echoed) == f"server said: {MASK} end"


def test_query_value_echo_does_not_mask_unrelated_tokens() -> None:
    url = "https://h/?firstkey=secondvalue=thirdpart"
    assert _echo(url, "secondvalue alone") == "server said: secondvalue alone end"
    assert _echo(url, "thirdpart") == "server said: thirdpart end"


def test_raw_pair_is_masked_as_one_unit() -> None:
    url = "https://h/?token=SECRETVALUE&mode=fast"
    assert _echo(url, "token=SECRETVALUE") == f"server said: {MASK} end"


def test_whitespace_only_component_is_never_replaced() -> None:
    assert _echo("https://h/?k=+", "a b") == "server said: a b end"


def test_failure_rendering_masks_message_secret() -> None:
    exc = RuntimeError("upstream rejected key SECRETKEY at https://h/SECRETKEY")
    assert redact_failure_with_secrets(exc, ("SECRETKEY",)) == (
        f"upstream rejected key {MASK} at https://h/***path***"
    )


def test_failure_rendering_scrubs_bytes_arguments_before_text_masking() -> None:
    exc = RuntimeError(b"SECRETKEY")
    assert redact_failure_with_secrets(exc, ("SECRETKEY",)) == MASK


def test_failure_rendering_uses_the_given_secrets_only() -> None:
    assert redact_failure_with_secrets(RuntimeError("keep OTHER"), ("SECRETKEY",)) == "keep OTHER"
