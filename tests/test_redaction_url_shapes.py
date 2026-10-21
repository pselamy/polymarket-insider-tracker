"""Exact ``redact_url`` output for every fail-closed shape and query/userinfo rule.

Each row pins the full masked string, so a weakened rule re-emitting any credential
position (port token, path, query key, userinfo) changes the output and fails.
"""

from __future__ import annotations

import pytest

from polymarket_insider_tracker.redaction import redact_url

_SHAPES = [
    # unparseable input: bare placeholder, readable scheme only when plain ASCII alphanumerics
    ("http://[::1/x://abc", "http://***path***"),
    ("//[::1", "***path***"),
    ("é://[::1", "***path***"),
    ("http://[]/", "http://***path***"),
    # malformed or hostless netloc
    ("http://:80/", "http://***path***"),
    ("http://u@/p", "http://***path***"),
    ("http://u@/p?k=v#frag", "http://***path***"),
    ("ht\ttp://u@/p", "http://***path***"),
    ("http://host//x", "http://host/***path***"),
    ("http://host:/p", "http://host:/***path***"),
    # invalid or out-of-range port: the port-position token is never re-emitted
    ("ws+x://host:bad/p", "host/***path***"),
    ("https://[::1]:key/p", "https://***path***"),
    ("http://host:99999/", "http://host/***path***"),
    ("http://u@host:KEY/", "http://host/***path***"),
    ("http://:key/", "http://***path***"),
    ("http://u@:key/", "http://***path***"),
    ("http://hóst:KEY/", "http://***path***"),
    ("http://ho-st.example:KEY/", "http://ho-st.example/***path***"),
    ("http://host:65535/p", "http://host:65535/***path***"),
    ("http://host:0/p", "http://host:0/***path***"),
    # nested scheme / smuggled userinfo: outer host kept only when provably safe
    ("wss://https://user:pw@host/p", "wss://***path***"),
    ("http://u:p@host/a@b", "http://host/***path***"),
    ("http://u@host:80/a@b?k=v", "http://host:80/***path***"),
    ("http://u@hóst/a@b", "http://***path***"),
    ("http://u@h.o-s_t/a@b", "http://***path***"),
    ("http://u@host:/a@b", "http://***path***"),
    ("http://u@host:65535/a@b", "http://host:65535/***path***"),
    ("http://host/x@y://z?k=v", "http://host/***path***"),
    ("http://host/a://b?x=1", "http://host/***path***?x=***"),
    ("http://host/a@b?x=1", "http://host/***path***?x=***"),
    # userinfo
    ("http://a@b@host/", "http://***@host/"),
    ("http://u:p@host/", "http://u:***@host/"),
    ("http://tok@host/", "http://***@host/"),
    ("http://u:@host/", "http://u:***@host/"),
    # query
    ("http://h:80/?a=&b=c", "http://h:80/?***&b=***"),
    ("http://h/?SECRET&b=c", "http://h/?***&b=***"),
    ("http://h/?SECRET", "http://h/?***"),
    ("http://h/?&", "http://h/?***"),
    ("http://h/?a=1&b=2", "http://h/?a=***&b=***"),
    ("http://h/?k%26x=v&b=c", "http://h/?***&b=***"),
    ("http://h/?k%3Dx=v&b=c", "http://h/?***&b=***"),
    ("http://h/?k%3Fx=v&b=c", "http://h/?***&b=***"),
    ("http://h/?k%23x=v&b=c", "http://h/?***&b=***"),
    ("http://h/?k%25x=v&b=c", "http://h/?***&b=***"),
    ("http://h/#frag", "http://h/#***"),
]


@pytest.mark.parametrize(("raw", "masked"), _SHAPES)
def test_redact_url_exact_masked_output(raw: str, masked: str) -> None:
    assert redact_url(raw) == masked
