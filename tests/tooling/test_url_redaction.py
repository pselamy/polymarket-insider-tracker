"""Characterization and property tests for URL credential redaction at both verifier call sites.

Cluster 5 of ``docs/duplication/clusters`` duplicated one URL credential
parser between ``scripts/_service_redaction.py`` (used by
``scripts/runtime_services.py``) and ``scripts/_verify_redaction.py`` (used by
``scripts/verify.py``). The characterization tests pin the observable behaviour
of both entry points; the property tests prove no userinfo or query-password
spelling survives redaction through either caller. Both exercise the public
redaction callables, never the private helpers that moved.
"""

from __future__ import annotations

import importlib.util
import string
from functools import cache
from pathlib import Path
from types import ModuleType
from urllib.parse import quote

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

SCRIPTS = Path(__file__).parents[2] / "scripts"

settings.register_profile(
    "repo",
    derandomize=True,
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("repo")


@cache
def _load(name: str, filename: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _verify() -> ModuleType:
    return _load("verify_url_redaction", "verify.py")


def _services() -> ModuleType:
    return _load("services_url_redaction", "runtime_services.py")


USERINFO_URL = "postgresql+psycopg://tracker:secretpw@localhost:5432/research"
QUERY_URL = "redis://localhost:6379/0?password=querypw"
MIXED_CASE_QUERY_URL = "redis://localhost:6379/0?PASSWORD=QueryPw"
ENCODED_USERINFO_URL = "redis://user:p%40ss@localhost:6379/0"
ENCODED_QUERY_URL = "redis://localhost:6379/?password=se%2Fcret"
OUT_OF_RANGE_PORT_URL = "redis://tracker:pw@localhost:99999/0"
BARE_USERINFO_URL = "redis://tokenonly@h:1/0"


def test_verify_redacts_userinfo_password_and_keeps_endpoint() -> None:
    rendered = _verify().redact_text(f"failed: {USERINFO_URL}", {"DATABASE_URL": USERINFO_URL})

    assert rendered == "failed: postgresql+psycopg://tracker:***@localhost:5432/research"
    assert "secretpw" not in rendered


def test_verify_redacts_query_password() -> None:
    rendered = _verify().redact_text(f"failed: {QUERY_URL}", {"REDIS_URL": QUERY_URL})

    assert rendered == "failed: redis://localhost:6379/0?***"
    assert "querypw" not in rendered


def test_verify_redacts_query_password_case_insensitively() -> None:
    rendered = _verify().redact_text(
        f"failed: {MIXED_CASE_QUERY_URL}", {"REDIS_URL": MIXED_CASE_QUERY_URL}
    )

    assert rendered == "failed: redis://localhost:6379/0?***"
    assert "QueryPw" not in rendered


def test_verify_redacts_percent_encoded_credential_and_its_decoded_form() -> None:
    rendered = _verify().redact_text(
        f"failed: {ENCODED_USERINFO_URL} decoded p@ss",
        {"REDIS_URL": ENCODED_USERINFO_URL},
    )

    assert rendered == "failed: redis://user:***@localhost:6379/0 decoded ***"
    assert "p%40ss" not in rendered
    assert "p@ss" not in rendered


def test_verify_redacts_percent_encoded_query_credential_and_its_decoded_form() -> None:
    rendered = _verify().redact_text(
        f"failed: {ENCODED_QUERY_URL} decoded se/cret",
        {"REDIS_URL": ENCODED_QUERY_URL},
    )

    assert rendered == "failed: redis://localhost:6379/?*** decoded ***"
    assert "se%2Fcret" not in rendered
    assert "se/cret" not in rendered


def test_verify_redacts_malformed_port_url_without_raising() -> None:
    rendered = _verify().redact_text(
        f"could not parse {OUT_OF_RANGE_PORT_URL}", {"REDIS_URL": OUT_OF_RANGE_PORT_URL}
    )

    assert rendered == "could not parse redis://tracker:***@localhost:99999/0"
    assert "pw" not in rendered


def test_verify_masks_unparseable_service_url_completely() -> None:
    rendered = _verify().redact_text(
        "failed: not-a-url-secret", {"DATABASE_URL": "not-a-url-secret"}
    )

    assert rendered == "failed: ***"


def test_verify_masks_non_url_secrets_as_complete_values() -> None:
    webhook = "https://discord.com/api/webhooks/public-id/private-token"
    api_key = "polymarket-private-key"
    rendered = _verify().redact_text(
        f"webhook={webhook} api_key={api_key}",
        {"DISCORD_WEBHOOK_URL": webhook, "POLYMARKET_API_KEY": api_key},
    )

    assert rendered == "webhook=*** api_key=***"
    assert "private-token" not in rendered


def test_verify_leaves_a_bare_userinfo_token_visible() -> None:
    """Characterization: a lone userinfo token has no password to mask under this policy."""
    rendered = _verify().redact_text(
        f"failed: {BARE_USERINFO_URL}", {"REDIS_URL": BARE_USERINFO_URL}
    )

    assert rendered == f"failed: {BARE_USERINFO_URL}"


def test_services_redacts_userinfo_password() -> None:
    rendered = _services()._redact_text(f"failed: {USERINFO_URL}", USERINFO_URL)

    assert rendered == "failed: postgresql+psycopg://tracker:***@localhost:5432/research"
    assert "secretpw" not in rendered


def test_services_redacts_query_password_and_decoded_form() -> None:
    rendered = _services()._redact_text(
        f"failed: {ENCODED_QUERY_URL} decoded se/cret", ENCODED_QUERY_URL
    )

    assert rendered == "failed: redis://localhost:6379/?*** decoded ***"
    assert "se%2Fcret" not in rendered
    assert "se/cret" not in rendered


def test_services_masks_unparseable_url_and_skips_empty_values() -> None:
    module = _services()

    assert module._redact_text("failed: not-a-url-secret", "not-a-url-secret") == "failed: ***"
    assert module._redact_text("nothing secret here", "") == "nothing secret here"


def test_services_masks_every_sensitive_url_argument() -> None:
    module = _services()
    other = "redis://other:otherpw@localhost:6379/0"

    rendered = module._redact_text(f"a={USERINFO_URL} b={other}", USERINFO_URL, other)

    assert "secretpw" not in rendered
    assert "otherpw" not in rendered
    assert rendered == (
        "a=postgresql+psycopg://tracker:***@localhost:5432/research "
        "b=redis://other:***@localhost:6379/0"
    )


@pytest.mark.parametrize(
    "url",
    [
        USERINFO_URL,
        QUERY_URL,
        MIXED_CASE_QUERY_URL,
        ENCODED_USERINFO_URL,
        ENCODED_QUERY_URL,
        OUT_OF_RANGE_PORT_URL,
        "not-a-url-secret",
    ],
)
def test_both_call_sites_render_a_shared_url_identically(url: str) -> None:
    text = f"failed: {url} and tail"

    assert _verify().redact_text(text, {"REDIS_URL": url}) == _services()._redact_text(text, url)


_CREDENTIAL_ALPHABET = string.ascii_letters + string.digits + "._~-@/:?#"
_CREDENTIALS = st.text(alphabet=_CREDENTIAL_ALPHABET, min_size=1, max_size=20)
_PASSWORD_KEYS = st.sampled_from(["password", "PASSWORD", "Password", "pAsSwOrD"])
_HOSTS = st.sampled_from(["localhost", "cache.internal", "10.0.0.5"])
_SCHEMES = st.sampled_from(["redis", "postgresql+psycopg", "rediss"])
_PORTS = st.integers(min_value=1, max_value=65535)
_PATHS = st.sampled_from(["", "/", "/0", "/research/db"])
_USE_USERINFO = st.booleans()
_USE_QUERY = st.booleans()


@st.composite
def _credential_url(draw: st.DrawFn) -> tuple[str, tuple[str, ...]]:
    """Build a URL carrying a userinfo password, a query password, or both."""
    credential = draw(_CREDENTIALS)
    encoded = quote(credential, safe="")
    scheme = draw(_SCHEMES)
    host = draw(_HOSTS)
    port = draw(_PORTS)
    path = draw(_PATHS)
    use_userinfo = draw(_USE_USERINFO)
    use_query = draw(_USE_QUERY)
    if not use_userinfo and not use_query:
        use_userinfo = True
    authority = host if not use_userinfo else f"user:{encoded}@{host}"
    tail = ""
    if use_query:
        tail = f"?{draw(_PASSWORD_KEYS)}={encoded}"
    return f"{scheme}://{authority}:{port}{path}{tail}", (encoded, credential)


@given(url_and_forms=_credential_url())
def test_no_credential_survives_verify_redaction(
    url_and_forms: tuple[str, tuple[str, ...]],
) -> None:
    url, forms = url_and_forms
    encoded, decoded = forms
    text = f"failed: {url} raw={encoded} decoded={decoded}"

    rendered = _verify().redact_text(text, {"DATABASE_URL": url, "REDIS_URL": url})

    for form in set(forms):
        assert form not in rendered


@given(url_and_forms=_credential_url())
def test_no_credential_survives_services_redaction(
    url_and_forms: tuple[str, tuple[str, ...]],
) -> None:
    url, forms = url_and_forms
    encoded, decoded = forms
    text = f"failed: {url} raw={encoded} decoded={decoded}"

    rendered = _services()._redact_text(text, url)

    for form in set(forms):
        assert form not in rendered


@given(url_and_forms=_credential_url())
def test_both_call_sites_agree_on_the_redacted_url(
    url_and_forms: tuple[str, tuple[str, ...]],
) -> None:
    url, _ = url_and_forms
    text = f"failed: {url}"

    assert _verify().redact_text(text, {"REDIS_URL": url}) == _services()._redact_text(text, url)
