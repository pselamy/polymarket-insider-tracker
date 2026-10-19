"""Property tests: no URL credential spelling survives either verifier's redaction.

The scripts' shared URL policy hides a userinfo password and any ``password``
query parameter, in both the raw and percent-decoded spelling. These properties
drive arbitrary credentials (mixed-case keys, percent-encoded values) through
both entry points and assert that neither spelling is re-emitted, including when
a tool echoes the credential on its own.
"""

from __future__ import annotations

import importlib.util
import string
from functools import cache
from pathlib import Path
from types import ModuleType
from urllib.parse import quote

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
    return _load("verify_url_redaction_properties", "verify.py")


def _services() -> ModuleType:
    return _load("services_url_redaction_properties", "runtime_services.py")


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
