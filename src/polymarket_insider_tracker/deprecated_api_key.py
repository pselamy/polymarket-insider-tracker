"""Deprecation handling for ``POLYMARKET_API_KEY``, which no request ever carried."""

from __future__ import annotations

import warnings
from typing import Annotated

from pydantic import AfterValidator, SecretStr


class ApiKeySettingDeprecationWarning(UserWarning):
    """``POLYMARKET_API_KEY`` is deprecated and ignored."""


def api_key_deprecation_message() -> str:
    """The actionable, value-free text emitted when the ignored API key setting is set."""
    return (
        "POLYMARKET_API_KEY is deprecated and ignored: every CLOB and trades request the tracker "
        "makes is public and anonymous, and no credential is sent. Remove it; support for the "
        "setting will be dropped in a later release."
    )


def _warn_about_ignored_api_key(value: SecretStr) -> SecretStr:
    """Accept the legacy key with the deprecation warning; the value is never printed."""
    warnings.warn(api_key_deprecation_message(), ApiKeySettingDeprecationWarning, stacklevel=2)
    return value


DeprecatedApiKey = Annotated[SecretStr, AfterValidator(_warn_about_ignored_api_key)]
