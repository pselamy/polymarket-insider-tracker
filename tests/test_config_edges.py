"""Configuration validation edges: unusable Redis hosts and blank webhook values."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from polymarket_insider_tracker.config import DiscordSettings, RedisSettings


@pytest.mark.parametrize("url", ["redis://", "redis:///0", "redis://:6379/0"])
def test_redis_url_without_a_host_is_rejected(url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDIS_URL", url)

    with pytest.raises(ValidationError, match="valid hostname"):
        RedisSettings()


def test_blank_webhook_url_leaves_discord_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "   ")

    settings = DiscordSettings()

    assert not settings.enabled
