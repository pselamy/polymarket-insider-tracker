"""In-memory fakes and helpers for pipeline and persistence tests."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import SecretStr

from polymarket_insider_tracker.alerter.dispatcher import DispatchResult
from polymarket_insider_tracker.alerter.models import FormattedAlert
from polymarket_insider_tracker.config import (
    DatabaseSettings,
    DetectorSettings,
    DiscordSettings,
    PolygonSettings,
    PolymarketSettings,
    RedisSettings,
    Settings,
    TelegramSettings,
)
from polymarket_insider_tracker.detector.models import (
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
)
from polymarket_insider_tracker.detector.scorer import SignalBundle
from polymarket_insider_tracker.ingestor.models import TradeEvent
from polymarket_insider_tracker.profiler.models import FundingChain


def make_test_settings(
    *,
    dry_run: bool = False,
    persist_assessments: bool = True,
    alert_threshold: float = 0.8,
    discord_enabled: bool = False,
    telegram_enabled: bool = False,
    discord_webhook_url: str | None = None,
    telegram_bot_token: str | None = None,
    telegram_chat_id: str | None = None,
) -> Settings:
    """Create a fully-formed Settings instance without external network calls."""
    discord_kwargs: dict[str, Any] = {"enabled": discord_enabled}
    if discord_webhook_url is not None:
        discord_kwargs["DISCORD_WEBHOOK_URL"] = SecretStr(discord_webhook_url)

    telegram_kwargs: dict[str, Any] = {"enabled": telegram_enabled}
    if telegram_bot_token is not None:
        telegram_kwargs["TELEGRAM_BOT_TOKEN"] = SecretStr(telegram_bot_token)
    if telegram_chat_id is not None:
        telegram_kwargs["TELEGRAM_CHAT_ID"] = telegram_chat_id

    return Settings(
        redis=RedisSettings(url="redis://localhost:6379/0"),
        database=DatabaseSettings(DATABASE_URL="postgresql+psycopg://user:pass@localhost:5432/db"),
        polygon=PolygonSettings(POLYGON_RPC_URL="https://polygon-rpc.com"),
        polymarket=PolymarketSettings(
            ws_url="wss://ws-subscriptions-clob.polymarket.com/ws/market"
        ),
        discord=DiscordSettings(**discord_kwargs),
        telegram=TelegramSettings(**telegram_kwargs),
        detector=DetectorSettings(
            persist_assessments=persist_assessments,
            alert_threshold=alert_threshold,
        ),
        DRY_RUN=dry_run,
    )


class FakeFreshWalletDetector:
    """Fake fresh wallet detector returning a preset signal."""

    def __init__(self, signal: FreshWalletSignal | None = None) -> None:
        self.signal = signal
        self.analyzed_trades: list[TradeEvent] = []

    async def analyze(self, trade: TradeEvent) -> FreshWalletSignal | None:
        self.analyzed_trades.append(trade)
        return self.signal


class FakeSizeAnomalyDetector:
    """Fake size anomaly detector returning a preset signal."""

    def __init__(self, signal: SizeAnomalySignal | None = None) -> None:
        self.signal = signal
        self.analyzed_trades: list[TradeEvent] = []

    async def analyze(self, trade: TradeEvent, **_kwargs: Any) -> SizeAnomalySignal | None:
        self.analyzed_trades.append(trade)
        return self.signal


class SlowDetector:
    """Fake detector that sleeps before returning None."""

    def __init__(self, delay: float = 0.1) -> None:
        self.delay = delay
        self.calls: list[TradeEvent] = []

    async def analyze(self, trade: TradeEvent, **_kwargs: Any) -> None:
        self.calls.append(trade)
        await asyncio.sleep(self.delay)
        return None


class ErrorDetector:
    """Fake detector that raises an exception on analyze."""

    def __init__(self, error: Exception) -> None:
        self.error = error
        self.calls: list[TradeEvent] = []

    async def analyze(self, trade: TradeEvent, **_kwargs: Any) -> None:
        self.calls.append(trade)
        raise self.error


class FakeFundingTracer:
    """Fake funding tracer returning a preset FundingChain."""

    def __init__(self, chain: FundingChain | None = None) -> None:
        self.chain = chain
        self.traced_addresses: list[str] = []

    async def trace(self, target_address: str, **_kwargs: Any) -> FundingChain:
        self.traced_addresses.append(target_address)
        if self.chain is not None:
            return self.chain
        return FundingChain(target_address=target_address)


class FakeRiskScorer:
    """Fake risk scorer returning a RiskAssessment."""

    def __init__(
        self,
        assessment: RiskAssessment | None = None,
        *,
        should_alert: bool = False,
        weighted_score: float = 0.3,
    ) -> None:
        self.assessment = assessment
        self.should_alert = should_alert
        self.weighted_score = weighted_score
        self.assessed_bundles: list[SignalBundle] = []

    async def assess(self, bundle: SignalBundle) -> RiskAssessment:
        self.assessed_bundles.append(bundle)
        if self.assessment is not None:
            return self.assessment
        return RiskAssessment(
            trade_event=bundle.trade_event,
            wallet_address=bundle.wallet_address,
            market_id=bundle.market_id,
            fresh_wallet_signal=bundle.fresh_wallet_signal,
            size_anomaly_signal=bundle.size_anomaly_signal,
            signals_triggered=1,
            weighted_score=self.weighted_score,
            should_alert=self.should_alert,
        )


class FakeAlertFormatter:
    """Fake alert formatter returning a FormattedAlert."""

    def __init__(self, formatted: FormattedAlert | None = None) -> None:
        self.formatted = formatted or FormattedAlert(
            title="Test Alert",
            body="Test Body",
            discord_embed={"title": "Test"},
            telegram_markdown="Test",
            plain_text="Test Alert",
        )
        self.calls: list[RiskAssessment] = []

    def format(self, assessment: RiskAssessment) -> FormattedAlert:
        self.calls.append(assessment)
        return self.formatted


class FakeAlertDispatcher:
    """Fake alert dispatcher recording calls and returning preset DispatchResult."""

    def __init__(self, result: DispatchResult | None = None) -> None:
        self.result = result or DispatchResult(
            success_count=1,
            failure_count=0,
            channel_results={"discord": True},
        )
        self.dispatched: list[FormattedAlert] = []

    async def dispatch(self, alert: FormattedAlert) -> DispatchResult:
        self.dispatched.append(alert)
        return self.result


class BrokenDatabaseManager:
    """Fake database manager that fails when getting an async session."""

    def __init__(self, error: Exception | None = None) -> None:
        self.calls = 0
        self.error = error or RuntimeError("DB connection failed")

    def get_async_session(self) -> Any:
        self.calls += 1
        raise self.error
