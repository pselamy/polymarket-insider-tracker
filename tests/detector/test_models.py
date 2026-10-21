"""Characterization tests for the detector signal models' public surface.

These tests pin the exact published payload and key order of every signal
model's ``to_dict``, the shared confidence-threshold and trade-identity
property behaviour, and the delivery/observation defaults, so the duplicated
knowledge recorded for cluster 7 in docs/duplication/clusters can move to
a single home without any observable change.
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polymarket_insider_tracker.detector.models import (
    SCORING_ALGORITHM_VERSION,
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
    SniperClusterSignal,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile

FIXED_TIMESTAMP = datetime(2026, 1, 15, 12, 30, 45, tzinfo=UTC)
TradeAnchoredBuilder = Callable[[TradeEvent | None], FreshWalletSignal | SizeAnomalySignal]
ConfidenceBuilder = Callable[..., FreshWalletSignal | SizeAnomalySignal | SniperClusterSignal]


def build_trade_event() -> TradeEvent:
    """Return a trade event whose serialized values are exact and deterministic."""
    return TradeEvent(
        market_id="market_abc123",
        trade_id="tx_001",
        wallet_address="0xabc123def4567890",
        side="BUY",
        outcome="Yes",
        outcome_index=0,
        price=Decimal("0.65"),
        size=Decimal("10000"),
        timestamp=FIXED_TIMESTAMP,
        asset_id="token_123",
    )


def build_wallet_profile(age_hours: float | None = 48.0) -> WalletProfile:
    """Return a wallet profile whose serialized values are exact and deterministic."""
    return WalletProfile(
        address="0xabc123def4567890",
        nonce=3,
        first_seen=None,
        age_hours=age_hours,
        is_fresh=True,
        total_tx_count=3,
        matic_balance=Decimal("1000000000000000000"),
        usdc_balance=Decimal("1000000000"),
        analyzed_at=FIXED_TIMESTAMP,
        fresh_threshold=5,
    )


def build_market_metadata() -> MarketMetadata:
    """Return market metadata whose serialized category is deterministic."""
    return MarketMetadata(
        condition_id="market_abc123",
        question="Will it rain tomorrow?",
        description="Weather prediction market",
        tokens=(Token(token_id="token_123", outcome="Yes", price=Decimal("0.65")),),
        category="science",
    )


def build_fresh_wallet_signal(
    trade: TradeEvent | None = None, confidence: float = 0.75
) -> FreshWalletSignal:
    """Return a fresh-wallet signal with exact, deterministic values."""
    return FreshWalletSignal(
        trade_event=trade if trade is not None else build_trade_event(),
        wallet_profile=build_wallet_profile(),
        confidence=confidence,
        factors={"freshness": 0.5},
        timestamp=FIXED_TIMESTAMP,
    )


def build_size_anomaly_signal(
    trade: TradeEvent | None = None, confidence: float = 0.8
) -> SizeAnomalySignal:
    """Return a size-anomaly signal with exact, deterministic values."""
    return SizeAnomalySignal(
        trade_event=trade if trade is not None else build_trade_event(),
        market_metadata=build_market_metadata(),
        volume_impact=0.05,
        book_impact=0.1,
        is_niche_market=True,
        confidence=confidence,
        factors={"volume_impact": 0.5},
        timestamp=FIXED_TIMESTAMP,
    )


def build_sniper_cluster_signal(confidence: float = 0.85) -> SniperClusterSignal:
    """Return a sniper-cluster signal with exact, deterministic values."""
    return SniperClusterSignal(
        wallet_address="0xsniperwallet",
        cluster_id="cluster_001",
        cluster_size=5,
        avg_entry_delta_seconds=30.0,
        markets_in_common=3,
        confidence=confidence,
        timestamp=FIXED_TIMESTAMP,
    )


class TestSignalSerializationShape:
    """Pin the exact published payload and key order of every signal model."""

    def test_fresh_wallet_signal_to_dict_exact_payload(self) -> None:
        """The serialized fresh-wallet signal has exactly these keys, values, and order."""
        result = build_fresh_wallet_signal().to_dict()

        expected = {
            "wallet_address": "0xabc123def4567890",
            "market_id": "market_abc123",
            "trade_id": "tx_001",
            "trade_size": "6500.00",
            "trade_side": "BUY",
            "trade_price": "0.65",
            "wallet_nonce": 3,
            "wallet_age_hours": 48.0,
            "wallet_is_fresh": True,
            "confidence": 0.75,
            "factors": {"freshness": 0.5},
            "timestamp": "2026-01-15T12:30:45+00:00",
        }
        assert result == expected
        assert list(result) == list(expected)

    def test_fresh_wallet_signal_to_dict_with_unknown_wallet_age(self) -> None:
        """An unknown wallet age serializes as None, not as a stringified placeholder."""
        signal = FreshWalletSignal(
            trade_event=build_trade_event(),
            wallet_profile=build_wallet_profile(age_hours=None),
            confidence=0.75,
            factors={},
            timestamp=FIXED_TIMESTAMP,
        )

        result = signal.to_dict()

        assert result["wallet_age_hours"] is None
        assert result["factors"] == {}

    def test_size_anomaly_signal_to_dict_exact_payload(self) -> None:
        """The serialized size-anomaly signal has exactly these keys, values, and order."""
        result = build_size_anomaly_signal().to_dict()

        expected = {
            "wallet_address": "0xabc123def4567890",
            "market_id": "market_abc123",
            "trade_id": "tx_001",
            "trade_size": "6500.00",
            "trade_side": "BUY",
            "trade_price": "0.65",
            "market_category": "science",
            "volume_impact": 0.05,
            "book_impact": 0.1,
            "is_niche_market": True,
            "confidence": 0.8,
            "factors": {"volume_impact": 0.5},
            "timestamp": "2026-01-15T12:30:45+00:00",
        }
        assert result == expected
        assert list(result) == list(expected)

    def test_sniper_cluster_signal_to_dict_exact_payload(self) -> None:
        """The serialized sniper-cluster signal has exactly these keys, values, and order."""
        result = build_sniper_cluster_signal().to_dict()

        expected = {
            "wallet_address": "0xsniperwallet",
            "cluster_id": "cluster_001",
            "cluster_size": 5,
            "avg_entry_delta_seconds": 30.0,
            "markets_in_common": 3,
            "confidence": 0.85,
            "timestamp": "2026-01-15T12:30:45+00:00",
        }
        assert result == expected
        assert list(result) == list(expected)

    def test_risk_assessment_to_dict_exact_payload_with_signals(self) -> None:
        """The serialized assessment with both signals and a recorded delivery outcome."""
        trade = build_trade_event()
        assessment = RiskAssessment(
            trade_event=trade,
            wallet_address="0xabc123def4567890",
            market_id="market_abc123",
            fresh_wallet_signal=build_fresh_wallet_signal(trade),
            size_anomaly_signal=build_size_anomaly_signal(trade),
            signals_triggered=2,
            weighted_score=0.72,
            should_alert=True,
            assessment_id="assessment_001",
            timestamp=FIXED_TIMESTAMP,
            delivery_disposition="delivered",
            delivery_channels="discord",
            dry_run=False,
            volume_available=True,
            market_daily_volume=Decimal("12345.50"),
            book_depth_available=False,
            wallet_tx_count=7,
            wallet_age_known=True,
            scoring_algorithm_version="003.1",
            scoring_config='{"alert_threshold":"0.7"}',
        )

        result = assessment.to_dict()

        expected = {
            "assessment_id": "assessment_001",
            "wallet_address": "0xabc123def4567890",
            "market_id": "market_abc123",
            "trade_id": "tx_001",
            "trade_size": "6500.00",
            "trade_side": "BUY",
            "trade_price": "0.65",
            "signals_triggered": 2,
            "weighted_score": 0.72,
            "should_alert": True,
            "has_fresh_wallet_signal": True,
            "has_size_anomaly_signal": True,
            "fresh_wallet_confidence": 0.75,
            "size_anomaly_confidence": 0.8,
            "delivery_disposition": "delivered",
            "delivery_channels": "discord",
            "dry_run": False,
            "volume_available": True,
            "market_daily_volume": "12345.50",
            "book_depth_available": False,
            "wallet_tx_count": 7,
            "wallet_age_known": True,
            "wallet_group_status": "unqualified_pre_T027",
            "scoring_algorithm_version": "003.1",
            "scoring_config": '{"alert_threshold":"0.7"}',
            "timestamp": "2026-01-15T12:30:45+00:00",
        }
        assert result == expected
        assert list(result) == list(expected)

    def test_risk_assessment_to_dict_exact_payload_with_defaults(self) -> None:
        """The serialized assessment with no signals and every default field."""
        assessment = RiskAssessment(
            trade_event=build_trade_event(),
            wallet_address="0xabc123def4567890",
            market_id="market_abc123",
            assessment_id="assessment_002",
            timestamp=FIXED_TIMESTAMP,
        )

        result = assessment.to_dict()

        expected = {
            "assessment_id": "assessment_002",
            "wallet_address": "0xabc123def4567890",
            "market_id": "market_abc123",
            "trade_id": "tx_001",
            "trade_size": "6500.00",
            "trade_side": "BUY",
            "trade_price": "0.65",
            "signals_triggered": 0,
            "weighted_score": 0.0,
            "should_alert": False,
            "has_fresh_wallet_signal": False,
            "has_size_anomaly_signal": False,
            "fresh_wallet_confidence": None,
            "size_anomaly_confidence": None,
            "delivery_disposition": "unrecorded",
            "delivery_channels": None,
            "dry_run": False,
            "volume_available": None,
            "market_daily_volume": None,
            "book_depth_available": None,
            "wallet_tx_count": None,
            "wallet_age_known": None,
            "wallet_group_status": "unqualified_pre_T027",
            "scoring_algorithm_version": SCORING_ALGORITHM_VERSION,
            "scoring_config": None,
            "timestamp": "2026-01-15T12:30:45+00:00",
        }
        assert result == expected
        assert list(result) == list(expected)


class TestSignalProperties:
    """Pin the shared confidence thresholds and trade-identity accessors."""

    @pytest.mark.parametrize("build_signal", [build_fresh_wallet_signal, build_size_anomaly_signal])
    def test_trade_identity_comes_from_trade_event(
        self, build_signal: TradeAnchoredBuilder
    ) -> None:
        """wallet_address, market_id and trade_size_usdc delegate to the trade event."""
        trade = build_trade_event()
        signal = build_signal(trade)

        assert signal.wallet_address == trade.wallet_address
        assert signal.market_id == trade.market_id
        assert signal.trade_size_usdc == trade.notional_value
        assert signal.trade_size_usdc == Decimal("6500.00")

    @pytest.mark.parametrize(
        "build_signal",
        [build_fresh_wallet_signal, build_size_anomaly_signal, build_sniper_cluster_signal],
    )
    @pytest.mark.parametrize(
        ("confidence", "expected_high", "expected_very_high"),
        [(0.7, True, False), (0.69, False, False), (0.85, True, True), (0.84, True, False)],
    )
    def test_confidence_thresholds(
        self,
        build_signal: ConfidenceBuilder,
        confidence: float,
        expected_high: bool,
        expected_very_high: bool,
    ) -> None:
        """The 0.7 and 0.85 boundaries are inclusive for every signal model."""
        signal = build_signal(confidence=confidence)

        assert signal.is_high_confidence is expected_high
        assert signal.is_very_high_confidence is expected_very_high


class TestImportIsolation:
    """Pin the dependency direction: detector models must not import storage.

    Any full import of ``polymarket_insider_tracker.detector.models`` still
    loads storage transitively through the pre-existing
    ``ingestor -> config -> storage.database_url -> storage/__init__`` chain,
    which predates the cluster-7 card and is outside its scope. What this card
    must never regress is that no detector module imports the storage layer and
    the disposition default stays the one dependency-free constant.
    """

    def test_detector_models_do_not_import_the_storage_package(self) -> None:
        """A fresh interpreter traces no storage import originating in detector."""
        program = "\n".join(
            [
                "import builtins",
                "STORAGE = 'polymarket_insider_tracker.storage'",
                "storage_imports = []",
                "real_import = builtins.__import__",
                "def traced_import(*args, **kwargs):",
                "    name = args[0]",
                "    caller = args[1].get('__name__') if len(args) > 1 else None",
                "    if name == STORAGE or name.startswith(STORAGE + '.'):",
                "        storage_imports.append((name, caller or '<unknown>'))",
                "    return real_import(*args, **kwargs)",
                "builtins.__import__ = traced_import",
                "import polymarket_insider_tracker.detector.models as models",
                "import polymarket_insider_tracker.constants as constants",
                "import polymarket_insider_tracker.storage.repos as repos",
                "assert models.UNRECORDED_DELIVERY_DISPOSITION == 'unrecorded'",
                "assert (",
                "    models.UNRECORDED_DELIVERY_DISPOSITION",
                "    is constants.UNRECORDED_DELIVERY_DISPOSITION",
                "    is repos.UNRECORDED_DELIVERY_DISPOSITION",
                ")",
                "importers = {caller for _, caller in storage_imports}",
                "det = [i for i in importers if i.startswith('polymarket_insider_tracker.det')]",
                "assert not det, sorted(storage_imports)",
            ]
        )

        result = subprocess.run(
            [sys.executable, "-c", program], capture_output=True, text=True, timeout=120
        )

        assert result.returncode == 0, result.stderr
