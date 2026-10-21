"""Tests for composite risk scorer."""

import json
import math
from collections.abc import MutableMapping
from dataclasses import replace
from decimal import Decimal

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.detector.models import (
    SCORING_ALGORITHM_VERSION,
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
)
from polymarket_insider_tracker.detector.scorer import (
    DEFAULT_ALERT_THRESHOLD,
    DEFAULT_WEIGHTS,
    RiskScorer,
    SignalBundle,
    quantize_score_value,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile
from tests.batch_trades import indexed_trades
from tests.fakes.redis_await import key_count

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_metadata() -> MarketMetadata:
    """Create sample market metadata."""
    return MarketMetadata(
        condition_id="market_abc123",
        question="Will it rain tomorrow?",
        description="Weather prediction market",
        tokens=(Token(token_id="token_123", outcome="Yes", price=Decimal("0.65")),),
        category="science",
    )


# ============================================================================
# SignalBundle Tests
# ============================================================================


class TestSignalBundle:
    """Tests for the SignalBundle dataclass."""

    def test_bundle_with_no_signals(self, sample_trade: TradeEvent) -> None:
        """Test bundle with only trade, no signals."""
        bundle = SignalBundle(trade_event=sample_trade)

        assert bundle.trade_event == sample_trade
        assert bundle.fresh_wallet_signal is None
        assert bundle.size_anomaly_signal is None
        assert bundle.wallet_address == sample_trade.wallet_address
        assert bundle.market_id == sample_trade.market_id

    def test_bundle_with_fresh_wallet_signal(
        self,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
    ) -> None:
        """Test bundle with fresh wallet signal."""
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
        )

        assert bundle.fresh_wallet_signal == fresh_wallet_signal
        assert bundle.size_anomaly_signal is None

    def test_bundle_with_all_signals(
        self,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """Test bundle with all signal types."""
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=size_anomaly_signal,
        )

        assert bundle.fresh_wallet_signal == fresh_wallet_signal
        assert bundle.size_anomaly_signal == size_anomaly_signal


# ============================================================================
# RiskAssessment Tests
# ============================================================================


class TestRiskAssessment:
    """Tests for the RiskAssessment dataclass."""

    def test_assessment_creation(self, sample_trade: TradeEvent) -> None:
        """Test basic assessment creation."""
        assessment = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=0,
            weighted_score=0.0,
            should_alert=False,
        )

        assert assessment.trade_event == sample_trade
        assert assessment.signals_triggered == 0
        assert assessment.weighted_score == 0.0
        assert assessment.should_alert is False
        assert assessment.assessment_id is not None
        assert assessment.timestamp is not None

    def test_is_high_risk(self, sample_trade: TradeEvent) -> None:
        """Test is_high_risk property."""
        high_risk = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=1,
            weighted_score=0.70,
            should_alert=True,
        )
        low_risk = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=1,
            weighted_score=0.69,
            should_alert=True,
        )

        assert high_risk.is_high_risk is True
        assert low_risk.is_high_risk is False

    def test_is_very_high_risk(self, sample_trade: TradeEvent) -> None:
        """Test is_very_high_risk property."""
        very_high = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=2,
            weighted_score=0.85,
            should_alert=True,
        )
        high = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=None,
            size_anomaly_signal=None,
            signals_triggered=2,
            weighted_score=0.84,
            should_alert=True,
        )

        assert very_high.is_very_high_risk is True
        assert high.is_very_high_risk is False

    def test_to_dict(
        self,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
    ) -> None:
        """Test to_dict serialization."""
        assessment = RiskAssessment(
            trade_event=sample_trade,
            wallet_address=sample_trade.wallet_address,
            market_id=sample_trade.market_id,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=None,
            signals_triggered=1,
            weighted_score=0.65,
            should_alert=True,
        )

        result = assessment.to_dict()

        assert result["wallet_address"] == sample_trade.wallet_address
        assert result["market_id"] == sample_trade.market_id
        assert result["signals_triggered"] == 1
        assert result["weighted_score"] == 0.65
        assert result["should_alert"] is True
        assert result["has_fresh_wallet_signal"] is True
        assert result["has_size_anomaly_signal"] is False
        assert result["fresh_wallet_confidence"] == 0.8
        assert result["size_anomaly_confidence"] is None


# ============================================================================
# RiskScorer Initialization Tests
# ============================================================================


class TestRiskScorerInit:
    """Tests for RiskScorer initialization."""

    def test_default_initialization(self, fake_redis: FakeAsyncRedis) -> None:
        """Test scorer initializes with default values."""
        scorer = RiskScorer(fake_redis)

        assert scorer.alert_threshold == DEFAULT_ALERT_THRESHOLD
        assert scorer.dedup_window_seconds == 3600

    def test_custom_configuration(self, fake_redis: FakeAsyncRedis) -> None:
        """Test scorer with custom threshold and window configuration."""
        scorer = RiskScorer(
            fake_redis,
            alert_threshold=0.7,
            dedup_window_seconds=1800,
        )

        assert scorer.alert_threshold == 0.7
        assert scorer.dedup_window_seconds == 1800


# ============================================================================
# Weighted Score Calculation Tests
# ============================================================================


class TestWeightedScoreCalculation:
    """Tests for weighted score calculation."""

    def test_no_signals_zero_score(
        self, fake_redis: FakeAsyncRedis, sample_trade: TradeEvent
    ) -> None:
        """Test score is zero when no signals present."""
        scorer = RiskScorer(fake_redis)
        bundle = SignalBundle(trade_event=sample_trade)

        score, count = scorer.calculate_weighted_score(bundle)

        assert score == 0.0
        assert count == 0

    def test_fresh_wallet_only(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
    ) -> None:
        """G1: a wallet signal alone is recorded evidence and scores nothing (was 0.32)."""
        scorer = RiskScorer(fake_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
        )

        score, count = scorer.calculate_weighted_score(bundle)

        assert score == 0.0
        assert count == 0

    def test_size_anomaly_only(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """Test score with only size anomaly signal."""
        scorer = RiskScorer(fake_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            size_anomaly_signal=size_anomaly_signal,
        )

        score, count = scorer.calculate_weighted_score(bundle)

        # 0.7 confidence * 0.35 weight + 0.7 * 0.25 niche weight = 0.42
        expected = 0.7 * DEFAULT_WEIGHTS["size_anomaly"] + 0.7 * DEFAULT_WEIGHTS["niche_market"]
        assert math.isclose(score, expected, rel_tol=1e-6, abs_tol=1e-12)
        assert count == 1

    def test_size_anomaly_non_niche(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        sample_metadata: MarketMetadata,
    ) -> None:
        """Test size anomaly without niche bonus."""
        signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.10,
            book_impact=0.15,
            is_niche_market=False,
            confidence=0.7,
            factors={},
        )
        scorer = RiskScorer(fake_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            size_anomaly_signal=signal,
        )

        score, _ = scorer.calculate_weighted_score(bundle)

        # 0.7 * 0.35 = 0.245 (no niche bonus)
        expected = 0.7 * DEFAULT_WEIGHTS["size_anomaly"]
        assert math.isclose(score, expected, rel_tol=1e-6, abs_tol=1e-12)

    def test_wallet_signal_adds_no_bonus_while_the_group_is_non_scoring(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """G1: a wallet signal beside a size signal changes nothing (was 0.888, bonus 1.2)."""
        scorer = RiskScorer(fake_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=size_anomaly_signal,
        )

        score, count = scorer.calculate_weighted_score(bundle)

        expected = 0.7 * DEFAULT_WEIGHTS["size_anomaly"] + 0.7 * DEFAULT_WEIGHTS["niche_market"]
        assert math.isclose(score, expected, rel_tol=1e-6, abs_tol=1e-12)
        assert count == 1

    def test_full_confidence_size_score_is_the_size_ceiling_without_wallet_inflation(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        sample_wallet_profile: WalletProfile,
        sample_metadata: MarketMetadata,
    ) -> None:
        """G1: full confidence on both signals scores 0.6, not the capped 1.0 it did before."""
        # Create high confidence signals
        fresh_signal = FreshWalletSignal(
            trade_event=sample_trade,
            wallet_profile=sample_wallet_profile,
            confidence=1.0,
            factors={},
        )
        size_signal = SizeAnomalySignal(
            trade_event=sample_trade,
            market_metadata=sample_metadata,
            volume_impact=0.10,
            book_impact=0.15,
            is_niche_market=True,
            confidence=1.0,
            factors={},
        )

        scorer = RiskScorer(fake_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_signal,
            size_anomaly_signal=size_signal,
        )

        score, count = scorer.calculate_weighted_score(bundle)

        assert score == 0.6  # 1.0 * (0.35 + 0.25), no wallet term, no bonus
        assert count == 1


# ============================================================================
# Assess Method Tests
# ============================================================================


class TestAssessMethod:
    """Tests for the assess method."""

    @pytest.mark.asyncio
    async def test_assess_triggers_alert(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """An operator override at or below the size ceiling still alerts (D2 R0)."""
        scorer = RiskScorer(fake_redis, alert_threshold=0.4)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=size_anomaly_signal,
        )

        assessment = await scorer.assess(bundle)

        assert assessment.should_alert is True
        assert assessment.signals_triggered == 1
        assert assessment.weighted_score == 0.42
        assert RiskScorer(fake_redis).scoring_config != scorer.scoring_config

    @pytest.mark.asyncio
    async def test_assess_no_alert_below_threshold(
        self, fake_redis: FakeAsyncRedis, sample_trade: TradeEvent
    ) -> None:
        """Test assess does not alert for low-risk trades."""
        scorer = RiskScorer(fake_redis)
        bundle = SignalBundle(trade_event=sample_trade)

        assessment = await scorer.assess(bundle)

        assert assessment.should_alert is False
        assert assessment.signals_triggered == 0
        assert assessment.weighted_score == 0.0

    @pytest.mark.asyncio
    async def test_assess_decides_at_persisted_precision_at_alert_boundary(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """The decision uses the NUMERIC(4,3) precision that is persisted, so the stored
        score, threshold, and should_alert can never contradict each other (FR-012).

        This replaces the earlier float-artifact pin (score 0.7999999999999999, no alert):
        that behavior stored 0.800 >= 0.800 with should_alert False, which the durable
        record could not explain or replay.
        """
        bundle = SignalBundle(
            trade_event=sample_trade,
            size_anomaly_signal=replace(
                size_anomaly_signal,
                confidence=0.6666666666666666,
                is_niche_market=True,
            ),
        )

        assessment = await RiskScorer(fake_redis, alert_threshold=0.4).assess(bundle)

        assert assessment.weighted_score == 0.4
        assert assessment.should_alert is True
        stored_score = Decimal(str(assessment.weighted_score))
        stored_threshold = Decimal(str(round(0.4, 3)))
        assert assessment.should_alert is (stored_score >= stored_threshold)
        assert await key_count(fake_redis) == 0

    @pytest.mark.asyncio
    async def test_assessment_replays_exactly_from_persisted_precision_inputs(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """Recomputing from quantized confidences and the pinned default weights must
        reproduce the stored score exactly (constitution IV: explain or replay)."""
        size_conf = 0.6512345678901
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=replace(
                size_anomaly_signal, confidence=size_conf, is_niche_market=True
            ),
        )

        assessment = await RiskScorer(fake_redis).assess(bundle)

        quantum = Decimal("0.001")
        stored_size = quantize_score_value(size_conf)
        replayed = stored_size * Decimal(
            str(DEFAULT_WEIGHTS["size_anomaly"])
        ) + stored_size * Decimal(str(DEFAULT_WEIGHTS["niche_market"]))
        replayed = min(replayed, Decimal(1)).quantize(quantum)

        assert Decimal(str(assessment.weighted_score)) == replayed

    @pytest.mark.asyncio
    async def test_assess_does_not_deduplicate_or_touch_redis(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """Test that RiskScorer evaluates risk only, does not deduplicate, and writes zero Redis keys."""
        scorer = RiskScorer(fake_redis, alert_threshold=0.4)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=size_anomaly_signal,
        )

        assessment1 = await scorer.assess(bundle)
        assessment2 = await scorer.assess(bundle)

        assert assessment1.should_alert is True
        assert assessment2.should_alert is True
        assert await key_count(fake_redis) == 0

    @pytest.mark.asyncio
    async def test_assess_preserves_signals(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
    ) -> None:
        """Test assess preserves original signals in assessment."""
        scorer = RiskScorer(fake_redis)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
        )

        assessment = await scorer.assess(bundle)

        assert assessment.fresh_wallet_signal == fresh_wallet_signal
        assert assessment.size_anomaly_signal is None


# ============================================================================
# Batch Analysis Tests
# ============================================================================


class TestBatchAnalysis:
    """Tests for batch assessment."""

    @pytest.mark.asyncio
    async def test_assess_batch(
        self,
        fake_redis: FakeAsyncRedis,
        sample_wallet_profile: WalletProfile,
    ) -> None:
        """Test batch assessment returns assessments for all bundles."""
        scorer = RiskScorer(fake_redis)

        bundles: list[SignalBundle] = []
        for trade in indexed_trades(3):
            signal = FreshWalletSignal(
                trade_event=trade,
                wallet_profile=sample_wallet_profile,
                confidence=0.8,
                factors={},
            )
            bundles.append(SignalBundle(trade_event=trade, fresh_wallet_signal=signal))

        assessments = await scorer.assess_batch(bundles)

        assert len(assessments) == 3
        assert all(isinstance(a, RiskAssessment) for a in assessments)

    @pytest.mark.asyncio
    async def test_assess_batch_empty(self, fake_redis: FakeAsyncRedis) -> None:
        """Test batch assessment with empty list."""
        scorer = RiskScorer(fake_redis)

        assessments = await scorer.assess_batch([])

        assert assessments == []


# ============================================================================
# Weight Management Tests
# ============================================================================


class TestScoringIdentityAndCompatibility:
    """Every assessment carries its algorithm version and exact configuration, the
    default weights are immutable, and the pre-slice-003 weight-configuration API
    stays functional for one deprecation window (Patrick's 2026-09-11 decision)."""

    def test_default_weights_are_immutable(self) -> None:
        assert not isinstance(DEFAULT_WEIGHTS, MutableMapping)
        assert not hasattr(DEFAULT_WEIGHTS, "__setitem__")

    def test_get_weights_returns_defensive_copy(self, fake_redis: FakeAsyncRedis) -> None:
        scorer = RiskScorer(fake_redis)

        weights = scorer.get_weights()

        assert weights == dict(DEFAULT_WEIGHTS)
        # Mutating the returned copy must not change the scorer's behavior.
        weights["fresh_wallet"] = 999
        assert scorer.get_weights() == dict(DEFAULT_WEIGHTS)

    def test_algorithm_version_is_pinned(self) -> None:
        assert SCORING_ALGORITHM_VERSION == "004.1"

    def test_weights_constructor_argument_works_with_deprecation_warning(
        self, fake_redis: FakeAsyncRedis
    ) -> None:
        custom = {"fresh_wallet": 0.5, "size_anomaly": 0.3, "niche_market": 0.2}

        with pytest.warns(DeprecationWarning, match="weights constructor argument"):
            scorer = RiskScorer(fake_redis, weights=custom)

        assert scorer.get_weights() == custom
        # The caller's dictionary is copied, not aliased.
        custom["fresh_wallet"] = 0.99
        assert scorer.get_weights()["fresh_wallet"] == 0.5

    def test_set_weights_works_with_deprecation_warning(self, fake_redis: FakeAsyncRedis) -> None:
        scorer = RiskScorer(fake_redis)

        with pytest.warns(DeprecationWarning, match="set_weights"):
            scorer.set_weights({"fresh_wallet": 0.6, "size_anomaly": 0.25, "niche_market": 0.15})

        assert scorer.get_weights()["fresh_wallet"] == 0.6
        assert dict(DEFAULT_WEIGHTS)["fresh_wallet"] == 0.40

    @pytest.mark.asyncio
    async def test_assessment_records_version_and_canonical_config(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
    ) -> None:
        scorer = RiskScorer(fake_redis, alert_threshold=0.8)
        bundle = SignalBundle(trade_event=sample_trade, fresh_wallet_signal=fresh_wallet_signal)

        assessment = await scorer.assess(bundle)

        assert assessment.scoring_algorithm_version == SCORING_ALGORITHM_VERSION
        assert assessment.scoring_config is not None
        config = json.loads(assessment.scoring_config)
        assert config["weights"] == {
            "fresh_wallet": "0.4",
            "size_anomaly": "0.35",
            "niche_market": "0.25",
        }
        assert config["alert_threshold"] == "0.800"
        assert config["multi_signal_bonus_2"] == "1.2"
        assert config["multi_signal_bonus_3"] == "1"  # neutral since the 3+ branch was removed
        assert config["score_quantum"] == "0.001"
        # Canonical form: sorted keys, fixed separators, deterministic across calls.
        assert assessment.scoring_config == json.dumps(
            config, sort_keys=True, separators=(",", ":")
        )
        assert (
            assessment.scoring_config == RiskScorer(fake_redis, alert_threshold=0.8).scoring_config
        )

    @pytest.mark.asyncio
    async def test_custom_weight_assessment_replays_from_its_own_config(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """A row produced under deprecated custom weights replays exactly from the
        weights recorded in its own scoring_config — nothing else is needed."""
        custom = {"fresh_wallet": 0.7, "size_anomaly": 0.2, "niche_market": 0.1}
        with pytest.warns(DeprecationWarning):
            scorer = RiskScorer(fake_redis, weights=custom)
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=fresh_wallet_signal,
            size_anomaly_signal=size_anomaly_signal,
        )

        assessment = await scorer.assess(bundle)

        assert assessment.scoring_config is not None
        stored = json.loads(assessment.scoring_config)
        stored_weights = {name: Decimal(value) for name, value in stored["weights"].items()}
        # G1: the stored fresh_wallet weight is recorded but the wallet group adds nothing.
        replayed = (
            quantize_score_value(size_anomaly_signal.confidence) * stored_weights["size_anomaly"]
            + quantize_score_value(size_anomaly_signal.confidence) * stored_weights["niche_market"]
        )
        replayed_score = min(replayed, Decimal(1)).quantize(Decimal(stored["score_quantum"]))

        assert Decimal(str(assessment.weighted_score)) == replayed_score
        assert assessment.should_alert == (replayed_score >= Decimal(stored["alert_threshold"]))

    @pytest.mark.asyncio
    async def test_persisted_record_replays_with_pinned_algorithm_only(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        fresh_wallet_signal: FreshWalletSignal,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        """Recompute a decision from persisted-precision values and pinned weights only.

        This is the durable-record replay contract: everything needed besides the row
        itself is a constant of SCORING_ALGORITHM_VERSION.
        """
        fresh_conf = 0.7999999999999999
        bundle = SignalBundle(
            trade_event=sample_trade,
            fresh_wallet_signal=replace(fresh_wallet_signal, confidence=fresh_conf),
            size_anomaly_signal=size_anomaly_signal,
        )

        assessment = await RiskScorer(fake_redis, alert_threshold=0.8).assess(bundle)

        # The "persisted row": quantized confidences, score, threshold, and decision.
        stored_size = quantize_score_value(size_anomaly_signal.confidence)
        stored_score = quantize_score_value(assessment.weighted_score)
        stored_threshold = quantize_score_value(0.8)

        replayed = stored_size * Decimal(
            str(DEFAULT_WEIGHTS["size_anomaly"])
        ) + stored_size * Decimal(str(DEFAULT_WEIGHTS["niche_market"]))
        replayed_score = min(replayed, Decimal(1)).quantize(Decimal("0.001"))

        assert stored_score == replayed_score
        assert assessment.should_alert == (stored_score >= stored_threshold)


class TestDeprecatedWeightCompatibilityInputs:
    """Round-23 N-R23-4: previously working empty and partial weight mappings.

    The pre-slice-003 API treated an empty constructor mapping as "use the
    defaults" (``weights or DEFAULT_WEIGHTS``) and a signal name missing from
    a partial mapping as a zero contribution (``weights.get(name, 0.0)``).
    The retained compatibility API must preserve those existing inputs, not
    only complete maps, while every new assessment still records the
    effective per-signal weights in its own ``scoring_config``.
    """

    def _single_signal_bundle(
        self, sample_trade: TradeEvent, size_anomaly_signal: SizeAnomalySignal
    ) -> SignalBundle:
        return SignalBundle(trade_event=sample_trade, size_anomaly_signal=size_anomaly_signal)

    def test_empty_constructor_weights_fall_back_to_defaults(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        with pytest.warns(DeprecationWarning, match="weights constructor argument"):
            scorer = RiskScorer(fake_redis, weights={})

        score, count = scorer.calculate_weighted_score(
            self._single_signal_bundle(sample_trade, size_anomaly_signal)
        )

        assert count == 1
        assert math.isclose(score, 0.42, rel_tol=1e-6, abs_tol=1e-12)
        assert scorer.get_weights() == dict(DEFAULT_WEIGHTS)

    def test_partial_constructor_weights_score_missing_signals_as_zero(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        with pytest.warns(DeprecationWarning, match="weights constructor argument"):
            scorer = RiskScorer(fake_redis, weights={"size_anomaly": 1.0})

        score, count = scorer.calculate_weighted_score(
            self._single_signal_bundle(sample_trade, size_anomaly_signal)
        )

        assert count == 1
        assert math.isclose(score, 0.7, rel_tol=1e-6, abs_tol=1e-12)
        assert scorer.get_weights() == {"size_anomaly": 1.0}

    def test_partial_set_weights_scores_missing_signals_as_zero(
        self,
        fake_redis: FakeAsyncRedis,
        sample_trade: TradeEvent,
        size_anomaly_signal: SizeAnomalySignal,
    ) -> None:
        scorer = RiskScorer(fake_redis)
        with pytest.warns(DeprecationWarning, match="set_weights"):
            scorer.set_weights({"size_anomaly": 1.0})

        score, count = scorer.calculate_weighted_score(
            self._single_signal_bundle(sample_trade, size_anomaly_signal)
        )

        assert count == 1
        assert math.isclose(score, 0.7, rel_tol=1e-6, abs_tol=1e-12)

    def test_partial_weights_record_effective_zero_contributions(
        self,
        fake_redis: FakeAsyncRedis,
    ) -> None:
        """A stored row must replay from its own config without knowing the
        missing-key rule, so the implied zeros are recorded explicitly."""
        with pytest.warns(DeprecationWarning, match="weights constructor argument"):
            scorer = RiskScorer(fake_redis, weights={"size_anomaly": 1.0})

        config = json.loads(scorer.scoring_config)

        assert config["weights"] == {
            "fresh_wallet": "0.0",
            "niche_market": "0.0",
            "size_anomaly": "1.0",
        }
