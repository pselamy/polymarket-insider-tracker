"""G1 (DECISION D1b): the wallet group is non-scoring until a T027 PASS receipt (FR-007c)."""

import pytest
from fakeredis import FakeAsyncRedis

from polymarket_insider_tracker.detector.models import FreshWalletSignal, SizeAnomalySignal
from polymarket_insider_tracker.detector.scorer import RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.scoring_config import (
    DEFAULT_WALLET_GROUP_STATUS,
    WalletGroupStatus,
)
from polymarket_insider_tracker.ingestor.models import MarketMetadata, Token, TradeEvent
from polymarket_insider_tracker.profiler.models import WalletProfile


@pytest.fixture
def scorer() -> RiskScorer:
    return RiskScorer(FakeAsyncRedis())


def _size_signal(trade: TradeEvent, confidence: float, *, niche: bool) -> SizeAnomalySignal:
    metadata = MarketMetadata(
        condition_id="market_abc123",
        question="Q?",
        description="",
        tokens=(Token(token_id="token_123", outcome="Yes", price=trade.price),),
        category="science",
    )
    return SizeAnomalySignal(
        trade_event=trade,
        market_metadata=metadata,
        volume_impact=0.0,
        book_impact=0.0,
        is_niche_market=niche,
        confidence=confidence,
        factors={},
    )


def _wallet_signal(
    trade: TradeEvent, profile: WalletProfile, confidence: float
) -> FreshWalletSignal:
    return FreshWalletSignal(
        trade_event=trade, wallet_profile=profile, confidence=confidence, factors={"base": 0.5}
    )


def test_the_shipped_posture_is_the_unqualified_wallet_group() -> None:
    assert DEFAULT_WALLET_GROUP_STATUS is WalletGroupStatus.UNQUALIFIED_PRE_T027


@pytest.mark.parametrize(
    ("wallet_confidence", "size_confidence", "niche", "score", "groups"),
    [
        (0.7, 0.75, True, 0.450, 1),  # was 0.7*0.4 + 0.75*0.35 + 0.75*0.25, times 1.2 = 0.8
        (0.7, 0.2, True, 0.120, 1),  # the volume-less journey fixture; was 0.480
        (0.7, 0.75, False, 0.262, 1),  # no niche term
        (1.0, None, False, 0.0, 0),  # a wallet signal alone no longer scores
    ],
)
async def test_only_the_size_group_scores(
    scorer: RiskScorer,
    sample_trade: TradeEvent,
    sample_wallet_profile: WalletProfile,
    wallet_confidence: float,
    size_confidence: float | None,
    niche: bool,
    score: float,
    groups: int,
) -> None:
    size = (
        None
        if size_confidence is None
        else _size_signal(sample_trade, size_confidence, niche=niche)
    )
    bundle = SignalBundle(
        trade_event=sample_trade,
        fresh_wallet_signal=_wallet_signal(sample_trade, sample_wallet_profile, wallet_confidence),
        size_anomaly_signal=size,
    )

    assessment = await scorer.assess(bundle)

    assert assessment.weighted_score == score
    assert assessment.signals_triggered == groups


async def test_the_wallet_signal_stays_recorded_as_non_scoring_evidence(
    scorer: RiskScorer, sample_trade: TradeEvent, sample_wallet_profile: WalletProfile
) -> None:
    wallet = _wallet_signal(sample_trade, sample_wallet_profile, 0.7)
    bundle = SignalBundle(
        trade_event=sample_trade,
        fresh_wallet_signal=wallet,
        size_anomaly_signal=_size_signal(sample_trade, 0.75, niche=True),
    )

    assessment = await scorer.assess(bundle)

    assert assessment.fresh_wallet_signal is wallet
    assert assessment.wallet_group_status is WalletGroupStatus.UNQUALIFIED_PRE_T027
    assert assessment.wallet_tx_count == sample_wallet_profile.nonce
    published = assessment.to_dict()
    assert published["wallet_group_status"] == "unqualified_pre_T027"
    assert published["fresh_wallet_confidence"] == 0.7


async def test_no_bonus_is_applied_to_a_size_only_score(
    scorer: RiskScorer, sample_trade: TradeEvent, sample_wallet_profile: WalletProfile
) -> None:
    bundle = SignalBundle(
        trade_event=sample_trade,
        fresh_wallet_signal=_wallet_signal(sample_trade, sample_wallet_profile, 1.0),
        size_anomaly_signal=_size_signal(sample_trade, 1.0, niche=True),
    )

    score, groups = scorer.calculate_weighted_score(bundle)

    assert (score, groups) == (0.6, 1)  # 1.0*0.35 + 1.0*0.25, bonus 1


@pytest.mark.parametrize(("count", "factor"), [(0, 1), (1, 1), (2, "1.2"), (3, "1.2")])
def test_the_third_group_bonus_is_removed(count: int, factor: object) -> None:
    from decimal import Decimal

    assert RiskScorer._apply_multi_signal_bonus(Decimal(1), count) == Decimal(str(factor))
