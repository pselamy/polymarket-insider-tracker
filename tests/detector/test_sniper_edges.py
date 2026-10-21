"""Edge behaviour of sniper clustering: noise, undersized clusters and cluster identity."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from polymarket_insider_tracker.detector.sniper import SniperDetector
from tests.detector.test_sniper import create_test_trade

WHALE = Decimal("1000000000")
WHALE_TWO = Decimal("1000000000000")


def _enter(
    detector: SniperDetector,
    wallet: str,
    market: str,
    size: Decimal = Decimal("1000"),
    delay_seconds: int = 30,
) -> None:
    created = datetime.now(UTC)
    trade = create_test_trade(
        wallet, market, created + timedelta(seconds=delay_seconds), notional_value=size
    )
    detector.record_entry(trade, created)


def _detector(**overrides: int) -> SniperDetector:
    settings = {"min_cluster_size": 3, "min_entries_per_wallet": 2, "min_samples": 2}
    return SniperDetector(eps=1.0, **{**settings, **overrides})


def _pack(detector: SniperDetector, wallets: list[str]) -> None:
    for wallet in wallets:
        for market in ("m_one", "m_two"):
            _enter(detector, wallet, market)


def _signalled(detector: SniperDetector) -> set[str]:
    return {signal.wallet_address for signal in detector.run_clustering()}


def test_a_detector_with_no_minimum_and_no_entries_finds_nothing() -> None:
    assert SniperDetector(min_cluster_size=0).run_clustering() == []


def test_an_outlier_wallet_is_noise_and_never_signalled() -> None:
    detector = _detector()
    _pack(detector, ["0xa", "0xb", "0xc"])
    _enter(detector, "0xloner", "m_one", WHALE)
    _enter(detector, "0xloner", "m_two", WHALE_TWO)

    assert _signalled(detector) == {"0xa", "0xb", "0xc"}
    assert not detector.is_sniper("0xloner")


def test_a_cluster_of_one_wallet_below_the_minimum_size_is_dropped() -> None:
    detector = _detector()
    _pack(detector, ["0xa", "0xb", "0xc"])
    _enter(detector, "0xtwin", "m_one", WHALE)
    _enter(detector, "0xtwin", "m_one", WHALE)

    assert _signalled(detector) == {"0xa", "0xb", "0xc"}
    assert detector.get_cluster_count() == 1


def test_a_lone_wallet_cluster_has_no_markets_in_common() -> None:
    detector = _detector(min_cluster_size=1)
    _enter(detector, "0xsolo", "m_one")
    _enter(detector, "0xsolo", "m_one")

    signals = detector.run_clustering()

    assert [(s.wallet_address, s.markets_in_common) for s in signals] == [("0xsolo", 0)]


def test_a_cluster_mostly_of_new_wallets_gets_a_new_identity() -> None:
    detector = _detector()
    _pack(detector, ["0xa", "0xb", "0xc"])
    detector.run_clustering()
    first = detector.get_cluster_for_wallet("0xa")
    assert first is not None
    detector.clear_entries()

    _pack(detector, ["0xa", "0xn1", "0xn2", "0xn3", "0xn4"])
    detector.run_clustering()
    second = detector.get_cluster_for_wallet("0xn1")

    assert second is not None
    assert second.cluster_id != first.cluster_id
    assert detector.get_cluster_count() == 2
