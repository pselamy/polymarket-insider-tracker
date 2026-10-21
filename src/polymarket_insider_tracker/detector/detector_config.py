"""The captured static detector configuration recorded in every 004.1 row.

Contract: ``specs/004-truthful-detection-contract/contracts/scoring-config.md`` section 2
(``detector_config``). The values are read from the detectors that actually run, never from a
reread setting.
"""

from decimal import Decimal

from polymarket_insider_tracker.detector import fresh_wallet, size_anomaly
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.scoring_config import DetectorConfig
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector


def capture_detector_config(
    fresh: FreshWalletDetector, size: SizeAnomalyDetector
) -> DetectorConfig:
    """The effective configuration of the two detectors an assessment is evaluated by."""
    return DetectorConfig(
        fresh_transaction_boundary=fresh.fresh_transaction_boundary,
        size_volume_threshold=Decimal(str(size.volume_threshold)),
        fresh_min_notional=fresh.min_trade_size,
        known_age_ceiling_hours=Decimal(str(fresh.max_age_hours)),
        niche_volume_threshold=size.niche_volume_threshold,
        niche_min_notional=size.niche_min_trade_size,
    )


def default_detector_config() -> DetectorConfig:
    """The configuration of default-constructed detectors (the pipeline builds them so)."""
    return DetectorConfig(
        fresh_transaction_boundary=fresh_wallet.DEFAULT_MAX_NONCE + 1,
        size_volume_threshold=Decimal(str(size_anomaly.DEFAULT_VOLUME_THRESHOLD)),
        fresh_min_notional=fresh_wallet.DEFAULT_MIN_TRADE_SIZE,
        known_age_ceiling_hours=Decimal(str(fresh_wallet.DEFAULT_MAX_AGE_HOURS)),
        niche_volume_threshold=size_anomaly.DEFAULT_NICHE_VOLUME_THRESHOLD,
        niche_min_notional=size_anomaly.DEFAULT_NICHE_MIN_TRADE_SIZE,
    )
