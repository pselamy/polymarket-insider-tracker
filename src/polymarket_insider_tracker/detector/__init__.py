"""Anomaly detection layer.

Distinguishes operational scoring components (SizeAnomalyDetector, RiskScorer),
pre-T027 non-scoring proxy-wallet observations (FreshWalletDetector), and
experimental non-operational clustering libraries (SniperDetector).
"""

from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.models import (
    FreshWalletSignal,
    RiskAssessment,
    SizeAnomalySignal,
    SniperClusterSignal,
)
from polymarket_insider_tracker.detector.scorer import RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.size_anomaly import SizeAnomalyDetector
from polymarket_insider_tracker.detector.sniper import SniperDetector

__all__ = [
    "FreshWalletDetector",
    "FreshWalletSignal",
    "RiskAssessment",
    "RiskScorer",
    "SignalBundle",
    "SizeAnomalyDetector",
    "SizeAnomalySignal",
    "SniperClusterSignal",
    "SniperDetector",
]
