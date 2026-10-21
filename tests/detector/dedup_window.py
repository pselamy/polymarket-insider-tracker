"""Read the scorer's retained deduplication window from inside its class hierarchy."""

from polymarket_insider_tracker.detector.scorer import RiskScorer


class DedupWindowScorer(RiskScorer):
    """``RiskScorer`` whose retained (otherwise unused) window is readable by tests."""

    @property
    def dedup_window(self) -> int:
        return self._dedup_window
