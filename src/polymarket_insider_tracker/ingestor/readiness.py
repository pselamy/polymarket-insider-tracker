"""Readiness proof for the ingestion component.

``running_ingestion_status`` is the cohesive readiness computation formerly
embedded in the pipeline orchestrator: given a running poller, require proof
of reachability (a recent successful acquisition) before reporting ``up``,
and surface recoverable poller states as ``degraded`` with their error.
"""

from __future__ import annotations

from polymarket_insider_tracker.ingestor.health import ComponentStatus, HealthMonitor
from polymarket_insider_tracker.ingestor.trade_poller import IngestionState, TradePoller

_DEGRADED_INGESTION_STATES = (IngestionState.DEGRADED, IngestionState.POSSIBLE_DATA_LOSS)


def running_ingestion_status(poller: TradePoller, monitor: HealthMonitor) -> ComponentStatus:
    """Readiness needs proof of reachability: a recent successful acquisition.

    A poller that is merely running (STARTING, or DEGRADED because every request
    failed) has not acquired anything; reporting it "up" would make a
    never-connected or unreachable source look ready. A progressing
    POSSIBLE_DATA_LOSS or transiently DEGRADED source keeps a fresh success and
    stays a visible, ready-compatible "degraded" component.
    """
    success_age = poller.seconds_since_last_success
    if success_age is None:
        return ComponentStatus(status="down", last_error=_no_acquisition_error(poller))
    if success_age > monitor.stale_threshold_seconds:
        return ComponentStatus(
            status="down", last_error=_stale_success_error(poller, success_age, monitor)
        )
    state = poller.state
    if state in _DEGRADED_INGESTION_STATES:
        error = poller.status.last_error or f"ingestion state: {state.value}"
        return ComponentStatus(status="degraded", last_error=error)
    return ComponentStatus(status="up")


def _no_acquisition_error(poller: TradePoller) -> str:
    last_error = poller.status.last_error
    message = "trade source has not completed a successful acquisition"
    return f"{message}: {last_error}" if last_error else message


def _stale_success_error(poller: TradePoller, success_age: float, monitor: HealthMonitor) -> str:
    last_error = poller.status.last_error
    threshold = monitor.stale_threshold_seconds
    message = (
        f"last successful acquisition was {success_age:.0f}s ago (staleness bound {threshold:.0f}s)"
    )
    return f"{message}: {last_error}" if last_error else message
