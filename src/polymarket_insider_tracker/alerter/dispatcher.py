"""Alert dispatcher for multi-channel delivery."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from polymarket_insider_tracker.alerter.history import AlertHistory
    from polymarket_insider_tracker.alerter.models import FormattedAlert
    from polymarket_insider_tracker.detector.models import RiskAssessment

logger = logging.getLogger(__name__)


class AlertChannel(Protocol):
    """Protocol for alert delivery channels."""

    name: str

    async def send(self, alert: FormattedAlert) -> bool:
        """Send alert to channel. Returns True on success."""
        ...


@dataclass
class CircuitBreakerState:
    """State for circuit breaker pattern.

    Tracks failures and manages open/closed state for a channel.
    """

    failure_count: int = 0
    last_failure_time: datetime | None = None
    is_open: bool = False
    half_open_attempts: int = 0


@dataclass
class DispatchResult:
    """Result of dispatching an alert to all channels."""

    success_count: int
    failure_count: int
    channel_results: dict[str, bool] = field(default_factory=dict[str, bool])
    channel_statuses: dict[str, str] = field(default_factory=dict[str, str])
    disposition: str = "delivered"
    dry_run: bool = False
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def all_succeeded(self) -> bool:
        """Return True if all channels succeeded."""
        return self.failure_count == 0 and self.success_count > 0


class AlertDispatcher:
    """Dispatcher for sending alerts to multiple channels.

    Manages concurrent delivery to all configured channels with
    circuit breaker protection for failing channels.
    """

    def __init__(
        self,
        channels: list[AlertChannel],
        *,
        history: AlertHistory | None = None,
        dry_run: bool = False,
        failure_threshold: int = 5,
        recovery_timeout_seconds: int = 60,
        half_open_max_attempts: int = 3,
    ) -> None:
        """Initialize the dispatcher.

        Args:
            channels: List of alert channels to dispatch to.
            history: Optional alert history tracker for channel-scoped deduplication.
            dry_run: If True, do not send network requests or write deduplication state.
            failure_threshold: Number of consecutive failures before opening circuit.
            recovery_timeout_seconds: Time to wait before half-opening circuit.
            half_open_max_attempts: Number of test attempts in half-open state.
        """
        self.channels = channels
        self.history = history
        self.dry_run = dry_run
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.half_open_max_attempts = half_open_max_attempts

        # Circuit breaker state per channel
        self._circuit_state: dict[str, CircuitBreakerState] = {
            ch.name: CircuitBreakerState() for ch in channels
        }

    def _should_attempt(self, channel_name: str) -> bool:
        """Check if we should attempt delivery to this channel."""
        state = self._circuit_state[channel_name]

        if not state.is_open:
            return True

        # Check if we should try half-open
        if state.last_failure_time:
            elapsed = (datetime.now(UTC) - state.last_failure_time).total_seconds()
            if (
                elapsed >= self.recovery_timeout_seconds
                and state.half_open_attempts < self.half_open_max_attempts
            ):
                # Allow half-open attempt
                logger.info(
                    f"Circuit half-open for {channel_name}, attempt {state.half_open_attempts + 1}"
                )
                return True

        return False

    def _record_success(self, channel_name: str) -> None:
        """Record a successful delivery."""
        state = self._circuit_state[channel_name]
        state.failure_count = 0
        state.is_open = False
        state.half_open_attempts = 0
        state.last_failure_time = None
        logger.debug(f"Circuit closed for {channel_name}")

    def _record_failure(self, channel_name: str) -> None:
        """Record a failed delivery."""
        state = self._circuit_state[channel_name]
        state.failure_count += 1
        state.last_failure_time = datetime.now(UTC)

        if state.is_open:
            # Failed during half-open, increment attempts
            state.half_open_attempts += 1
        elif state.failure_count >= self.failure_threshold:
            # Open the circuit
            state.is_open = True
            logger.warning(
                f"Circuit opened for {channel_name} after {state.failure_count} failures"
            )

    def _create_dry_run_result(self) -> DispatchResult:
        statuses = {ch.name: "dry_run" for ch in self.channels}
        results = {ch.name: False for ch in self.channels}
        return DispatchResult(
            success_count=0,
            failure_count=0,
            channel_results=results,
            channel_statuses=statuses,
            disposition="dry_run",
            dry_run=True,
        )

    @staticmethod
    def _extract_wallet_market(
        wallet_address: str | None,
        market_id: str | None,
        assessment: RiskAssessment | None,
    ) -> tuple[str, str]:
        if assessment is not None:
            return assessment.wallet_address, assessment.market_id
        return wallet_address or "", market_id or ""

    async def _check_channel_suppression(
        self, channel_name: str, wallet: str, market: str
    ) -> str | None:
        if not (self.history and wallet and market):
            return None
        suppressed, reason = await self.history.is_channel_suppressed(channel_name, wallet, market)
        if not suppressed:
            return None
        return reason

    async def _record_channel_outcome(
        self, channel_name: str, status: str, wallet: str, market: str
    ) -> None:
        if not (self.history and wallet and market):
            return
        if status == "delivered":
            await self.history.record_channel_delivery(channel_name, wallet, market)
        elif status == "ambiguous":
            await self.history.record_channel_ambiguous(channel_name, wallet, market)

    async def _execute_channel_send(
        self, channel: AlertChannel, alert: FormattedAlert
    ) -> tuple[bool, str]:
        channel_name = channel.name
        if not self._should_attempt(channel_name):
            logger.debug("Skipping %s - circuit open", channel_name)
            return False, "circuit_open"
        try:
            success = await channel.send(alert)
            if success:
                self._record_success(channel_name)
                return True, "delivered"
            self._record_failure(channel_name)
            return False, "failed"
        except TimeoutError:
            logger.warning("Timeout delivering alert to channel %s", channel_name)
            self._record_failure(channel_name)
            return False, "ambiguous"
        except Exception as e:
            logger.error("Error sending to %s: %s", channel_name, e)
            self._record_failure(channel_name)
            return False, "failed"

    async def _send_to_channel_with_history(
        self, channel: AlertChannel, alert: FormattedAlert, wallet: str, market: str
    ) -> tuple[str, bool, str]:
        channel_name = channel.name
        suppression = await self._check_channel_suppression(channel_name, wallet, market)
        if suppression is not None:
            return channel_name, False, suppression

        success, status = await self._execute_channel_send(channel, alert)
        await self._record_channel_outcome(channel_name, status, wallet, market)
        return channel_name, success, status

    @staticmethod
    def _is_all_duplicate(statuses: list[str]) -> bool:
        return bool(statuses) and all(s == "duplicate" for s in statuses)

    @staticmethod
    def _is_all_ambiguous(statuses: list[str]) -> bool:
        return bool(statuses) and all(s in ("ambiguous", "ambiguous_timeout") for s in statuses)

    @staticmethod
    def _classify_mixed_statuses(statuses: list[str]) -> str:
        delivered = "delivered" in statuses
        failed = any(s in ("failed", "ambiguous", "circuit_open") for s in statuses)
        if delivered and failed:
            return "partial_failure"
        if delivered:
            return "delivered"
        return "failed"

    @classmethod
    def _compute_disposition(cls, statuses: list[str]) -> str:
        if cls._is_all_duplicate(statuses):
            return "duplicate"
        if cls._is_all_ambiguous(statuses):
            return "ambiguous"
        return cls._classify_mixed_statuses(statuses)

    async def _dispatch_to_channels(
        self, alert: FormattedAlert, wallet: str, market: str
    ) -> list[tuple[str, bool, str]]:
        tasks = [
            self._send_to_channel_with_history(ch, alert, wallet, market) for ch in self.channels
        ]
        return await asyncio.gather(*tasks)

    @staticmethod
    def _aggregate_results(
        results: list[tuple[str, bool, str]],
    ) -> tuple[dict[str, bool], dict[str, str], int, int]:
        channel_results: dict[str, bool] = {}
        channel_statuses: dict[str, str] = {}
        success_count = 0
        failure_count = 0
        for name, succ, stat in results:
            channel_results[name] = succ
            channel_statuses[name] = stat
            if succ:
                success_count += 1
            elif stat in ("failed", "ambiguous", "circuit_open"):
                failure_count += 1
        return channel_results, channel_statuses, success_count, failure_count

    async def dispatch(
        self,
        alert: FormattedAlert,
        wallet_address: str | None = None,
        market_id: str | None = None,
        *,
        assessment: RiskAssessment | None = None,
    ) -> DispatchResult:
        """Dispatch alert to all channels concurrently.

        Args:
            alert: Formatted alert to send.
            wallet_address: Optional trader wallet address for deduplication.
            market_id: Optional market ID for deduplication.
            assessment: Optional risk assessment for extracting wallet and market.

        Returns:
            DispatchResult with per-channel status.
        """
        if self.dry_run:
            return self._create_dry_run_result()

        if not self.channels:
            logger.warning("No channels configured for dispatch")
            return DispatchResult(success_count=0, failure_count=0)

        wallet, market = self._extract_wallet_market(wallet_address, market_id, assessment)
        results = await self._dispatch_to_channels(alert, wallet, market)
        res, stats, succ_count, fail_count = self._aggregate_results(results)
        disposition = self._compute_disposition(list(stats.values()))

        logger.info(
            "Dispatch complete: %s (%d/%d succeeded)",
            disposition,
            succ_count,
            len(self.channels),
        )

        return DispatchResult(
            success_count=succ_count,
            failure_count=fail_count,
            channel_results=res,
            channel_statuses=stats,
            disposition=disposition,
            dry_run=False,
        )

    async def dispatch_batch(self, alerts: list[FormattedAlert]) -> list[DispatchResult]:
        """Dispatch multiple alerts sequentially.

        Args:
            alerts: List of formatted alerts to send.

        Returns:
            List of DispatchResult for each alert.
        """
        results: list[DispatchResult] = []
        for alert in alerts:
            result = await self.dispatch(alert)
            results.append(result)
        return results

    def get_circuit_status(self) -> dict[str, dict[str, object]]:
        """Get current circuit breaker status for all channels."""
        return {
            name: {
                "is_open": state.is_open,
                "failure_count": state.failure_count,
                "half_open_attempts": state.half_open_attempts,
                "last_failure": (
                    state.last_failure_time.isoformat() if state.last_failure_time else None
                ),
            }
            for name, state in self._circuit_state.items()
        }

    def reset_circuit(self, channel_name: str) -> bool:
        """Manually reset circuit breaker for a channel.

        Args:
            channel_name: Name of channel to reset.

        Returns:
            True if channel was found and reset.
        """
        if channel_name in self._circuit_state:
            self._circuit_state[channel_name] = CircuitBreakerState()
            logger.info(f"Circuit reset for {channel_name}")
            return True
        return False
