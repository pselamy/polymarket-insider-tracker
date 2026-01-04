"""Alert dispatcher for multi-channel delivery."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from polymarket_insider_tracker.alerter.models import FormattedAlert

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
    channel_results: dict[str, bool] = field(default_factory=dict)
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
        failure_threshold: int = 5,
        recovery_timeout_seconds: int = 60,
        half_open_max_attempts: int = 3,
    ) -> None:
        """Initialize the dispatcher.

        Args:
            channels: List of alert channels to dispatch to.
            failure_threshold: Number of consecutive failures before opening circuit.
            recovery_timeout_seconds: Time to wait before half-opening circuit.
            half_open_max_attempts: Number of test attempts in half-open state.
        """
        self.channels = channels
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
                    f"Circuit half-open for {channel_name}, "
                    f"attempt {state.half_open_attempts + 1}"
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
                f"Circuit opened for {channel_name} after "
                f"{state.failure_count} failures"
            )

    async def _send_to_channel(
        self, channel: AlertChannel, alert: FormattedAlert
    ) -> tuple[str, bool]:
        """Send alert to a single channel with circuit breaker."""
        channel_name = channel.name

        if not self._should_attempt(channel_name):
            logger.debug(f"Skipping {channel_name} - circuit open")
            return (channel_name, False)

        try:
            success = await channel.send(alert)
            if success:
                self._record_success(channel_name)
            else:
                self._record_failure(channel_name)
            return (channel_name, success)
        except Exception as e:
            logger.error(f"Error sending to {channel_name}: {e}")
            self._record_failure(channel_name)
            return (channel_name, False)

    async def dispatch(self, alert: FormattedAlert) -> DispatchResult:
        """Dispatch alert to all channels concurrently.

        Args:
            alert: Formatted alert to send.

        Returns:
            DispatchResult with per-channel status.
        """
        if not self.channels:
            logger.warning("No channels configured for dispatch")
            return DispatchResult(success_count=0, failure_count=0)

        # Send to all channels concurrently
        tasks = [self._send_to_channel(ch, alert) for ch in self.channels]
        results = await asyncio.gather(*tasks)

        # Aggregate results
        channel_results = dict(results)
        success_count = sum(1 for success in channel_results.values() if success)
        failure_count = len(channel_results) - success_count

        result = DispatchResult(
            success_count=success_count,
            failure_count=failure_count,
            channel_results=channel_results,
        )

        logger.info(
            f"Dispatch complete: {success_count}/{len(channel_results)} succeeded"
        )

        return result

    async def dispatch_batch(
        self, alerts: list[FormattedAlert]
    ) -> list[DispatchResult]:
        """Dispatch multiple alerts sequentially.

        Args:
            alerts: List of formatted alerts to send.

        Returns:
            List of DispatchResult for each alert.
        """
        results = []
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
                    state.last_failure_time.isoformat()
                    if state.last_failure_time
                    else None
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
