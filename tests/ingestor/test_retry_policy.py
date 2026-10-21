"""Behavioural pins for the shared retry policy (mutation step 1)."""

from __future__ import annotations

from polymarket_insider_tracker.ingestor._retry import (
    RetryPolicy,
    retry_delay_seconds,
)


def test_delay_doubles_per_attempt_is_capped_and_adds_capped_retry_after() -> None:
    def full() -> float:
        return 1.0

    delays = [
        retry_delay_seconds(attempt, retry_after_seconds=0.0, random_source=full)
        for attempt in (0, 1, 2, 5, 6, 10)
    ]
    assert delays == [1.0, 2.0, 4.0, 30.0, 30.0, 30.0]
    assert retry_delay_seconds(0, retry_after_seconds=7.5, random_source=full) == 8.5
    assert retry_delay_seconds(0, retry_after_seconds=600.0, random_source=full) == 61.0


def test_policy_delay_uses_its_own_random_source() -> None:
    policy = RetryPolicy(random_source=lambda: 0.5)
    assert policy.delay_seconds(2, retry_after_seconds=3.0) == 5.0
