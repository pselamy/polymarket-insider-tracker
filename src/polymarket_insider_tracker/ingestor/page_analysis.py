"""Strictly parsed page content: unique eligible observations plus per-row tallies.

Both acquisition paths fold their raw rows into one :class:`PageAnalysis` per cycle, so the
boundary, duplicate, deferral, and emission logic downstream is identical whether the rows came from
one v1 page or several v2 cursor pages. ``add_observations`` lets the v2 walk rebuild the same
analysis from rows it has already decoded, so the continuity proof is not re-implemented.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from polymarket_insider_tracker.ingestor.trade_rows import (
    InvalidRow,
    OutcomeResolution,
    TradeObservation,
    parse_trade_row,
)


@dataclass
class PageAnalysis:
    """Strictly parsed page content: unique eligible observations plus per-row tallies."""

    cutoff: int
    observations: dict[str, TradeObservation] = field(default_factory=dict[str, TradeObservation])
    outcomes: list[TradeObservation] = field(default_factory=list[TradeObservation])
    resolved_outcomes: int = 0
    invalid: list[InvalidRow] = field(default_factory=list[InvalidRow])
    deferred: int = 0
    repeats: int = 0
    row_count: int = 0
    latest_seen: int | None = None

    def add_rows(self, rows: Iterable[object], *, now: int) -> None:
        for raw in rows:
            self._add(parse_trade_row(raw, now=now))

    def add_observations(self, observations: Sequence[TradeObservation]) -> None:
        """Add already-decoded observations (the v2 walk's cumulative rows) in order."""
        for parsed in observations:
            self._add(parsed)

    def _add(self, parsed: TradeObservation | InvalidRow) -> None:
        self.row_count += 1
        if isinstance(parsed, InvalidRow):
            self.invalid.append(parsed)
            return
        self.outcomes.append(parsed)
        self.latest_seen = max(parsed.provider_timestamp, self.latest_seen or 0)
        if parsed.provider_timestamp > self.cutoff:
            self.deferred += 1
            return
        self._remember_observation(parsed)

    def _remember_observation(self, parsed: TradeObservation) -> None:
        existing = self.observations.get(parsed.identity)
        if existing is None:
            self.observations[parsed.identity] = parsed
            return
        self.repeats += 1
        if existing.outcome_resolution is OutcomeResolution.UNKNOWN:
            self.observations[parsed.identity] = parsed

    @property
    def oldest(self) -> int | None:
        timestamps = [observation.provider_timestamp for observation in self.observations.values()]
        return min(timestamps) if timestamps else None

    @property
    def newest(self) -> int | None:
        timestamps = [observation.provider_timestamp for observation in self.observations.values()]
        return max(timestamps) if timestamps else None

    @property
    def span_seconds(self) -> int:
        if self.newest is None or self.oldest is None:
            return 0
        return self.newest - self.oldest
