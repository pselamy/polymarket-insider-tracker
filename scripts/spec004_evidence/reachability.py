"""Recompute the slice-004 threshold-reachability table (research.md R10, T028).

Usage:
    python3 -m scripts.spec004_evidence.reachability                 # print the markdown report
    python3 -m scripts.spec004_evidence.reachability --check FILE    # exit 0 iff FILE equals the report byte for byte; 1 if stale or
                                                                     # missing; 2 on any other arguments

Every grid point is scored by the product code (``real_scoring``): the real size and fresh-wallet
detectors and the scorer's ``size_group_score``, the same function the runtime alert-path report
(``detector/alert_path.py``) computes its ceiling with; ``report`` fails if the two ceilings differ.
``verify_constants`` additionally re-reads the product source as text and lists the scoring literals,
so a weight, cap or bonus change shows up as a reviewed diff of the committed report.

Rules modelled: the 004 contract (unknown age = exactly 0.50, book depth non-operational, volume
usable only when ``available``, category fallback only when no volume value was observed, DECISION
D3 (d)) in its two wallet postures -- the default pre-T027 posture (FR-007c: wallet group
non-scoring, so only the size group can score and the two-group bonus never applies) and the
post-T027 scenario (wallet group qualified) -- next to the *003.1* rules (the pre-004 live pipeline
passed no volume, so the size signal could only come from the category fallback).
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from decimal import Decimal
from itertools import product
from pathlib import Path

from polymarket_insider_tracker.detector.alert_path import alert_path_report
from polymarket_insider_tracker.detector.fresh_wallet import LARGE_TRADE_THRESHOLD
from polymarket_insider_tracker.detector.scorer import (
    DEFAULT_ALERT_THRESHOLD,
    quantize_score_value,
)
from polymarket_insider_tracker.detector.size_anomaly import DEFAULT_NICHE_VOLUME_THRESHOLD
from scripts.spec004_evidence.cli import print_or_check
from scripts.spec004_evidence.real_scoring import (
    Market,
    Wallet,
    score,
    size_signal,
    wallet_confidence,
)

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "src" / "polymarket_insider_tracker"
LARGE_THRESHOLD = LARGE_TRADE_THRESHOLD
NICHE_VOLUME = DEFAULT_NICHE_VOLUME_THRESHOLD
THRESHOLD = quantize_score_value(DEFAULT_ALERT_THRESHOLD)

REQUIRED_LITERALS: dict[str, tuple[str, ...]] = {
    "detector/scorer.py": (
        '"fresh_wallet": 0.40',
        '"size_anomaly": 0.35',
        '"niche_market": 0.25',
        "MULTI_SIGNAL_BONUS_2 = 1.2",
        "DEFAULT_ALERT_THRESHOLD = 0.80",
    ),
    "detector/size_anomaly.py": (
        "NICHE_MULTIPLIER = 1.5",
        "VOLUME_SCORE_CAP = 0.5",
        "VOLUME_RATIO_CAP = 3.0",
        'factors["niche_multiplier"] = NICHE_MULTIPLIER',
        'factors["niche_base"] = 0.2',
        "ratio / VOLUME_RATIO_CAP * VOLUME_SCORE_CAP",
        "min(volume_impact / self._volume_threshold, VOLUME_RATIO_CAP)",
        "confidence < 0.1",
    ),
    "detector/fresh_wallet.py": (
        "BASE_CONFIDENCE = 0.5",
        "BRAND_NEW_BONUS = 0.2",
        "VERY_YOUNG_BONUS = 0.1",
        "LARGE_TRADE_BONUS = 0.1",
        'LARGE_TRADE_THRESHOLD = Decimal("10000")',
    ),
}


def check_literals(relative: str, text: str, literals: tuple[str, ...]) -> list[str]:
    for literal in literals:
        if literal not in text:
            raise SystemExit(f"constant drifted: {literal!r} not found in {relative}")
    return [f"- `{relative}`: `{literal}`" for literal in literals]


def verify_constants(source: Path = SOURCE) -> list[str]:
    """Return one line per verified literal; raise SystemExit if any is missing."""
    lines: list[str] = []
    for relative, literals in REQUIRED_LITERALS.items():
        lines += check_literals(relative, (source / relative).read_text(), literals)
    return lines


WALLETS = (
    Wallet("unknown age (the only age state main produces; nonce < B = 5)", 0, None),
    Wallet("known age, best case (nonce 0, < 2 h)", 0, 1.0),
    Wallet("known age, typical (nonce 1-4, 2-48 h)", 2, 24.0),
)
NOTIONALS = tuple(Decimal(n) for n in (1000, 3250, 10000, 10001, 50000, 1000000))
VOLUMES = tuple(Decimal(n) for n in (5000, 30000, 49999, 50000, 200000, 5000000))


def niche_market(volume: Decimal) -> Market | None:
    return Market("a", volume, False) if volume < NICHE_VOLUME else None


def broad_market(volume: Decimal) -> Market | None:
    return Market("b", volume, False) if volume >= NICHE_VOLUME else None


MARKETS: tuple[tuple[str, Callable[[Decimal], Market | None]], ...] = (
    ("volume available, niche (< $50k)", niche_market),
    ("volume available, not niche (>= $50k)", broad_market),
)
UNAVAILABLE = (
    ("volume unavailable, niche-prone category", Market("c", None, True)),
    ("volume unavailable, other category", Market("d", None, False)),
)
BANDS: tuple[tuple[str, Callable[[Decimal], bool]], ...] = (
    ("<= $10,000", lambda notional: notional <= LARGE_THRESHOLD),
    ("> $10,000", lambda notional: notional > LARGE_THRESHOLD),
)


def gated_confidence(wallet: Wallet | None, notional: Decimal, *, proposed: bool) -> float | None:
    """``wallet=None`` is the pre-T027 posture (FR-007c): the wallet group never scores."""
    if wallet is None:
        return None
    return wallet_confidence(wallet, notional, proposed=proposed)


def grid_available(
    wallet: Wallet | None,
    regime: int,
    notionals: list[Decimal],
    *,
    proposed: bool,
    volume_passed: bool,
) -> Iterator[tuple[Decimal, Decimal]]:
    selector = MARKETS[regime][1]
    for volume, notional in product(VOLUMES, notionals):
        market = selector(volume)
        if market is not None:
            yield score(
                gated_confidence(wallet, notional, proposed=proposed),
                size_signal(market, notional, volume_passed=volume_passed),
            ), notional


def grid_unavailable(
    wallet: Wallet | None, regime: int, notionals: list[Decimal], *, proposed: bool
) -> Iterator[tuple[Decimal, Decimal]]:
    market = UNAVAILABLE[regime - 2][1]
    for notional in notionals:
        yield score(
            gated_confidence(wallet, notional, proposed=proposed),
            size_signal(market, notional, volume_passed=False),
        ), notional


def grid(
    wallet: Wallet | None,
    regime: int,
    band: Callable[[Decimal], bool],
    *,
    proposed: bool,
    volume_passed: bool = True,
) -> Iterator[tuple[Decimal, Decimal]]:
    notionals = [n for n in NOTIONALS if band(n)]
    if regime < 2:
        return grid_available(
            wallet, regime, notionals, proposed=proposed, volume_passed=volume_passed
        )
    return grid_unavailable(wallet, regime, notionals, proposed=proposed)


REGIME_NAMES = (MARKETS[0][0], MARKETS[1][0], UNAVAILABLE[0][0], UNAVAILABLE[1][0])


GATED_NAME = "any (wallet group non-scoring before T027)"


def reachability_row(
    wallet: Wallet | None, regime: int, band_name: str, band: Callable[[Decimal], bool]
) -> str | None:
    best = max((s for s, _ in grid(wallet, regime, band, proposed=True)), default=None)
    if best is None:
        return None
    verdict = "yes" if best >= THRESHOLD else "**no**"
    name = GATED_NAME if wallet is None else wallet.name
    return f"| {name} | {REGIME_NAMES[regime]} | {band_name} | {best} | {verdict} |"


def gated_table() -> list[str]:
    """Default pre-T027 posture: only the size group scores; no two-group bonus."""
    rows = [
        "| Wallet evidence | Market evidence | Trade | Max score | Reaches 0.80? |",
        "|---|---|---|---|---|",
    ]
    for regime, (band_name, band) in product(range(4), BANDS):
        row = reachability_row(None, regime, band_name, band)
        if row is not None:
            rows.append(row)
    return rows


def reachability_table() -> list[str]:
    rows = [
        "| Wallet evidence | Market evidence | Trade | Max score | Reaches 0.80? |",
        "|---|---|---|---|---|",
    ]
    for wallet, regime, (band_name, band) in product(WALLETS, range(4), BANDS):
        row = reachability_row(wallet, regime, band_name, band)
        if row is not None:
            rows.append(row)
    return rows


def old_table() -> list[str]:
    rows = [
        "| Wallet evidence (003.1 rules) | Market evidence (volume never passed) | Trade | Max score |",
        "|---|---|---|---|",
    ]
    for wallet, regime, (band_name, band) in product(WALLETS[:1], (2, 3), BANDS):
        best = max(s for s, _ in grid(wallet, regime, band, proposed=False, volume_passed=False))
        rows.append(f"| unknown age, nonce 0 | {REGIME_NAMES[regime]} | {band_name} | {best} |")
    return rows


def sensitivity_row(threshold: str, scores: list[Decimal]) -> str:
    hit = [s for s in scores if s >= Decimal(threshold)]
    return f"| {threshold} | {len(hit)} | {max(hit) if hit else '-'} |"


def sensitivity_table(wallet: Wallet | None) -> list[str]:
    """Grid points (not live frequencies) at or above candidate thresholds for one wallet posture."""
    scores = [
        s
        for regime, (_, band) in product(range(4), BANDS)
        for s, _ in grid(wallet, regime, band, proposed=True)
    ]
    rows = [
        f"| Threshold | Grid points >= threshold (of {len(scores)}) | Highest score among them |",
        "|---|---|---|",
    ]
    for threshold in ("0.80", "0.78", "0.75", "0.70", "0.60", "0.45", "0.40"):
        rows.append(sensitivity_row(threshold, scores))
    return rows


def gated_ceiling() -> Decimal:
    """The grid maximum of the default posture; it must equal the runtime alert-path ceiling."""
    best = max(
        s
        for regime, (_, band) in product(range(4), BANDS)
        for s, _ in grid(None, regime, band, proposed=True)
    )
    runtime = alert_path_report(DEFAULT_ALERT_THRESHOLD).ceiling
    if best != runtime:
        raise SystemExit(f"grid ceiling {best} differs from the alert-path ceiling {runtime}")
    return best


def unknown_confidence_needed() -> str:
    """Smallest unknown-age confidence (0.001 steps) whose best niche case reaches 0.80."""
    size = size_signal(Market("a", Decimal(30000), False), Decimal(3250), volume_passed=True)
    for milli in range(500, 1001):
        if score(milli / 1000, size) >= THRESHOLD:
            return f"{milli / 1000:.3f}"
    return "none"


def grid_note() -> str:
    return (
        "Book depth is non-operational (FR-006). Grid: notionals "
        + ", ".join(f"${n:,}" for n in NOTIONALS)
        + "; usable volumes "
        + ", ".join(f"${v:,}" for v in VOLUMES)
        + ". Max over the grid; the volume ratio that saturates the 3x cap is >= 0.06."
    )


def proposed_sections() -> list[str]:
    return (
        [
            "## Proposed 004 rules, default pre-T027 posture (FR-007c): maximum attainable score",
            "",
            "The wallet group is non-scoring until the T027 qualification receipt exists: it "
            "contributes 0, is not counted as a signal group, and the 1.2 two-group bonus never "
            "applies. This is the posture the live product would ship with. Best case: "
            "0.75 x (0.35 + 0.25) = 0.450.",
            "",
            grid_note(),
            "",
        ]
        + gated_table()
        + [
            "",
            "## Proposed 004 rules, post-T027 scenario (wallet group qualified): maximum attainable score",
            "",
            "Valid only after T027 authorizes wallet scoring (DECISION D1b). Known-age rows also "
            "need an operational age source (DECISION D1a); none exists on main.",
            "",
            grid_note(),
            "",
        ]
        + reachability_table()
        + [""]
    )


def sensitivity_sections() -> list[str]:
    note = ["Grid points are illustrative combinations, **not** live frequencies.", ""]
    return (
        ["## Sensitivity: default pre-T027 posture, proposed rules", ""]
        + note
        + sensitivity_table(None)
        + ["", "## Sensitivity: unknown-age wallet, post-T027 scenario, proposed rules", ""]
        + note
        + sensitivity_table(WALLETS[0])
        + [""]
    )


def report() -> str:
    parts = ["# Threshold reachability (generated by reachability.py)", ""]
    parts += ["## Constants verified against product source", ""] + verify_constants() + [""]
    parts += proposed_sections()
    parts += (
        ["## 003.1 rules as wired on main today (live pipeline passes no volume, G-022)", ""]
        + old_table()
        + [""]
    )
    parts += sensitivity_sections()
    parts += [
        "## Recalibration arithmetic",
        "",
        f"- Default pre-T027 posture: the highest score of any trade is {gated_ceiling()}, so no "
        "alert is possible at any threshold above it; lowering the threshold to 0.78 (R1) "
        "still yields zero alerts.",
        f"- Post-T027 scenario: smallest unknown-age confidence that lets the best niche case reach 0.80: {unknown_confidence_needed()} "
        "(vs the proposed exact 0.500).",
        "- Post-T027 scenario: with confidence fixed at 0.500 the highest unknown-age score is 0.780 for "
        "any trade size, so no threshold-0.80 alert is possible while age is unknown.",
        "",
    ]
    return "\n".join(parts)


def main(argv: list[str]) -> int:
    return print_or_check(argv, report() + "\n")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
