"""Recompute the slice-004 threshold-reachability table (research.md R10).

Usage:
    python3 reachability.py                 # print the markdown report
    python3 reachability.py --check FILE    # exit 1 unless FILE equals the report byte for byte

Stdlib only. It never imports from src/; the scoring constants below are copies, and
``verify_constants`` re-reads the product source as text (default: the repository containing this
file) and fails if a copied literal is no longer present, so the table cannot silently diverge
from detector/scorer.py, detector/size_anomaly.py and detector/fresh_wallet.py.

Rules modelled: the *proposed* 004 contract (unknown age = exactly 0.50, book depth
non-operational, volume usable only when ``available``, category fallback whenever volume is not
usable) next to the *003.1* rules as currently wired on main (the live pipeline passes no volume,
so the size signal can only come from the category fallback).
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from itertools import product
from pathlib import Path

QUANTUM = Decimal("0.001")
ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "src" / "polymarket_insider_tracker"

# --- constants copied from product source (verified textually by verify_constants) ------------
W_WALLET, W_SIZE, W_NICHE = Decimal("0.40"), Decimal("0.35"), Decimal("0.25")  # scorer.py
BONUS_TWO = Decimal("1.2")  # scorer.py MULTI_SIGNAL_BONUS_2
BASE, BRAND_NEW, VERY_YOUNG, LARGE = 0.5, 0.2, 0.1, 0.1  # fresh_wallet.py
LARGE_THRESHOLD = Decimal(10000)  # fresh_wallet.py
WALLET_MIN_NOTIONAL = Decimal(1000)  # fresh_wallet.py DEFAULT_MIN_TRADE_SIZE
VOLUME_THRESHOLD = 0.02  # size_anomaly.py DEFAULT_VOLUME_THRESHOLD
NICHE_VOLUME = Decimal(50000)  # size_anomaly.py DEFAULT_NICHE_VOLUME_THRESHOLD
NICHE_MIN_NOTIONAL = Decimal(500)  # size_anomaly.py DEFAULT_NICHE_MIN_TRADE_SIZE
NICHE_MULTIPLIER, NICHE_BASE = 1.5, 0.2  # size_anomaly.py
THRESHOLD = Decimal("0.80")  # scorer.py DEFAULT_ALERT_THRESHOLD
UNKNOWN_AGE_CONFIDENCE = 0.5  # proposed FR-008 (exactly)

REQUIRED_LITERALS: dict[str, tuple[str, ...]] = {
    "detector/scorer.py": (
        '"fresh_wallet": 0.40',
        '"size_anomaly": 0.35',
        '"niche_market": 0.25',
        "MULTI_SIGNAL_BONUS_2 = 1.2",
        "DEFAULT_ALERT_THRESHOLD = 0.80",
    ),
    "detector/size_anomaly.py": (
        'factors["niche_multiplier"] = 1.5',
        'factors["niche_base"] = 0.2',
        "ratio / 3.0 * 0.5",
        "min(volume_impact / self._volume_threshold, 3.0)",
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


def verify_constants(source: Path = SOURCE) -> list[str]:
    """Return one line per verified literal; raise SystemExit if any is missing."""
    lines: list[str] = []
    for relative, literals in REQUIRED_LITERALS.items():
        text = (source / relative).read_text()
        for literal in literals:
            if literal not in text:
                raise SystemExit(f"constant drifted: {literal!r} not found in {relative}")
            lines.append(f"- `{relative}`: `{literal}`")
    return lines


def quantize(value: float | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(QUANTUM, rounding=ROUND_HALF_EVEN)


@dataclass(frozen=True)
class Wallet:
    name: str
    nonce: int
    age_hours: float | None  # None = unknown


@dataclass(frozen=True)
class Market:
    name: str
    volume: Decimal | None  # usable positive 24h volume, else None
    niche_prone_category: bool  # category in science/tech/finance/other


def wallet_confidence(wallet: Wallet, notional: Decimal, *, proposed: bool) -> float | None:
    """None = no wallet signal. ``proposed`` applies the 004 unknown-age rule."""
    if notional < WALLET_MIN_NOTIONAL:
        return None
    if proposed and wallet.age_hours is None:
        return UNKNOWN_AGE_CONFIDENCE
    confidence = BASE
    if wallet.nonce == 0:
        confidence += BRAND_NEW
    if wallet.age_hours is not None and wallet.age_hours < 2.0:
        confidence += VERY_YOUNG
    if notional > LARGE_THRESHOLD:
        confidence += LARGE
    return max(0.0, min(1.0, confidence))


def size_signal(
    market: Market, notional: Decimal, *, volume_passed: bool
) -> tuple[float, bool] | None:
    """(confidence, is_niche) or None when no size signal is emitted."""
    volume = market.volume if volume_passed else None
    impact = float(notional / volume) if volume is not None else 0.0
    is_niche = volume < NICHE_VOLUME if volume is not None else market.niche_prone_category
    confidence = 0.0
    exceeds_volume = impact > VOLUME_THRESHOLD
    if is_niche and not exceeds_volume and notional < NICHE_MIN_NOTIONAL:
        return None
    if exceeds_volume:
        confidence = min(impact / VOLUME_THRESHOLD, 3.0) / 3.0 * 0.5
    if is_niche:
        confidence = confidence * NICHE_MULTIPLIER if confidence > 0 else NICHE_BASE
    confidence = max(0.0, min(1.0, confidence))
    return (confidence, is_niche) if confidence >= 0.1 else None


def score(wallet_conf: float | None, size: tuple[float, bool] | None) -> Decimal:
    total = Decimal(0)
    count = 0
    if wallet_conf is not None:
        total += quantize(wallet_conf) * W_WALLET
        count += 1
    if size is not None:
        total += quantize(size[0]) * W_SIZE
        if size[1]:
            total += quantize(size[0]) * W_NICHE
        count += 1
    if count >= 2:
        total *= BONUS_TWO
    return min(total, Decimal(1)).quantize(QUANTUM, rounding=ROUND_HALF_EVEN)


WALLETS = (
    Wallet("unknown age (live today; any nonce < 5)", 0, None),
    Wallet("known age, best case (nonce 0, < 2 h)", 0, 1.0),
    Wallet("known age, typical (nonce 1-4, 2-48 h)", 2, 24.0),
)
NOTIONALS = tuple(Decimal(n) for n in (1000, 3250, 10000, 10001, 50000, 1000000))
VOLUMES = tuple(Decimal(n) for n in (5000, 30000, 49999, 50000, 200000, 5000000))
MARKETS: tuple[tuple[str, Callable[[Decimal], Market | None]], ...] = (
    (
        "volume available, niche (< $50k)",
        lambda v: Market("a", v, False) if v < NICHE_VOLUME else None,
    ),
    (
        "volume available, not niche (>= $50k)",
        lambda v: Market("b", v, False) if v >= NICHE_VOLUME else None,
    ),
)
UNAVAILABLE = (
    ("volume unavailable, niche-prone category", Market("c", None, True)),
    ("volume unavailable, other category", Market("d", None, False)),
)
BANDS = (
    ("<= $10,000", lambda n: n <= LARGE_THRESHOLD),
    ("> $10,000", lambda n: n > LARGE_THRESHOLD),
)


def grid(
    wallet: Wallet,
    regime: int,
    band: Callable[[Decimal], bool],
    *,
    proposed: bool,
    volume_passed: bool = True,
) -> Iterator[tuple[Decimal, Decimal]]:
    notionals = [n for n in NOTIONALS if band(n)]
    if regime < 2:
        selector = MARKETS[regime][1]
        for volume, notional in product(VOLUMES, notionals):
            market = selector(volume)
            if market is None:
                continue
            yield score(
                wallet_confidence(wallet, notional, proposed=proposed),
                size_signal(market, notional, volume_passed=volume_passed),
            ), notional
        return
    market = UNAVAILABLE[regime - 2][1]
    for notional in notionals:
        yield score(
            wallet_confidence(wallet, notional, proposed=proposed),
            size_signal(market, notional, volume_passed=False),
        ), notional


REGIME_NAMES = tuple(name for name, _ in MARKETS) + tuple(name for name, _ in UNAVAILABLE)


def reachability_table() -> list[str]:
    rows = [
        "| Wallet evidence | Market evidence | Trade | Max score | Reaches 0.80? |",
        "|---|---|---|---|---|",
    ]
    for wallet, regime, (band_name, band) in product(WALLETS, range(4), BANDS):
        best = max((s for s, _ in grid(wallet, regime, band, proposed=True)), default=None)
        if best is None:
            continue
        verdict = "yes" if best >= THRESHOLD else "**no**"
        rows.append(
            f"| {wallet.name} | {REGIME_NAMES[regime]} | {band_name} | {best} | {verdict} |"
        )
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


def sensitivity_table() -> list[str]:
    """Grid points (not live frequencies) at or above candidate thresholds, unknown-age wallet."""
    wallet = WALLETS[0]
    scores = [
        s
        for regime, (_, band) in product(range(4), BANDS)
        for s, _ in grid(wallet, regime, band, proposed=True)
    ]
    rows = [
        f"| Threshold | Grid points >= threshold (of {len(scores)}) | Highest score among them |",
        "|---|---|---|",
    ]
    for threshold in ("0.80", "0.78", "0.75", "0.70", "0.60", "0.40"):
        hit = [s for s in scores if s >= Decimal(threshold)]
        rows.append(f"| {threshold} | {len(hit)} | {max(hit) if hit else '-'} |")
    return rows


def unknown_confidence_needed() -> str:
    """Smallest unknown-age confidence (0.001 steps) whose best niche case reaches 0.80."""
    size = size_signal(Market("a", Decimal(30000), False), Decimal(3250), volume_passed=True)
    for milli in range(500, 1001):
        if score(milli / 1000, size) >= THRESHOLD:
            return f"{milli / 1000:.3f}"
    return "none"


def report() -> str:
    parts = ["# Threshold reachability (generated by reachability.py)", ""]
    parts += ["## Constants verified against product source", ""] + verify_constants() + [""]
    parts += (
        [
            "## Proposed 004 rules: maximum attainable score",
            "",
            "Book depth is non-operational (FR-006). Grid: notionals "
            + ", ".join(f"${n:,}" for n in NOTIONALS)
            + "; usable volumes "
            + ", ".join(f"${v:,}" for v in VOLUMES)
            + ". Max over the grid; the volume ratio that saturates the 3x cap is >= 0.06.",
            "",
        ]
        + reachability_table()
        + [""]
    )
    parts += (
        ["## 003.1 rules as wired on main today (live pipeline passes no volume, G-022)", ""]
        + old_table()
        + [""]
    )
    parts += (
        [
            "## Sensitivity: unknown-age wallet, proposed rules",
            "",
            "Grid points are illustrative combinations, **not** live frequencies.",
            "",
        ]
        + sensitivity_table()
        + [""]
    )
    parts += [
        "## Recalibration arithmetic",
        "",
        f"- Smallest unknown-age confidence that lets the best niche case reach 0.80: {unknown_confidence_needed()} "
        "(vs the proposed exact 0.500).",
        "- With confidence fixed at 0.500 the highest unknown-age score is 0.780 for any trade size, so no "
        "threshold-0.80 alert is possible while age is unknown.",
        "",
    ]
    return "\n".join(parts)


def main(argv: list[str]) -> int:
    text = report() + "\n"
    if len(argv) == 3 and argv[1] == "--check":
        return 0 if Path(argv[2]).read_text() == text else 1
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
