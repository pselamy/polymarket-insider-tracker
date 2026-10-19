# Evidence tools (literate source)

**Why this is a Markdown file and not four `.py` files**: `tests/tooling/test_verify.py` requires every
tracked `*.py` file to sit inside the vulture/complexipy scope (`src tests scripts alembic conftest.py
quality`), and this revision is spec-only (no change to those directories or to gate configuration).
A tracked Python file under `specs/` would therefore fail the compatibility profile. Rather than hide
code from a gate, the tools are kept here as reviewable source, extracted to a private temporary
directory and run by [`run-evidence.sh`](run-evidence.sh). They are **not** under the Python quality
gates (types, complexity, dead code); the gap is recorded in
[decision-packet.md](../decision-packet.md) (known limits). Moving them under `scripts/` with the
full gates is the clean follow-up.

```bash
sh specs/004-truthful-detection-contract/evidence/run-evidence.sh reachability --check specs/004-truthful-detection-contract/evidence/reachability.md
sh specs/004-truthful-detection-contract/evidence/run-evidence.sh replay_live --check specs/004-truthful-detection-contract/evidence/replay-live/results.csv
sh specs/004-truthful-detection-contract/evidence/run-evidence.sh check_envelope
sh specs/004-truthful-detection-contract/evidence/run-evidence.sh check_source_hashes
```

Rules: Python standard library only; no import from `src/` (the product source is read as text only,
by `reachability.verify_constants`); every tool exits 0 when its committed artifact is current.
Each block below is preceded by a marker comment naming the file it is extracted to.

## `reachability.py`

Threshold reachability table generator (output committed as `reachability.md`).

<!-- file: reachability.py -->
```python
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

import os
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from itertools import product
from pathlib import Path

QUANTUM = Decimal("0.001")
ROOT = Path(os.environ["REPO_ROOT"])
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
```

## `replay_live.py`

Live-realistic replay; executes the volume-state precedence table (output committed as `replay-live/results.csv`).

<!-- file: replay_live.py -->
```python
"""Live-realistic replay addendum for slice 004 (research.md R10, evidence/replay-live.md).

Usage:
    python3 replay_live.py                 # print the results CSV
    python3 replay_live.py --check FILE    # exit 1 unless FILE equals the CSV byte for byte

Stdlib only; never imports from src/. Scoring arithmetic is shared with reachability.py (whose
constants are textually verified against the product source). The volume-state classifier below is
the executable form of the precedence table in data-model.md section 1; the cases cover every
state and reason. ``old_score_as_wired`` is the 003.1 rule with the live wiring (no volume passed).
"""

from __future__ import annotations

import csv
import io
import json
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from reachability import Market, Wallet, score, size_signal, verify_constants, wallet_confidence

HERE = Path(os.environ["EVIDENCE_DIR"])
QUALIFIED_SOURCES = frozenset({"gamma-markets-v1", "caller"})
NICHE_PRONE = frozenset({"science", "tech", "finance", "other"})
RECENCY_SECONDS = 600
BOUNDARY = 5  # B: exclusive nonce bound (proposed); old rule is nonce <= 5
THRESHOLDS = ("0.80", "0.78")
COLUMNS = (
    "case_id",
    "volume_state",
    "volume_reason",
    "wallet_class",
    "wallet_confidence",
    "size_confidence",
    "niche_basis",
    "groups",
    "old_score_as_wired",
    "proposed_score",
    *(f"alerts_at_{t}" for t in THRESHOLDS),
    "purpose",
)


@dataclass(frozen=True)
class Observation:
    kind: str
    reason: str | None
    source: str | None
    present: bool
    raw: object
    acquired_age_seconds: object


def parse_finite(raw: object) -> Decimal | None:
    """A finite Decimal from a JSON number or numeric string; None for anything else."""
    if isinstance(raw, bool) or not isinstance(raw, int | float | str):
        return None
    try:
        value = Decimal(str(raw).strip()) if isinstance(raw, str) else Decimal(str(raw))
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def classify(obs: Observation, explicit: Decimal | None) -> tuple[str, str, Decimal | None]:
    """First-match precedence P1..P8; returns (state, reason, usable_volume)."""
    if explicit is not None:
        if explicit > 0:
            return "available", "caller", explicit
        return "nonpositive", "zero" if explicit == 0 else "negative", None
    if obs.kind == "unavailable":
        return "unavailable", obs.reason or "unknown", None
    if obs.source not in QUALIFIED_SOURCES:
        return "unqualified", "no_provenance" if obs.source is None else "unknown_source", None
    if not obs.present or obs.raw is None:
        return "missing", "null_or_absent", None
    value = parse_finite(obs.raw)
    if value is None:
        return "invalid", "not_a_finite_number", None
    if value <= 0:
        return "nonpositive", "zero" if value == 0 else "negative", None
    age = obs.acquired_age_seconds
    if not isinstance(age, int) or age < 0:
        return "invalid_time", str(age if not isinstance(age, int) else "future"), None
    if age > RECENCY_SECONDS:
        return "stale", "older_than_600s", None
    return "available", "gamma", value


def optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def build_observation(raw: dict[str, object]) -> Observation:
    return Observation(
        kind=str(raw.get("kind", "observed")),
        reason=optional_str(raw.get("reason")),
        source=optional_str(raw.get("source")),
        present=bool(raw.get("present", True)),
        raw=raw.get("raw"),
        acquired_age_seconds=raw.get("acquired_age_seconds"),
    )


def merged(defaults: dict[str, Any], case: dict[str, Any]) -> dict[str, Any]:
    out = {**defaults, **{k: v for k, v in case.items() if k != "observation"}}
    observation = case.get("observation", {})
    base = {} if observation.get("kind") == "unavailable" else dict(defaults["observation"])
    out["observation"] = {**base, **observation}
    return out


def evaluate(spec: dict[str, Any]) -> dict[str, object]:
    notional = Decimal(str(spec["notional"]))
    nonce = int(spec["nonce"])
    explicit = (
        Decimal(str(spec["explicit_daily_volume"])) if spec["explicit_daily_volume"] else None
    )
    state, reason, volume = classify(build_observation(spec["observation"]), explicit)
    category = str(spec["category"])
    real_metadata = spec["category_source"] == "metadata"
    market = Market("m", volume, category in NICHE_PRONE and real_metadata)
    wallet = Wallet("unknown age", nonce, None)
    wconf = wallet_confidence(wallet, notional, proposed=True) if nonce < BOUNDARY else None
    size = size_signal(market, notional, volume_passed=True)
    proposed = score(wconf, size)
    old_market = Market(
        "m", None, category in NICHE_PRONE
    )  # live wiring: no volume, category as given
    old_w = wallet_confidence(wallet, notional, proposed=False) if nonce <= 5 else None
    old = score(old_w, size_signal(old_market, notional, volume_passed=False))
    if size is None:
        basis = "none"
    elif volume is not None:
        basis = "numeric" if size[1] else "numeric-not-niche"
    else:
        basis = "category" if size[1] else "none"
    return {
        "case_id": spec["case_id"],
        "volume_state": state,
        "volume_reason": reason,
        "wallet_class": "low_activity_unknown_age" if wconf is not None else "none",
        "wallet_confidence": "" if wconf is None else f"{wconf:.3f}",
        "size_confidence": "" if size is None else f"{size[0]:.3f}",
        "niche_basis": basis,
        "groups": int(wconf is not None) + int(size is not None),
        "old_score_as_wired": old,
        "proposed_score": proposed,
        **{f"alerts_at_{t}": proposed >= Decimal(t) for t in THRESHOLDS},
        "purpose": spec["purpose"],
    }


def results() -> list[dict[str, object]]:
    corpus = json.loads((HERE / "replay-live" / "cases.json").read_text())
    return [evaluate(merged(corpus["defaults"], case)) for case in corpus["cases"]]


def render() -> str:
    verify_constants()
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(results())
    return buffer.getvalue()


def main(argv: list[str]) -> int:
    text = render()
    if len(argv) == 3 and argv[1] == "--check":
        return 0 if Path(argv[2]).read_text() == text else 1
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
```

## `check_envelope.py`

Validates the 004.1 JSON Schema, example documents and cross-field reconstruction.

<!-- file: check_envelope.py -->
```python
"""Validate the 004.1 scoring_config schema and examples (contracts/scoring-config.md).

Usage: python3 check_envelope.py        # exit 0 = schema, examples and reconstruction agree

Stdlib only; never imports from src/. ``validate`` implements the small JSON Schema subset the
schema file uses ($ref/$defs, type, enum, const, pattern, required, properties, additionalProperties,
items, oneOf, minimum, maximum, maxLength). The checks beyond the schema are the cross-field rules
the contract states in prose: score reconstruction, ratio reconstruction and state consistency.
"""

from __future__ import annotations

import copy
import json
import os
import re
import sys
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import Any

CONTRACTS = Path(os.environ["EVIDENCE_DIR"]).parent / "contracts"
TYPES: dict[str, tuple[type, ...]] = {
    "object": (dict,),
    "array": (list,),
    "string": (str,),
    "integer": (int,),
    "boolean": (bool,),
    "null": (type(None),),
}


def matches_type(value: Any, name: str) -> bool:
    if name == "integer" and isinstance(value, bool):
        return False
    return isinstance(value, TYPES[name])


def resolve(ref: str, root: dict[str, Any]) -> dict[str, Any]:
    node: Any = root
    for part in ref.removeprefix("#/").split("/"):
        node = node[part]
    return node  # type: ignore[no-any-return]


def validate(
    value: Any, schema: dict[str, Any], root: dict[str, Any], path: str = "$"
) -> list[str]:
    if "$ref" in schema:
        return validate(value, resolve(schema["$ref"], root), root, path)
    errors: list[str] = []
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: expected {schema['const']!r}, got {value!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: {value!r} not in enum")
    if "type" in schema:
        names = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if not any(matches_type(value, n) for n in names):
            return [*errors, f"{path}: type {type(value).__name__} not in {names}"]
    if "oneOf" in schema:
        passing = [s for s in schema["oneOf"] if not validate(value, s, root, path)]
        if len(passing) != 1:
            errors.append(f"{path}: matches {len(passing)} oneOf branches")
    errors += scalar_errors(value, schema, path)
    if isinstance(value, dict):
        errors += object_errors(value, schema, root, path)
    if isinstance(value, list) and "items" in schema:
        for index, item in enumerate(value):
            errors += validate(item, schema["items"], root, f"{path}[{index}]")
    return errors


def scalar_errors(value: Any, schema: dict[str, Any], path: str) -> list[str]:
    errors: list[str] = []
    if isinstance(value, str):
        if "pattern" in schema and not re.search(schema["pattern"], value):
            errors.append(f"{path}: {value!r} fails pattern")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            errors.append(f"{path}: longer than {schema['maxLength']}")
    if isinstance(value, int) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{path}: below minimum")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{path}: above maximum")
    return errors


def object_errors(
    value: dict[str, Any], schema: dict[str, Any], root: dict[str, Any], path: str
) -> list[str]:
    errors = [f"{path}: missing {k!r}" for k in schema.get("required", []) if k not in value]
    props = schema.get("properties", {})
    extra = schema.get("additionalProperties", True)
    for key, item in value.items():
        if key in props:
            errors += validate(item, props[key], root, f"{path}.{key}")
        elif extra is False:
            errors.append(f"{path}: unexpected key {key!r}")
        elif isinstance(extra, dict):
            errors += validate(item, extra, root, f"{path}.{key}")
    return errors


def reconstruct(doc: dict[str, Any]) -> list[str]:
    """Cross-field rules stated in contracts/scoring-config.md."""
    evidence = doc["evaluation_evidence"]
    if evidence is None:
        return []
    errors: list[str] = []
    weights = {k: Decimal(v) for k, v in doc["weights"].items()}
    contributions = evidence["contributions"]
    total = sum(Decimal(contributions[k]) for k in ("fresh_wallet", "size_anomaly", "niche_market"))
    capped = min(total * Decimal(contributions["bonus"]), Decimal(1))
    if capped != Decimal(contributions["capped_score"]):
        errors.append(f"capped_score {contributions['capped_score']} != recomputed {capped}")
    quantized = capped.quantize(Decimal("0.001"), rounding=ROUND_HALF_EVEN)
    result = evidence["result"]
    if quantized != Decimal(result["weighted_score"]):
        errors.append(f"weighted_score {result['weighted_score']} != recomputed {quantized}")
    if result["should_alert"] != (quantized >= Decimal(doc["alert_threshold"])):
        errors.append("should_alert disagrees with weighted_score >= alert_threshold")
    wallet, size = evidence["wallet"], evidence["size"]
    if wallet["confidence"] is not None:
        want = Decimal(wallet["confidence"]) * weights["fresh_wallet"]
        if want != Decimal(contributions["fresh_wallet"]):
            errors.append("fresh_wallet contribution != confidence x weight")
    if size["confidence"] is not None:
        conf = Decimal(size["confidence"])
        if conf * weights["size_anomaly"] != Decimal(contributions["size_anomaly"]):
            errors.append("size_anomaly contribution != confidence x weight")
        niche_want = conf * weights["niche_market"] if size["niche"] else Decimal(0)
        if niche_want != Decimal(contributions["niche_market"]):
            errors.append("niche_market contribution != confidence x weight (niche only)")
    return errors + state_errors(evidence)


def state_errors(evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    size, wallet = evidence["size"], evidence["wallet"]
    available = size["volume_state"] == "available"
    if available != (size["daily_volume"] is not None):
        errors.append("daily_volume must be non-null exactly when volume_state is available")
    if available != (size["volume_impact"] is not None):
        errors.append("volume_impact must be non-null exactly when volume_state is available")
    if available:
        ratio = (Decimal(evidence["notional"]) / Decimal(size["daily_volume"])).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_EVEN
        )
        if ratio != Decimal(size["volume_impact"]):
            errors.append(f"volume_impact {size['volume_impact']} != notional/daily_volume {ratio}")
        acquired = datetime.fromisoformat(size["daily_volume_acquired_at"])
        age = datetime.fromisoformat(evidence["evaluated_at"]) - acquired
        if Decimal(str(age.total_seconds())) != Decimal(size["daily_volume_age_seconds"]):
            errors.append("daily_volume_age_seconds != evaluated_at - acquired_at")
        if not 0 <= age.total_seconds() <= 600:
            errors.append("available volume outside the 0-600 s window")
    if (wallet["age_state"] == "known") != (wallet["age_basis"] != "none"):
        errors.append("age_state known requires a basis other than none")
    if wallet["classification"] == "low_activity_unknown_age" and wallet["confidence"] != "0.500":
        errors.append("unknown-age confidence must be exactly 0.500")
    return errors


def mutations(doc: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    cases: list[tuple[str, dict[str, Any]]] = []

    def variant(name: str, edit: Any) -> None:
        clone = copy.deepcopy(doc)
        edit(clone)
        cases.append((name, clone))

    variant("float score", lambda d: d["evaluation_evidence"]["result"].update(weighted_score=0.78))
    variant("missing key", lambda d: d.pop("detector_failures"))
    variant("extra key", lambda d: d.update(surprise=1))
    variant("bad enum", lambda d: d["evaluation_evidence"]["size"].update(volume_state="zero"))
    variant(
        "naive stamp", lambda d: d["evaluation_evidence"].update(evaluated_at="2026-10-19T12:00:00")
    )
    variant("wrong version", lambda d: d.update(evidence_schema="003.1"))
    variant("bool as int", lambda d: d["detector_config"].update(fresh_transaction_boundary=True))
    return cases


def main() -> int:
    schema = json.loads((CONTRACTS / "scoring-config-004.1.schema.json").read_text())
    failures: list[str] = []
    for name in ("scoring-config-004.1.example.json", "scoring-config-004.1.failure-row.json"):
        doc = json.loads((CONTRACTS / name).read_text())
        failures += [f"{name}: {e}" for e in validate(doc, schema, schema) + reconstruct(doc)]
        canonical = json.dumps(doc, sort_keys=True, separators=(",", ":"))
        if json.loads(canonical) != doc:
            failures.append(f"{name}: canonical form changes the document")
    example = json.loads((CONTRACTS / "scoring-config-004.1.example.json").read_text())
    for label, bad in mutations(example):
        if not validate(bad, schema, schema):
            failures.append(f"mutation accepted by schema: {label}")
    for failure in failures:
        print(failure)
    print("ok" if not failures else f"{len(failures)} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
```

## `check_source_hashes.py`

Compares the hashes recorded in `replay/summary.json` with a checkout.

<!-- file: check_source_hashes.py -->
```python
"""Compare the source hashes recorded in replay/summary.json with a checked-out tree.

Usage: python3 check_source_hashes.py [REPO_ROOT]   (default: the repository containing this file)

Stdlib only; it never imports from src/. Exit 0 when every recorded hash matches, 1 otherwise.
The recorded hashes are the receipt of the replay's execution checkout, so a mismatch means
"changed since the replay", not "wrong"; read source-refresh-1bdccbc.md for the disposition.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

HERE = Path(os.environ["EVIDENCE_DIR"])
SOURCE_PREFIX = Path("src") / "polymarket_insider_tracker"


def main(argv: list[str]) -> int:
    root = Path(argv[1]).resolve() if len(argv) > 1 else Path(os.environ["REPO_ROOT"])
    recorded = json.loads((HERE / "replay" / "summary.json").read_text())["source_sha256"]
    drifted = 0
    for relative, expected in sorted(recorded.items()):
        actual = hashlib.sha256((root / SOURCE_PREFIX / relative).read_bytes()).hexdigest()
        status = "match" if actual == expected else "DRIFT"
        drifted += actual != expected
        print(f"{status:5} {relative} {actual}")
    return 1 if drifted else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
```
