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
import re
import sys
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import Any

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"
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
