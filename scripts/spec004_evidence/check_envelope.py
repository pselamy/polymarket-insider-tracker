"""Validate the 004.1 scoring_config schema and examples (contracts/scoring-config.md).

Usage: python3 -m scripts.spec004_evidence.check_envelope        # exit 0 = schema, examples and reconstruction agree

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
from collections.abc import Callable
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal
from itertools import product
from pathlib import Path
from typing import Any, TypeGuard, cast

from scripts.spec004_evidence.replay_envelopes import (
    GROUP_STATUSES,
    POST_T027,
    PRE_T027,
    RAW_BOUND_BYTES,
    RESERVED_KEYS,
    build_envelope,
    canonical,
    has_nonfinite,
    replay_specs,
)

CONTRACTS = (
    Path(__file__).resolve().parents[2] / "specs" / "004-truthful-detection-contract" / "contracts"
)
RAW_EXAMPLES = {
    "scoring-config-004.1.nonfinite.example.json": "live-invalid-nonfinite-nan-token",
    "scoring-config-004.1.truncated.example.json": "live-invalid-oversize-string",
}
POSTURE_EXAMPLES = {
    "scoring-config-004.1.example.json": PRE_T027,
    "scoring-config-004.1.post-t027.example.json": POST_T027,
}
EXAMPLE_CASE = "live-available-niche-saturated"
DOCUMENTS = (
    *POSTURE_EXAMPLES,
    "scoring-config-004.1.failure-row.json",
    *RAW_EXAMPLES,
)
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


def is_object(value: object) -> TypeGuard[dict[str, Any]]:
    return isinstance(value, dict)


def is_array(value: object) -> TypeGuard[list[Any]]:
    return isinstance(value, list)


def resolve(ref: str, root: dict[str, Any]) -> dict[str, Any]:
    node: Any = root
    for part in ref.removeprefix("#/").split("/"):
        node = node[part]
    return cast(dict[str, Any], node)


def type_names(schema: dict[str, Any]) -> list[str]:
    declared = schema["type"]
    return cast(list[str], declared) if isinstance(declared, list) else [str(declared)]


def const_errors(value: Any, schema: dict[str, Any], path: str) -> list[str]:
    if "const" not in schema or value == schema["const"]:
        return []
    return [f"{path}: expected {schema['const']!r}, got {value!r}"]


def enum_errors(value: Any, schema: dict[str, Any], path: str) -> list[str]:
    if "enum" not in schema or value in schema["enum"]:
        return []
    return [f"{path}: {value!r} not in enum"]


def type_errors(value: Any, schema: dict[str, Any], path: str) -> list[str]:
    if "type" not in schema:
        return []
    names = type_names(schema)
    if any(matches_type(value, n) for n in names):
        return []
    return [f"{path}: type {type(value).__name__} not in {names}"]


def one_of_errors(value: Any, schema: dict[str, Any], root: dict[str, Any], path: str) -> list[str]:
    if "oneOf" not in schema:
        return []
    passing = [s for s in schema["oneOf"] if not validate(value, s, root, path)]
    if len(passing) == 1:
        return []
    return [f"{path}: matches {len(passing)} oneOf branches"]


def item_errors(value: Any, schema: dict[str, Any], root: dict[str, Any], path: str) -> list[str]:
    if not is_array(value) or "items" not in schema:
        return []
    errors: list[str] = []
    for index, item in enumerate(value):
        errors += validate(item, schema["items"], root, f"{path}[{index}]")
    return errors


def validate(
    value: Any, schema: dict[str, Any], root: dict[str, Any], path: str = "$"
) -> list[str]:
    if "$ref" in schema:
        return validate(value, resolve(schema["$ref"], root), root, path)
    errors = const_errors(value, schema, path) + enum_errors(value, schema, path)
    wrong_type = type_errors(value, schema, path)
    if wrong_type:
        return errors + wrong_type
    errors += one_of_errors(value, schema, root, path)
    errors += scalar_errors(value, schema, path)
    if isinstance(value, dict):
        errors += object_errors(cast(dict[str, Any], value), schema, root, path)
    return errors + item_errors(value, schema, root, path)


def string_errors(value: str, schema: dict[str, Any], path: str) -> list[str]:
    errors: list[str] = []
    if "pattern" in schema and not re.search(schema["pattern"], value):
        errors.append(f"{path}: {value!r} fails pattern")
    if "maxLength" in schema and len(value) > schema["maxLength"]:
        errors.append(f"{path}: longer than {schema['maxLength']}")
    return errors


def integer_errors(value: int, schema: dict[str, Any], path: str) -> list[str]:
    errors: list[str] = []
    if "minimum" in schema and value < schema["minimum"]:
        errors.append(f"{path}: below minimum")
    if "maximum" in schema and value > schema["maximum"]:
        errors.append(f"{path}: above maximum")
    return errors


def scalar_errors(value: Any, schema: dict[str, Any], path: str) -> list[str]:
    if isinstance(value, str):
        return string_errors(value, schema, path)
    if isinstance(value, int) and not isinstance(value, bool):
        return integer_errors(value, schema, path)
    return []


def property_errors(
    key: str, item: Any, schema: dict[str, Any], root: dict[str, Any], path: str
) -> list[str]:
    props = schema.get("properties", {})
    extra = schema.get("additionalProperties", True)
    if key in props:
        return validate(item, props[key], root, f"{path}.{key}")
    if extra is False:
        return [f"{path}: unexpected key {key!r}"]
    if isinstance(extra, dict):
        return validate(item, cast(dict[str, Any], extra), root, f"{path}.{key}")
    return []


def object_errors(
    value: dict[str, Any], schema: dict[str, Any], root: dict[str, Any], path: str
) -> list[str]:
    errors = [f"{path}: missing {k!r}" for k in schema.get("required", []) if k not in value]
    for key, item in value.items():
        errors += property_errors(key, item, schema, root, path)
    return errors


def score_errors(doc: dict[str, Any], evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
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
    return errors


def wallet_contribution_errors(weights: dict[str, Decimal], evidence: dict[str, Any]) -> list[str]:
    confidence = evidence["wallet"]["confidence"]
    if confidence is None:
        return []
    want = Decimal(confidence) * weights["fresh_wallet"]
    if want == Decimal(evidence["contributions"]["fresh_wallet"]):
        return []
    return ["fresh_wallet contribution != confidence x weight"]


def size_contribution_errors(weights: dict[str, Decimal], evidence: dict[str, Any]) -> list[str]:
    size, contributions = evidence["size"], evidence["contributions"]
    if size["confidence"] is None:
        return []
    errors: list[str] = []
    conf = Decimal(size["confidence"])
    if conf * weights["size_anomaly"] != Decimal(contributions["size_anomaly"]):
        errors.append("size_anomaly contribution != confidence x weight")
    niche_want = conf * weights["niche_market"] if size["niche"] else Decimal(0)
    if niche_want != Decimal(contributions["niche_market"]):
        errors.append("niche_market contribution != confidence x weight (niche only)")
    return errors


def reconstruct(doc: dict[str, Any]) -> list[str]:
    """Cross-field rules stated in contracts/scoring-config.md."""
    evidence = doc["evaluation_evidence"]
    if evidence is None:
        return []
    weights = {k: Decimal(v) for k, v in doc["weights"].items()}
    return (
        score_errors(doc, evidence)
        + wallet_contribution_errors(weights, evidence)
        + size_contribution_errors(weights, evidence)
        + group_errors(evidence)
        + state_errors(evidence)
    )


def gated_wallet_errors(evidence: dict[str, Any]) -> list[str]:
    """FR-007c: before T027 the wallet group carries no confidence, factors or contribution."""
    wallet = evidence["wallet"]
    if wallet["group_status"] != PRE_T027:
        return []
    errors: list[str] = []
    if wallet["confidence"] is not None or wallet["factors"]:
        errors.append("pre-T027 wallet must have null confidence and no factors")
    if Decimal(evidence["contributions"]["fresh_wallet"]) != 0:
        errors.append("pre-T027 wallet contribution must be 0")
    return errors


def group_errors(evidence: dict[str, Any]) -> list[str]:
    """signals_triggered counts scoring groups only; the 1.2 bonus applies exactly at two."""
    wallet_scores = evidence["wallet"]["confidence"] is not None
    groups = int(wallet_scores) + int(evidence["size"]["signal_emitted"])
    errors = gated_wallet_errors(evidence)
    if evidence["result"]["signals_triggered"] != groups:
        errors.append(f"signals_triggered != scoring groups ({groups})")
    if (evidence["contributions"]["bonus"] == "1.2") != (groups == 2):
        errors.append("bonus must be 1.2 exactly when two groups score")
    return errors


def available_volume_errors(evidence: dict[str, Any]) -> list[str]:
    size = evidence["size"]
    errors: list[str] = []
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
    return errors


def age_sign_errors(size: dict[str, Any]) -> list[str]:
    age = size["daily_volume_age_seconds"]
    if age is None or Decimal(age) >= 0:
        return []
    if (size["volume_state"], size["volume_reason"]) == ("invalid_time", "future"):
        return []
    return ["negative age outside invalid_time/future"]


RAW_TAGS = {"$nonfinite": "raw_nonfinite", "$truncated": "raw_truncated"}


def verbatim_raw_errors(raw: Any) -> list[str]:
    errors: list[str] = []
    if has_nonfinite(raw):
        errors.append("raw: non-finite number stored verbatim (must be a $nonfinite tag)")
    if is_object(raw) and RESERVED_KEYS & raw.keys():
        errors.append("raw: reserved key stored verbatim")
    if len(canonical(raw)) > RAW_BOUND_BYTES:
        errors.append(f"raw: verbatim value over {RAW_BOUND_BYTES} bytes (must be a marker)")
    return errors


def stored_raw_errors(raw: Any) -> list[str]:
    """The raw-value rule: markers match their defs; anything else is verbatim, bounded and finite."""
    schema = load("scoring-config-004.1.schema.json")
    tags = [tag for tag in RAW_TAGS if is_object(raw) and tag in raw]
    if not tags:
        return verbatim_raw_errors(raw)
    return validate(raw, schema["$defs"][RAW_TAGS[tags[0]]], schema, "raw")


def tag_state_errors(size: dict[str, Any]) -> list[str]:
    """A non-finite tag is a malformed observation: the state is always `invalid`."""
    raw = size["volume24hr_raw"]
    if is_object(raw) and "$nonfinite" in raw and size["volume_state"] != "invalid":
        return ["raw: a $nonfinite tag requires volume_state invalid"]
    return []


def volume_state_errors(evidence: dict[str, Any]) -> list[str]:
    size = evidence["size"]
    available = size["volume_state"] == "available"
    errors: list[str] = []
    if available != (size["daily_volume"] is not None):
        errors.append("daily_volume must be non-null exactly when volume_state is available")
    if available != (size["volume_impact"] is not None):
        errors.append("volume_impact must be non-null exactly when volume_state is available")
    errors += age_sign_errors(size)
    errors += tag_state_errors(size) + stored_raw_errors(size["volume24hr_raw"])
    return errors + (available_volume_errors(evidence) if available else [])


def wallet_state_errors(wallet: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if (wallet["age_state"] == "known") != (wallet["age_basis"] != "none"):
        errors.append("age_state known requires a basis other than none")
    scoring = wallet["group_status"] == POST_T027
    unknown = wallet["classification"] == "low_activity_unknown_age"
    if scoring and unknown and wallet["confidence"] != "0.500":
        errors.append("unknown-age confidence must be exactly 0.500")
    return errors


def state_errors(evidence: dict[str, Any]) -> list[str]:
    return volume_state_errors(evidence) + wallet_state_errors(evidence["wallet"])


def mutations(doc: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    cases: list[tuple[str, dict[str, Any]]] = []

    def variant(name: str, edit: Callable[[dict[str, Any]], object]) -> None:
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
    variant(
        "non-numeric age",
        lambda d: d["evaluation_evidence"]["size"].update(daily_volume_age_seconds="x"),
    )
    variant("wrong version", lambda d: d.update(evidence_schema="003.1"))
    variant("bool as int", lambda d: d["detector_config"].update(fresh_transaction_boundary=True))
    variant("no group status", lambda d: d["evaluation_evidence"]["wallet"].pop("group_status"))
    variant(
        "bad group status",
        lambda d: d["evaluation_evidence"]["wallet"].update(group_status="unqualified"),
    )
    return cases


def gated_scoring(doc: dict[str, Any]) -> None:
    """A pre-T027 envelope whose wallet scores as if qualified (consistent arithmetic)."""
    evidence = doc["evaluation_evidence"]
    evidence["wallet"].update(confidence="0.500", factors={"base": "0.5"})
    evidence["contributions"].update(fresh_wallet="0.200", bonus="1.2", capped_score="0.78000")
    evidence["result"].update(signals_triggered=2, weighted_score="0.780")


def gated_bonus(doc: dict[str, Any]) -> None:
    doc["evaluation_evidence"]["contributions"].update(bonus="1.2", capped_score="0.54000")
    doc["evaluation_evidence"]["result"].update(weighted_score="0.540")


def gated_count(doc: dict[str, Any]) -> None:
    doc["evaluation_evidence"]["result"].update(signals_triggered=2)


def gated_mutation_failures() -> list[str]:
    """Pre-T027 envelopes the cross-field rules must reject (schema-valid, arithmetic-consistent)."""
    failures: list[str] = []
    for name, edit in (
        ("wallet scores", gated_scoring),
        ("bonus", gated_bonus),
        ("count", gated_count),
    ):
        doc = load("scoring-config-004.1.example.json")
        edit(doc)
        if not reconstruct(doc):
            failures.append(f"pre-T027 mutation accepted: {name}")
    return failures


def raw_mutation_failures() -> list[str]:
    """Stored raw values the rule must reject."""
    digest = "0" * 64
    bad: dict[str, Any] = {
        "bare NaN float": float("nan"),
        "nested NaN float": [float("nan")],
        "reserved key verbatim": {"$truncated": False},
        "oversize verbatim": "x" * 300,
        "tag with wrong spelling": {"$nonfinite": "nan"},
        "marker without digest": {
            "$truncated": True,
            "bytes": 9,
            "reason": "oversize",
            "type": "string",
        },
        "marker with bad reason": {
            "$truncated": True,
            "bytes": 9,
            "reason": "big",
            "sha256": digest,
            "type": "string",
        },
    }
    return [f"raw mutation accepted: {n}" for n, v in bad.items() if not stored_raw_errors(v)]


def load(name: str) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads((CONTRACTS / name).read_text()))


def document_failures(name: str, schema: dict[str, Any]) -> list[str]:
    doc = load(name)
    found = validate(doc, schema, schema) + reconstruct(doc)
    failures = [f"{name}: {e}" for e in found]
    canonical = json.dumps(doc, sort_keys=True, separators=(",", ":"))
    if json.loads(canonical) != doc:
        failures.append(f"{name}: canonical form changes the document")
    return failures


def mutation_failures(schema: dict[str, Any]) -> list[str]:
    example = load("scoring-config-004.1.example.json")
    return [
        f"mutation accepted by schema: {label}"
        for label, bad in mutations(example)
        if not validate(bad, schema, schema)
    ]


def replay_failures(schema: dict[str, Any]) -> list[str]:
    """One envelope per replay-live case: schema, reconstruction and state rules must all hold."""
    template = load("scoring-config-004.1.example.json")
    failures: list[str] = []
    for spec, status in product(replay_specs(), GROUP_STATUSES):
        doc = build_envelope(spec, template, status)
        found = validate(doc, schema, schema) + reconstruct(doc)
        failures += [f"replay {spec['case_id']} {status}: {e}" for e in found]
    return failures


def example_drift() -> list[str]:
    """The committed examples must equal what the builder produces for their cases and postures."""
    template = load("scoring-config-004.1.example.json")
    by_id = {spec["case_id"]: spec for spec in replay_specs()}
    expected = {name: (case, PRE_T027) for name, case in RAW_EXAMPLES.items()}
    expected |= {name: (EXAMPLE_CASE, status) for name, status in POSTURE_EXAMPLES.items()}
    return [
        f"{name} drifts from replay case {case} ({status})"
        for name, (case, status) in expected.items()
        if load(name) != build_envelope(by_id[case], template, status)
    ]


def main() -> int:
    schema = load("scoring-config-004.1.schema.json")
    failures: list[str] = replay_failures(schema)
    for name in DOCUMENTS:
        failures += document_failures(name, schema)
    failures += example_drift()
    failures += mutation_failures(schema) + raw_mutation_failures() + gated_mutation_failures()
    for failure in failures:
        print(failure)
    print("ok" if not failures else f"{len(failures)} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
