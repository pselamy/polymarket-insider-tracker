"""Derive only transport configuration, with independent native TOML readback."""

import tomllib
from pathlib import Path

from quality.native_api import native_callable
from quality.report_data import record


def execution_config(path: Path, urls: list[str]) -> Path:
    """Keep original scope/operators/test/deadline settings byte-for-byte intact."""
    original = path.read_bytes()
    config = record(tomllib.loads(original.decode("utf-8"))["cosmic-ray"])
    if record(config["distributor"]) != {"name": "local"}:
        raise ValueError("mutation base distributor must be local")
    config["distributor"] = {"name": "http", "http": {"worker-urls": urls}}
    output = native_callable("cosmic_ray.config", "serialize_config")(config)
    if not isinstance(output, str):
        raise TypeError("native transport serialization returned a non-string")
    if tomllib.loads(output)["cosmic-ray"] != config:
        raise ValueError("native transport serialization changed mutation policy")
    if path.read_bytes() != original:
        raise ValueError("original mutation configuration changed")
    target = path.with_name("mutation-execution.toml")
    target.write_text(output, encoding="utf-8")
    return target
