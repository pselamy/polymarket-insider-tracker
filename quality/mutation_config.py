"""Derive only transport configuration, with independent native TOML readback."""

import importlib
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import cast

from quality.report_data import record


def execution_config(path: Path, urls: list[str]) -> Path:
    """Keep original scope/operators/test/deadline settings byte-for-byte intact."""
    original = path.read_bytes()
    config = record(tomllib.loads(original.decode("utf-8"))["cosmic-ray"])
    if record(config["distributor"]) != {"name": "local"}:
        raise ValueError("mutation base distributor must be local")
    config["distributor"] = {"name": "http", "http": {"worker-urls": urls}}
    module = importlib.import_module("cosmic_ray.config")
    serialize = cast(Callable[[object], str], module.serialize_config)
    output = serialize(config)
    if tomllib.loads(output)["cosmic-ray"] != config:
        raise ValueError("native transport serialization changed mutation policy")
    if path.read_bytes() != original:
        raise ValueError("original mutation configuration changed")
    target = path.with_name("mutation-execution.toml")
    target.write_text(output, encoding="utf-8")
    return target
