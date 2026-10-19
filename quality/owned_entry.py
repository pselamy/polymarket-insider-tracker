"""A dedicated supervisor writes evidence only after the complete owned cleanup."""

import json
import os
from dataclasses import asdict
from pathlib import Path

from quality.owned_process import execute
from quality.owned_receipt import positive_timeout
from quality.report_data import array, record, text


def supervise(request_path: Path) -> None:
    value: object = json.loads(request_path.read_text(encoding="utf-8"))
    request = record(value)
    command = [text(item) for item in array(request["command"])]
    if not command:
        raise ValueError("supervised command is empty")
    root = Path(text(request["root"]))
    timeout = float(text(request["timeout"]))
    positive_timeout(timeout)
    nonce = text(request["nonce"])
    receipt = Path(text(request["receipt"]))
    result = execute(command, root, timeout, dict(os.environ))
    completion = asdict(result)
    completion["nonce"] = nonce
    receipt.write_text(json.dumps(completion), encoding="utf-8")
