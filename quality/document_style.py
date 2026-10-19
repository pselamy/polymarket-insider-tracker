"""Require every authored Markdown file to match pinned GFM formatting."""

import hashlib
import json
from collections.abc import Callable
from importlib import metadata
from pathlib import Path
from typing import cast

import mdformat

from quality.document_links import markdown_inputs
from quality.support_files import authored

VERSIONS = {"mdformat": "1.0.0", "mdformat-gfm": "1.0.0"}


def formatted(source: str) -> str:
    """Adapt the formatter's broad external signature to our fixed GFM call."""
    formatter: object = vars(mdformat).get("text")
    if not callable(formatter):
        raise TypeError("Markdown formatter text API is unavailable")
    return cast(Callable[..., str], formatter)(source, extensions={"gfm"})


def require_versions() -> None:
    """Fail if installed formatter or GFM plugin differs from the lock."""
    for package, expected in VERSIONS.items():
        if metadata.version(package) != expected:
            raise ValueError(f"required Markdown formatter version differs: {package}")


def verify_format(inputs: dict[Path, bytes]) -> None:
    """Check pinned GFM style without modifying authored documents."""
    for path, data in inputs.items():
        source = data.decode("utf-8")
        if formatted(source) != source:
            raise ValueError(f"authored Markdown style differs: {path}")


def output_path(root: Path) -> Path:
    """Reject stale or redirected style receipts."""
    output = root / ".quality-results/document-style.json"
    if output.is_symlink() or output.parent.is_symlink():
        raise ValueError("Markdown style report may not be a symlink")
    output.unlink(missing_ok=True)
    return output


def verify_document_style(root: Path) -> None:
    """Check complete authored Markdown scope and hash-bind the result."""
    root = root.resolve()
    output = output_path(root)
    inputs = markdown_inputs(root, set(authored(root, root)))
    require_versions()
    verify_format(inputs)
    if any(path.read_bytes() != data for path, data in inputs.items()):
        raise ValueError("documentation changed while style was checked")
    output.parent.mkdir(exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "versions": VERSIONS,
                "files": {
                    path.relative_to(root).as_posix(): hashlib.sha256(data).hexdigest()
                    for path, data in inputs.items()
                },
            },
            sort_keys=True,
        )
        + "\n"
    )
