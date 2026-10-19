"""Fail on typos in every authored Markdown file using a locked native scanner."""

import hashlib
import json
from pathlib import Path

from quality.document_links import markdown_inputs
from quality.report_data import is_object
from quality.support_files import authored
from quality.workflows import tool

VERSION = "typos-cli 1.50.3"
BASE = ["typos", "--isolated", "--hidden", "--no-ignore", "--format", "json"]


def names(root: Path, inputs: dict[Path, bytes]) -> list[str]:
    """Use exact relative paths so the native scan cannot broaden or shrink scope."""
    return sorted(path.relative_to(root).as_posix() for path in inputs)


def file_record(line: str) -> str:
    """Parse exactly one native spelling inventory record."""
    item: object = json.loads(line)
    if not is_object(item) or set(item) != {"type", "path"}:
        raise ValueError("native spelling inventory has malformed record")
    if item["type"] not in ("file",) or not isinstance(item["path"], str):
        raise ValueError("native spelling inventory has malformed file")
    return item["path"]


def verify_inventory(report: str, expected: list[str]) -> None:
    """Require a native file record for every independently discovered document."""
    found = [file_record(line) for line in report.splitlines()]
    if sorted(found) != expected:
        raise ValueError("native spelling file inventory differs from authored Markdown")


def output_path(root: Path) -> Path:
    """A stale or redirected spelling receipt cannot pass."""
    output = root / ".quality-results/document-spelling.json"
    if output.is_symlink() or output.parent.is_symlink():
        raise ValueError("spelling report may not be a symlink")
    output.unlink(missing_ok=True)
    return output


def verify_document_spelling(root: Path) -> None:
    """Verify native version, complete file inventory, empty findings and hashes."""
    root = root.resolve()
    output = output_path(root)
    inputs = markdown_inputs(root, set(authored(root, root)))
    paths = names(root, inputs)
    if tool(root, ["typos", "--version"]).strip() != VERSION:
        raise ValueError("required native typos version is missing or wrong")
    verify_inventory(tool(root, [*BASE, "--files", *paths]), paths)
    if tool(root, [*BASE, *paths]).strip():
        raise ValueError("native spelling scanner returned findings")
    if any(path.read_bytes() != data for path, data in inputs.items()):
        raise ValueError("documentation changed while spelling was checked")
    output.parent.mkdir(exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "version": VERSION,
                "files": {
                    path.relative_to(root).as_posix(): hashlib.sha256(data).hexdigest()
                    for path, data in inputs.items()
                },
            },
            sort_keys=True,
        )
        + "\n"
    )
