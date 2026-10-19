"""Check every authored Markdown link against a source-bound file inventory."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit

from markdown_it import MarkdownIt
from markdown_it.token import Token

from quality.support_files import authored


@dataclass(frozen=True)
class Document:
    anchors: set[str]
    links: list[str]


def slug(text: str) -> str:
    """Use stable lowercase heading fragments for local links."""
    return re.sub(r"[^\w -]", "", text.lower()).replace(" ", "-")


def child_links(token: Token) -> list[str]:
    """Collect ordinary links and image destinations from an inline token."""
    return [url for child in token.children or [] if (url := destination(child)) is not None]


def destination(child: Token) -> str | None:
    """Require the parser's link destination to be a real string."""
    if child.type not in {"link_open", "image"}:
        return None
    attribute = {"link_open": "href", "image": "src"}[child.type]
    href = child.attrGet(attribute)
    if not isinstance(href, str):
        raise ValueError("Markdown link destination must be a string")
    return href


def remember_heading(anchors: set[str], repeats: dict[str, int], text: str) -> None:
    """Number repeated heading slugs like common Markdown renderers."""
    base = slug(text)
    count = repeats.get(base, 0)
    anchors.add(base if count == 0 else f"{base}-{count}")
    repeats[base] = count + 1


def document(text: str) -> Document:
    """Parse headings and links with the pinned CommonMark parser."""
    tokens = MarkdownIt("commonmark").parse(text)
    anchors: set[str] = set()
    repeats: dict[str, int] = {}
    links: list[str] = []
    for index, token in enumerate(tokens):
        if token.type in {"inline"}:
            links.extend(child_links(token))
        if token.type in {"heading_open"}:
            remember_heading(anchors, repeats, tokens[index + 1].content)
    return Document(anchors, links)


def local_target(root: Path, source: Path, path: str, files: set[Path]) -> Path:
    """Resolve an authored local path without allowing escapes or missing files."""
    target = source if not path else (source.parent / unquote(path)).resolve()
    if not target.is_relative_to(root) or target not in files:
        raise ValueError(f"local documentation target is missing: {path}")
    return target


def verify_fragment(target: Path, fragment: str, href: str) -> None:
    """A local heading fragment must be present in a Markdown target."""
    if not fragment:
        return
    if target.suffix.lower() not in {".md"}:
        raise ValueError("heading fragment requires Markdown target")
    if unquote(fragment) not in document(target.read_text(encoding="utf-8")).anchors:
        raise ValueError(f"local documentation heading is missing: {href}")


def checked_target(root: Path, source: Path, href: str, files: set[Path]) -> str | None:
    """External URLs are recorded; local paths and heading fragments must exist."""
    url = urlsplit(href)
    if url.scheme in {"https", "http", "mailto"}:
        return href
    if url.scheme or url.netloc or url.query:
        raise ValueError(f"unsupported documentation link: {href}")
    target = local_target(root, source, url.path, files)
    verify_fragment(target, url.fragment, href)
    return None


def markdown_inputs(_root: Path, files: set[Path]) -> dict[Path, bytes]:
    """Freeze a nonempty independently discovered authored Markdown inventory."""
    markdown = sorted(path for path in files if path.suffix.lower() in {".md"})
    if not markdown:
        raise ValueError("authored Markdown inventory is empty")
    return {path: path.read_bytes() for path in markdown}


def external_links(root: Path, files: set[Path], inputs: dict[Path, bytes]) -> list[str]:
    """Validate each parsed link and keep external URLs out of local failures."""
    pages = {path: document(data.decode("utf-8")) for path, data in inputs.items()}
    return sorted(
        {
            url
            for source, page in pages.items()
            for link in page.links
            if (url := checked_target(root, source, link, files)) is not None
        }
    )


def write_receipt(root: Path, output: Path, inputs: dict[Path, bytes], external: list[str]) -> None:
    """Record verified document hashes and external references."""
    output.parent.mkdir(exist_ok=True)
    files = {
        path.relative_to(root).as_posix(): hashlib.sha256(data).hexdigest()
        for path, data in inputs.items()
    }
    output.write_text(json.dumps({"files": files, "external": external}, sort_keys=True) + "\n")


def verify_document_links(root: Path) -> None:
    """Check all authored Markdown and write a hash-bound external URL receipt."""
    root = root.resolve()
    output = root / ".quality-results/document-links.json"
    if output.is_symlink() or output.parent.is_symlink():
        raise ValueError("documentation report may not be a symlink")
    output.unlink(missing_ok=True)
    files = set(authored(root, root))
    inputs = markdown_inputs(root, files)
    external = external_links(root, files, inputs)
    if any(path.read_bytes() != data for path, data in inputs.items()):
        raise ValueError("documentation inputs changed while being checked")
    write_receipt(root, output, inputs, external)
