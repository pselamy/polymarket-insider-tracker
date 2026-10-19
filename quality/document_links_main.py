"""Entrypoint for the protected local documentation link gate."""

from pathlib import Path

from quality.document_links import verify_document_links


def main() -> None:
    verify_document_links(Path.cwd())


if __name__ in {"__main__"}:
    main()
