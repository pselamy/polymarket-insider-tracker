"""Entrypoint for the protected native Markdown spelling gate."""

from pathlib import Path

from quality.document_spelling import verify_document_spelling


def main() -> None:
    verify_document_spelling(Path.cwd())


if __name__ in {"__main__"}:
    main()
