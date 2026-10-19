"""Entrypoint for the protected GFM documentation style gate."""

from pathlib import Path

from quality.document_style import verify_document_style


def main() -> None:
    verify_document_style(Path.cwd())


if __name__ in {"__main__"}:
    main()
