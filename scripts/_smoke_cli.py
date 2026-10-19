"""Command-line surface for the live-safe trades smoke check."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Callable, Sequence
from typing import NoReturn

import httpx

from polymarket_insider_tracker.endpoints import OFFICIAL_V1_TRADES_URL
from scripts._smoke_records import SmokeRecord
from scripts._smoke_runner import COVERAGE_MODES as COVERAGE_MODES
from scripts._smoke_runner import run_smoke as run_smoke

DEFAULT_TRADES_URL = OFFICIAL_V1_TRADES_URL
EXIT_PASSED = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
ClientFactory = Callable[[], httpx.AsyncClient]


def exit_code_for(records: Sequence[SmokeRecord]) -> int:
    """``0`` only when every record passed its verdict and the endpoint was reachable."""
    if all(record.passed and record.reachable for record in records):
        return EXIT_PASSED
    return EXIT_FAILED


def resolve_trades_url() -> str:
    """The configured trades URL, or the documented default; never the legacy setting."""
    return os.environ.get("POLYMARKET_TRADES_URL", DEFAULT_TRADES_URL)


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise argparse.ArgumentError(None, message)


def _window(value: str) -> int:
    seconds = int(value)
    if not 1 <= seconds <= 60:
        raise argparse.ArgumentTypeError("--window-seconds must be between 1 and 60")
    return seconds


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="trades_smoke.py", description=__doc__, allow_abbrev=False, exit_on_error=False
    )
    parser.add_argument("--live", action="store_true", help="perform the bounded live request")
    parser.add_argument("--window-seconds", type=_window, default=5)
    parser.add_argument("--coverage", choices=(*COVERAGE_MODES, "both"), default="both")
    parser.add_argument("--json", action="store_true", help="one JSON object per record")
    return parser


def _render_line(record: SmokeRecord) -> str:
    verdict = "passed" if record.passed else "failed"
    reach = "reachable" if record.reachable else "unreachable"
    return (
        f"{record.coverage}: {record.case} {verdict} ({reach}); rows={record.row_count} "
        f"valid={record.valid_rows} invalid={record.invalid_rows} "
        f"lag={record.provider_lag_seconds} saturated={record.page_saturated}"
    )


def _render(records: Sequence[SmokeRecord], as_json: bool) -> None:
    for record in records:
        print(record.to_json() if as_json else _render_line(record))


def _parse(argv: Sequence[str] | None) -> argparse.Namespace | None:
    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except (argparse.ArgumentError, argparse.ArgumentTypeError) as exc:
        print(f"{parser.prog}: error: {exc}", file=sys.stderr)
        return None
    if not args.live:
        print(
            f"{parser.prog}: live runs are explicit; pass --live to perform one bounded "
            "anonymous request per coverage mode",
            file=sys.stderr,
        )
        return None
    return args


def main(argv: Sequence[str] | None = None, *, client_factory: ClientFactory | None = None) -> int:
    """Parse options, run the bounded live smoke, print records, and return the exit status."""
    args = _parse(argv)
    if args is None:
        return EXIT_USAGE
    url = resolve_trades_url()
    if not url.startswith(("http://", "https://")):
        print(
            "trades_smoke.py: error: POLYMARKET_TRADES_URL must be an HTTP(S) URL", file=sys.stderr
        )
        return EXIT_USAGE
    modes = list(COVERAGE_MODES) if args.coverage == "both" else [args.coverage]
    build = client_factory or httpx.AsyncClient
    records = asyncio.run(
        run_smoke(build(), url=url, coverage_modes=modes, window_seconds=args.window_seconds)
    )
    _render(records, args.json)
    return exit_code_for(records)
