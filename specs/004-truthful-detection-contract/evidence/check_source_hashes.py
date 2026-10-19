"""Compare the source hashes recorded in replay/summary.json with a checked-out tree.

Usage: python3 check_source_hashes.py [REPO_ROOT]   (default: the repository containing this file)

Stdlib only; it never imports from src/. Exit 0 when every recorded hash matches, 1 otherwise.
The recorded hashes are the receipt of the replay's execution checkout, so a mismatch means
"changed since the replay", not "wrong"; read source-refresh-1bdccbc.md for the disposition.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE_PREFIX = Path("src") / "polymarket_insider_tracker"


def main(argv: list[str]) -> int:
    root = Path(argv[1]).resolve() if len(argv) > 1 else HERE.parents[2]
    recorded = json.loads((HERE / "replay" / "summary.json").read_text())["source_sha256"]
    drifted = 0
    for relative, expected in sorted(recorded.items()):
        actual = hashlib.sha256((root / SOURCE_PREFIX / relative).read_bytes()).hexdigest()
        status = "match" if actual == expected else "DRIFT"
        drifted += actual != expected
        print(f"{status:5} {relative} {actual}")
    return 1 if drifted else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
