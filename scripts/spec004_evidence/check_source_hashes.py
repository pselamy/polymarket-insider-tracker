"""Compare the current-base hashes in evidence/source-hashes.json with a checked-out tree.

Usage: python3 -m scripts.spec004_evidence.check_source_hashes [REPO_ROOT]   (default: the repository containing this file)

Stdlib only; it never imports from src/. Exit 0 when every recorded hash matches, 1 otherwise.
replay/summary.json stays the immutable receipt of the replay's execution checkout and is not
read here; source-refresh-f2e2167.md records how the receipt differs from the current base.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

HERE = (
    Path(__file__).resolve().parents[2] / "specs" / "004-truthful-detection-contract" / "evidence"
)
SOURCE_PREFIX = Path("src") / "polymarket_insider_tracker"


def main(argv: list[str]) -> int:
    root = Path(argv[1]).resolve() if len(argv) > 1 else Path(__file__).resolve().parents[2]
    recorded = json.loads((HERE / "source-hashes.json").read_text())["source_sha256"]
    drifted = 0
    for relative, expected in sorted(recorded.items()):
        actual = hashlib.sha256((root / SOURCE_PREFIX / relative).read_bytes()).hexdigest()
        status = "match" if actual == expected else "DRIFT"
        drifted += actual != expected
        print(f"{status:5} {relative} {actual}")
    return 1 if drifted else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
