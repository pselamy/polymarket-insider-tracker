"""Run the complete suite twice under different order and process environments."""

import os
import subprocess
import sys
from pathlib import Path

VARIANTS = ((41, "UTC0", "C"), (73, "UTC-14", "C.UTF-8"))


def run_variant(root: Path, variant: tuple[int, str, str]) -> None:
    """Run one seeded full suite; any failure stops the gate immediately."""
    seed, timezone, locale = variant
    env = dict(os.environ)
    env.update(
        PYTHONHASHSEED=str(seed),
        RLCI_TEST_ORDER_SEED=str(seed),
        TZ=timezone,
        LC_ALL=locale,
    )
    subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "quality.test_order", "-q"],
        cwd=root,
        env=env,
        check=True,
        timeout=600,
    )


def run_all(root: Path) -> None:
    """Repeat the entire suite with two fixed, reportable seeds."""
    for variant in VARIANTS:
        print(
            f"stability seed={variant[0]} timezone={variant[1]} locale={variant[2]}",
            flush=True,
        )
        run_variant(root, variant)
