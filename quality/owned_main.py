"""Run the Linux supervisor without installing its kernel state in the caller."""

import sys
from pathlib import Path

from quality.owned_entry import supervise

supervise(Path(sys.argv[1]))
