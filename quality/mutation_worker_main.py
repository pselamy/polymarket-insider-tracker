"""Native worker entry point uses its own copied checkout as the working root."""

import sys
from pathlib import Path

from quality.mutation_worker import worker

worker(Path.cwd(), sys.argv[1])
