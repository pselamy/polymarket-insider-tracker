"""Thin native Cosmic CLI adapter; scope and trial configuration remain native."""

import sys

from quality.mutation_coordinator import execute

execute(sys.argv[1:])
