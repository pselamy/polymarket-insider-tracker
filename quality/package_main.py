"""Verify fresh distribution archives and their isolated installed consumers."""

from pathlib import Path

from quality.package_build import verify_packages

verify_packages(Path.cwd())
