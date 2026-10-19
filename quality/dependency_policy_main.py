"""Registered local and hosted dependency-origin gate."""

from pathlib import Path

from quality.dependency_policy import verify

print("approved dependency sources:", verify(Path.cwd()))
