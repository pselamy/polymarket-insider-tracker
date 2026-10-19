"""One home for loading ``scripts/*.py`` entry points by file path under an explicit name."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

def load_module(name: str, path: Path) -> ModuleType:
    """Execute ``path`` as module ``name`` without registering it in ``sys.modules``."""
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
