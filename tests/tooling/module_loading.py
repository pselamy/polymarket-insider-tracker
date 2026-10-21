"""One home for loading ``scripts/*.py`` entry points by file path under an explicit name."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

# Program text for an isolated interpreter (run with ``-c``). It defines ``load(name, path)``
# which registers the module in ``sys.modules`` before executing it, exactly as an import would.
LOAD_PROGRAM = """
import importlib.util
from pathlib import Path
import sys

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
"""

# Program prologue for an isolated interpreter: ``argv[1]`` is the repository root, and the
# named ``scripts`` module is imported as ``from scripts import <module>``.
_IMPORT_SCRIPTS = (
    "import pickle\nfrom pathlib import Path\nimport sys\n"
    "sys.path.insert(0, sys.argv[1])\nfrom scripts import "
)


def load_module(name: str, path: Path, *, register: bool = False) -> ModuleType:
    """Execute ``path`` as module ``name``, registering it in ``sys.modules`` only if asked.

    Registration is what dataclasses need to resolve postponed annotations.
    """
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    if register:
        sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def scripts_program(module: str, body: str) -> str:
    """Prefix ``body`` with the imports that expose ``scripts.<module>`` as ``<module>``."""
    return _IMPORT_SCRIPTS + module + "\n" + body
