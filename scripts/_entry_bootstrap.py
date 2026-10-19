"""Resolve a script's sibling modules against the script's own directory.

Entry points such as ``verify.py`` and ``runtime_services.py`` import private
sibling modules. That must work when the file is run directly, imported with
``python -m scripts.x``, loaded by file location under an arbitrary module name,
or copied standalone into another directory. A unique synthetic package whose
``__path__`` is the script directory gives each load its own siblings, so a
cached foreign ``scripts`` package cannot redirect policy.

The caller opens the context around its relative imports:

    _bootstrap = runpy.run_path(str(Path(__file__).with_name("_entry_bootstrap.py")))
    with _bootstrap["sibling_imports"](globals(), "_polymarket_verifier"):
        from ._verify_models import Gate

This module is loaded by file path, not imported, because the package machinery
it provides is not available until it has run.
"""

from __future__ import annotations

import sys
from collections.abc import Generator
from contextlib import contextmanager
from importlib.machinery import ModuleSpec
from importlib.util import module_from_spec
from pathlib import Path
from typing import Any
from uuid import uuid4

__all__ = ["sibling_imports"]


@contextmanager
def sibling_imports(namespace: dict[str, Any], prefix: str) -> Generator[None, None, None]:
    """Point ``namespace``'s relative imports at a fresh package for this entry point."""
    helper_name = f"{prefix}_{uuid4().hex}"
    helper_package = module_from_spec(ModuleSpec(helper_name, loader=None, is_package=True))
    helper_package.__path__.append(str(Path(namespace["__file__"]).resolve().parent))
    sys.modules[helper_name] = helper_package
    package_before = namespace["__package__"]
    spec_before = namespace["__spec__"]
    namespace["__package__"] = helper_name
    namespace["__spec__"] = ModuleSpec(f"{helper_name}.entry", loader=None)
    try:
        yield
    finally:
        namespace["__package__"] = package_before
        namespace["__spec__"] = spec_before
