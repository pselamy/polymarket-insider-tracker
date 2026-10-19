"""Dependency-free constants shared across the storage and detector layers.

Every subpackage is expected to be able to import this module without pulling
in any other part of the package, so it must never gain imports.
"""

UNRECORDED_DELIVERY_DISPOSITION = "unrecorded"

__all__ = ["UNRECORDED_DELIVERY_DISPOSITION"]
