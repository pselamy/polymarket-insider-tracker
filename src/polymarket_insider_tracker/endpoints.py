"""Single source of truth for the official Polymarket public trades endpoints.

``OFFICIAL_V1_TRADES_URL`` is the documented anonymous public trades query (the
configuration default) and ``OFFICIAL_V2_TRADES_URL`` is its successor. They are
declared here once so the configuration default, the live smoke check, and the
logical source namespace all derive from the same literals. This module has no
intra-package imports so it is safe to import from ``config`` at module load.
"""

from __future__ import annotations

OFFICIAL_V1_TRADES_URL = "https://data-api.polymarket.com/trades"
OFFICIAL_V2_TRADES_URL = "https://data-api.polymarket.com/v2/trades"

__all__ = ["OFFICIAL_V1_TRADES_URL", "OFFICIAL_V2_TRADES_URL"]
