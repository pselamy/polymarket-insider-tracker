"""Registered, fresh component and license inventory receipt."""

import json
from pathlib import Path

from quality.component_inventory import inventory

root = Path.cwd()
report = root / ".quality-results/component-inventory.json"
report.unlink(missing_ok=True)
result = inventory(root)
report.parent.mkdir(exist_ok=True)
report.write_text(json.dumps(result, indent=2) + "\n")
print("reviewed locked components:", result["componentCount"])
