#!/bin/sh
# Extract the literate Python evidence tools from evidence-tools.md into a private temporary
# directory and run one of them.  Usage: sh run-evidence.sh TOOL [ARGS...]
# TOOL is one of: reachability replay_live check_envelope check_source_hashes
set -eu

tool="${1:?usage: run-evidence.sh TOOL [ARGS...]}"
shift
here="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$here/../../.." && pwd)"
EVIDENCE_DIR="$here"
export REPO_ROOT EVIDENCE_DIR

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

awk -v dir="$work" '
  /^<!-- file: .* -->$/ { name = $3; next }
  /^```python$/ && name != "" { out = dir "/" name; next }
  /^```$/ && out != "" { close(out); out = ""; name = ""; next }
  out != "" { print > out }
' "$here/evidence-tools.md"

[ -f "$work/$tool.py" ] || { echo "unknown tool: $tool" >&2; exit 2; }
python3 "$work/$tool.py" "$@"
