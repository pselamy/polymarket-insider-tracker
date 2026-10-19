# Evidence tools

Four stdlib-only Python tools recompute or check the committed evidence. They are ordinary tracked
files in [`scripts/spec004_evidence/`](../../../scripts/spec004_evidence/) so the repository's Python
gates apply to them; none imports from `src/` (product source is read as text only).

| Tool | Checks | Run from the repository root |
|---|---|---|
| [`reachability.py`](../../../scripts/spec004_evidence/reachability.py) | threshold reachability table ([reachability.md](reachability.md)) and the product constants it copies | `uv run python -m scripts.spec004_evidence.reachability --check specs/004-truthful-detection-contract/evidence/reachability.md` |
| [`replay_live.py`](../../../scripts/spec004_evidence/replay_live.py) | live-realistic replay ([replay-live/results.csv](replay-live/results.csv)) | `uv run python -m scripts.spec004_evidence.replay_live --check specs/004-truthful-detection-contract/evidence/replay-live/results.csv` |
| [`check_envelope.py`](../../../scripts/spec004_evidence/check_envelope.py) | 004.1 schema, example and failure row ([contracts](../contracts/)) | `uv run python -m scripts.spec004_evidence.check_envelope` |
| [`check_source_hashes.py`](../../../scripts/spec004_evidence/check_source_hashes.py) | pinned source hashes ([source-refresh-f2e2167.md](source-refresh-f2e2167.md)) | `uv run python -m scripts.spec004_evidence.check_source_hashes` |

Every tool exits 0 when its committed artifact is current.
