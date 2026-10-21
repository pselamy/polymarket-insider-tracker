# Evidence tools

Four Python tools recompute or check the committed evidence. They are ordinary tracked
files in [`scripts/spec004_evidence/`](../../../scripts/spec004_evidence/) so the repository's Python
gates apply to them. Since T028 the two generators score every case through the product code
([`real_scoring.py`](../../../scripts/spec004_evidence/real_scoring.py): the real size and
fresh-wallet detectors and the scorer's `size_group_score`, the function the runtime alert-path report
uses); no scoring rule is copied. The post-T027 scenario, which main does not wire, is composed from
the scorer's own weights, quantization and bonus.

| Tool | Checks | Run from the repository root |
|---|---|---|
| [`reachability.py`](../../../scripts/spec004_evidence/reachability.py) | threshold reachability table ([reachability.md](reachability.md)), its equality with the runtime alert-path ceiling, and the pinned scoring literals | `uv run python -m scripts.spec004_evidence.reachability --check specs/004-truthful-detection-contract/evidence/reachability.md` |
| [`replay_live.py`](../../../scripts/spec004_evidence/replay_live.py) | live-realistic replay ([replay-live/results.csv](replay-live/results.csv)) | `uv run python -m scripts.spec004_evidence.replay_live --check specs/004-truthful-detection-contract/evidence/replay-live/results.csv` |
| [`check_envelope.py`](../../../scripts/spec004_evidence/check_envelope.py) | 004.1 schema and the derived 004.2 schema (contracts/scoring-config.md §2a), the pre-T027 (004.1) and post-T027 (004.2) examples, failure row ([contracts](../contracts/)), the cross-field rules the schema cannot express, and one envelope per replay-live case in each wallet posture (built by [`replay_envelopes.py`](../../../scripts/spec004_evidence/replay_envelopes.py)) | `uv run python -m scripts.spec004_evidence.check_envelope` |
| [`check_source_hashes.py`](../../../scripts/spec004_evidence/check_source_hashes.py) | pinned source hashes ([source-refresh-f2e2167.md](source-refresh-f2e2167.md)) | `uv run python -m scripts.spec004_evidence.check_source_hashes` |

Every tool exits 0 when its committed artifact is current. The two generators share
[`cli.py`](../../../scripts/spec004_evidence/cli.py), which fails closed: `--check FILE` exits 1 when FILE is
stale or missing, and any other argument list (for example `--check` without a path) exits 2.
