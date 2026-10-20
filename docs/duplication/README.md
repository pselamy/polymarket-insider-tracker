# Duplication inventory

Clone duplication is tracked with the pinned jscpd 5.3.3 at the 50-token tier, one file per
cluster of duplicated *knowledge*. This directory replaces the former single
the single-file duplication inventory, which every dedup PR edited and which caused most of the rebase
conflicts on 2026-10-19.

## Layout

- `clusters/NN-<slug>.md` — one cluster: its knowledge, verdict (`real`, `incidental` or
  `resolved`), resolving PR and notes. A dedup PR edits only its own cluster files.
- `history.md` — frozen measurement receipts moved from the old inventory. Not edited again.
- No file here holds totals or a per-clone list. They are generated from the tree (below).

## Cluster file header

Each cluster file opens with a flat front-matter block:

| key | meaning |
| --- | --- |
| `cluster` | the cluster number; the file name starts with it, zero padded |
| `status` | `real`, `incidental` or `resolved` |
| `resolved-by` | optional; the resolving PR |
| `files` | required for `real` and `incidental`; comma-separated paths. A clone belongs to the cluster when both sides sit in these files |
| `watch` | optional for `resolved`; comma-separated `A <-> B` file pairs that must report no clone |

When a card removes a cluster's last clone it sets `status: resolved`, moves the file list to
`watch` if the pair should stay gone, and records the receipt in the same file.

## Totals and the per-clone list

Totals, per-cluster counts and every clone are generated, never hand-edited:

```bash
uv run --locked --group dev python -m quality.duplication_clusters_main
```

Add `--check` to fail when the cluster files disagree with the scan: a `real` or `incidental`
cluster with no remaining clone (mark it resolved), or a `resolved` cluster whose `watch` pair
still has a clone. CI runs the check in the "Relentless CI gate" job. Clones that no cluster file
owns are reported as `unassigned`; they do not fail the check.

## Policy and how to measure

Reproduce: enumerate the authored scope, stage it, then call the vendored
`quality.duplication.scan(root, config, staged, output, minimum=50, deadline)`
(equivalent raw command, run from the repo root with the pinned tool set:
`mise --yes --locked exec -- jscpd --config <empty-config.json> --mode mild
--format python --min-tokens 50 --min-lines 4 --threshold 0 --max-size
9007199254740991 --no-gitignore --absolute --reporters json --summary
--summary-top <n> --workers 1 --no-colors --no-tips --output <dir> <files...>`).
jscpd exits 1 under the threshold-0 policy whenever any clone exists; the
report it writes is the measurement artifact.

Every clone reported by the scanner is grouped into
clusters by the duplicated *knowledge* (same rule, mapping, query,
validation, or fixture — not merely similar text).
A cluster is annotated **real** (the same knowledge is genuinely maintained
in two places and a divergence would be a defect risk) or **incidental**
(structurally similar scaffolding required by convention, not shared
knowledge).

The authored scope is `quality.source_scope.discover` plus `validate_path` over `.py` files (the
`verify_sources` set without its 399-line size hook), scanned by `quality.duplication.scan` with
the vendored `SCAN_POLICY` at the 50-token tier: `--mode mild --format python --min-tokens 50
--min-lines 4 --threshold 0 --no-gitignore --workers 1`, an empty config, no ignore lists and no
exclusions. Do not add baselines, exclusions or a lower token tier to make a number smaller.
