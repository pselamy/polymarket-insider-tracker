# Obsolete Skill Draft Pruning

## Authorization and scope

Patrick requested pruning the stray `docs/` directory after approving PR117's merge
on 2026-09-09. Base: `357c350af2f2b4080fbd90cbddfc01900452c5fe`.
This is a separate documentation-only hygiene change, not implementation of slices
001 or 004 and not part of the root AGENTS.md PR.

## Decision and plan

The directory contains one tracked file, `docs/skill-tracking-prediction-market-flow.md`.
Remove that obsolete skill draft instead of maintaining a second operational guide.
Update only its G-029a/b dispositions and slice-map annotations. Preserve other
runtime gaps, the managed Spec Kit integration, and the upstream template's generic
`docs/` example. No source, test, lock, workflow, or configuration changes are intended.

The draft was introduced in [PR105](https://github.com/pselamy/polymarket-insider-tracker/pull/105)
and is unchanged since then. Its [immutable historical copy](https://github.com/pselamy/polymarket-insider-tracker/blob/b962bdaee2aa2917399ffe49beed706f665eb2c7/docs/skill-tracking-prediction-market-flow.md)
preserves the original content and WebSocket incident context; Git retains recovery.

No content needs migration. Setup and CLI guidance already live in the root README
and the [runtime quickstart](../002-reproducible-runtime/quickstart.md). Research-only
safeguards live in the [constitution](../../.specify/memory/constitution.md). The
draft's obsolete ingestion claims, threshold of 0.6, and operational sniper/funding
claims are precisely the misinformation recorded in G-029a/b, not missing decisions.
Their removal does not fix the ingestion, scoring, or delivery implementation gaps.

## Acceptance and verification

- Remove only the identified tracked draft; no unrelated or untracked data deletion.
- Leave no active link to its removed path; historical links must pin a commit.
- Retain the other slice001/004 gaps and their ownership.
- Verify that the diff is documentation-only and all existing verifier gates still pass.
- Obtain independent review and green required checks before seeking merge approval.

An independent read-only reviewer checked the draft, its history, its consumers,
and the canonical replacements before deletion. It recommended removal without
migration; the only active path references were the two gap-register rows.
Implementation and verification results will be appended after they occur.

## Implementation and local verification

Removed the single 113-line draft and its empty directory. Replaced both active
gap-register path references with this decision record and annotated the two
slice-map entries as resolved by removal, merge pending. The only remaining
mentions of the old path are historical prose and the commit-pinned link above.
No unique content was relocated and no non-documentation file changed.

The root reviewer ran all 12 verifier gates on Python 3.13, including real local
PostgreSQL 15 / Redis 7 and the disposable migration cycle: all passed. Isolated
Python 3.11 and 3.12 compatibility profiles also passed. Receipts remain in the
coordination workspace as `outputs/docs-pruning-all.json`,
`outputs/docs-pruning-311.json`, and `outputs/docs-pruning-312.json`.

Independent review of the actual deletion, gap diff and this record returned
SHIP. It verified the historical blob exists, the relative quickstart/constitution
links resolve, no active link dangles, runtime gap ownership remains unchanged,
and `git diff --check` passes. PR publication, exact-head CI and Patrick's merge
approval are separate pending events.
