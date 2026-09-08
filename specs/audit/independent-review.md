# Independent Review and Adjudication

**Date**: 2026-09-06
**Reviewers**: Agy and Claude Code, independently invoked on `dev@selamy-core`
**Mode**: read-only review of copied artifacts; no repository edits or external actions

## Shared Recommendation

Both reviewers recommended approval with changes, endorsed the direction of A1–A4, agreed that the human
pause is correct, and recommended executing the reproducible runtime/CI foundation before ingestion.

## Accepted Findings

- Execute `002-reproducible-runtime` before `001-supported-trade-ingestion`, then `003` and `004`.
- Explicitly set `SPECIFY_FEATURE_DIRECTORY` before every per-slice command; do not rely on ignored local
  `.specify/feature.json` state on a branch with several feature directories.
- Distinguish the built-in author-owned `checklists/requirements.md` from reviewer-owned custom domain
  checklists, and never let an agent self-approve the latter.
- Add source page-saturation/loss detection, all-participant versus taker-only coverage, and a bounded
  feasibility record before implementing the polling design.
- Define delivery identity and ambiguous network outcomes, not only clean success and failure.
- Give slice 003 sole ownership of the target assessment/delivery schema and reusable end-to-end harness.
- Require a fixed old-versus-proposed score/qualification comparison before slice 004 implementation.
- Replace subjective “audit finds zero” outcomes with deterministic checks over enumerated surfaces.
- Record indexer-backed wallet age and Python 3.14+ as explicit deferrals.

## Rejected or Narrowed Findings

- Agy initially said the built-in `checklists/requirements.md` files should be renamed. The generated
  v1.0.4 `speckit-checklist` skill explicitly says the opposite. Agy re-reviewed the exact text, retracted
  the finding, and confirmed the paths conform.
- Claude stated that the Data API rate limit was unpublished. Current official Polymarket documentation
  publishes `/trades` at 200 requests per 10 seconds, so the spec now grounds its conservative request
  budget in that source.
- Claude recommended one branch/PR per feature and demoting three drafts to roadmap notes. Spec Kit v1.0.4
  permits independent feature directories on one branch, and Patrick explicitly requested a first
  specification set, bounded implementation commits, and a prepared pull request. The drafts remain
  proposed and execute one at a time with explicit activation.

## New Verification Triggered by Review

A fresh read-only aggregate probe requested a five-second public-trade window at the maximum page size.
Both taker-only and all-participant responses returned 10,000 rows. Only 83 taker-only and 259
all-participant rows were timestamped inside the requested window; some rows fell outside documented
`start`/`end` bounds. All-participant results contained multiple wallet observations per transaction.
No wallet identifiers were retained.

This directly validated the saturation/coverage concern. Gap entries G-031 and G-032 and the strengthened
slice 001 requirements now make unresolved data-loss risk a stop condition rather than a hidden assumption.

## Process Result

- Generated Spec Kit v1.0.4 scaffolding remains untouched.
- The built-in specification-quality checklist paths remain canonical.
- `specs/README.md` now records explicit feature activation, checklist ownership, command order, and
  no-concurrent-feature-writing discipline.
- A patch-level constitution clarification is proposed separately and remains unapplied pending Patrick.
- No application or delivery implementation is authorized by this review.
