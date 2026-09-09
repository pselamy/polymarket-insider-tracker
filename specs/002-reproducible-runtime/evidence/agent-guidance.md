# Root Agent Guidance Evidence

## Local preparation — 2026-09-09 UTC

Base: `7a4f11cd645dd21b049db3649747bb4e03b652e2` (merged PR116).
Branch: `docs/root-agent-guidance`. Scope: root guidance and its owned documentation;
no application, test, script, dependency, workflow, or constitution modification.

Codex read the constitution, runtime spec/plan/contract, actual `pyproject.toml`,
CI workflow, and verifier commands before authoring. Specification/plan additions
preceded the root file. The root guidance explicitly distinguishes desired test
policy from the pending separate mocks-to-fakes migration.

Executed on Apple Silicon macOS, Python 3.13.14:

- `uv sync --locked --all-extras --python 3.13`: passed, 102 resolved packages.
- `uv run python scripts/verify.py --profile static`: all seven gates passed,
  including Black, Ruff, isolated Python 3.11 mypy/Pyright/Vulture/Complexipy.
  mypy checked 42 source files; Pyright reported zero errors/warnings; full-scope
  Complexipy accepted every function and module at the unchanged maximum of 5.
- `uv run python scripts/verify.py --profile compatibility`: passed lock/imports
  and the complete suite, **860 passed, 2 existing skips, 16 warnings**, in 14.80s
  pytest time. The legacy unawaited AsyncMock warning remains a separate fakes
  concern; no warning-free claim is made.
- Explicit `SPECIFY_FEATURE_DIRECTORY=specs/002-reproducible-runtime` with the
  real `check-prerequisites.sh --json --require-tasks --include-tasks`: passed,
  returning the intended feature directory and tasks.
- Seven local Markdown links in the root file and guidance contract resolved to
  existing files. `scripts/verify.py --help` matched the documented underlying
  gate commands. `git diff --check` passed.

An independent read-only Codex documentation reviewer checked policy completeness,
source consistency, links, commands, ownership, and completion claims. It returned
REVISE for two omissions: the conditional Agy → Fable → Codex sequence and the ban
on introducing new unittest.mock doubles. Both were corrected while preserving
valid monkeypatch/functional-transport use and separate-PR scope. The reviewer
re-read the corrections and evidence, then returned SHIP for locally prepared
documentation with no further actionable findings. It explicitly did not claim
PR readiness or independently rerun the parent's recorded tests.

## Not yet established at the preparation checkpoint

The guidance branch has not been reconciled with the final fakes follow-up, published
as a PR, approved, or merged. No new service/migration run or full Linux matrix is
claimed by this local documentation checkpoint. Existing runtime/service contracts
are unchanged. Human-owned requirements-quality checklist items remain unchecked.

## Reconciliation and independent verification after PR117

Patrick approved merging PR117 and continuing on 2026-09-09. Root rechecked its
exact head `408177b55c2f2eb97f1cb167d80bef867fc94081` and all push/PR checks, then
squash-merged with a head-match guard as `357c350af2f2b4080fbd90cbddfc01900452c5fe`
at `2026-09-09T03:52:00Z`. Both commits have tree
`f91dc26d9c289b3b200b097e59154458cb635aec`. Root directly verified all nine jobs in
[main CI](https://github.com/pselamy/polymarket-insider-tracker/actions/runs/34308759971)
passed at the merge SHA. T097/T099 record the actual publication and merge events.

The original guidance commit `1909680945c10e77df628422d3bfa386ec796b2d` was preserved.
Main was merged into the guidance branch; the single task-ledger conflict was resolved
by retaining both independent ledgers. Relative to merged main, this branch changes
only AGENTS.md and seven owned documentation files. Guidance now links to the enforced
test-quality contract and explicitly identifies the still-unresolved G-018 dry-run
dedup gap. It does not claim application-wide convergence.

Independent Apple Silicon verification after reconciliation:

- Locked Python 3.13 sync passed, resolving 104 packages.
- All 12 gates of the `all` profile passed on Python 3.13, including real local
  PostgreSQL 15 / Redis 7 and the disposable migration lifecycle.
- Isolated Python 3.11 and 3.12 full compatibility profiles both passed.
- Explicit slice002 prerequisite activation passed, returning the intended directory.
- All nine relative links in AGENTS.md and its contract resolved; diff check passed.

Raw machine-readable receipts are retained in the coordination workspace as
`outputs/agents-guidance-all.json`, `outputs/agents-guidance-311.json`, and
`outputs/agents-guidance-312.json`.

An independent read-only reviewer compared the reconciled diff against main and
returned SHIP: no actionable findings in policy consistency, links, commands, narrow
test exceptions, task reconciliation, or human-approval ownership. Reviewer-owned
criteria remain unchecked. PR publication, exact-head CI and merge approval are
separate pending events. The requested stale docs-directory pruning is a separate PR.
