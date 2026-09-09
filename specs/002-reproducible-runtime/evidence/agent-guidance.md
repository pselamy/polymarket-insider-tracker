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

## Not yet established

The guidance branch has not been reconciled with the final fakes follow-up, published
as a PR, approved, or merged. No new service/migration run or full Linux matrix is
claimed by this local documentation checkpoint. Existing runtime/service contracts
are unchanged. Human-owned requirements-quality checklist items remain unchecked.
