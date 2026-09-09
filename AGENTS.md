# Repository Agent Guidance

Read the [constitution](.specify/memory/constitution.md), the [slice index](specs/README.md),
and the active slice's spec, plan, tasks, contracts, and owned
[gap-register entries](specs/audit/gap-register.md) before changing the repository.
The constitution governs; specs own what/why; plans own implementation. This file
is an entry point to those authorities, not a replacement for them.

## Scope and safe effects

- This is read-only research and monitoring, not trading execution or advice.
  Detection signals are evidence for investigation, not proof of wrongdoing.
- Preserve public APIs, CLI/configuration, schemas, thresholds, and behavior unless
  an approved specification explicitly authorizes a change and migration path.
- Tests and verification must not accidentally contact market, chain, or notification
  services. Real Discord/Telegram delivery requires explicit authorization. Dry runs
  must neither deliver nor poison later real-delivery deduplication state.
  The existing dedup-ordering gap is tracked as G-018 in slice 003; do not claim it
  is fixed or use dry-run alone as evidence that shared delivery state is isolated.
- Preserve durable research assessments. Persistence failures must be observable
  without blocking an otherwise authorized alert attempt.
- Never log secrets or credential-bearing URLs. Exercise migration downgrades only
  in disposable databases, never in the configured application database.

## Spec Kit and change ownership

Follow the applicable sequence: specify → clarify → plan → reviewer-owned domain
requirements-quality checklists → tasks → analyze → human validation → implement
→ converge. Record existing approval honestly; new scope needs its own validation.

Explicitly activate the intended feature before every per-slice command. For
slice 002, from the repository root (choose the actual slice, not this example blindly):

```bash
SPECIFY_FEATURE_DIRECTORY=specs/002-reproducible-runtime .specify/scripts/bash/check-prerequisites.sh --json --require-tasks --include-tasks
```

Do not let ignored local pointer state silently choose a slice. The built-in
`checklists/requirements.md` is author-owned; domain requirements-quality checklists
are human-reviewer-owned. Agents must not mark human criteria satisfied.

Update owned tasks and gaps with actual evidence. Append corrections and verification
receipts rather than rewriting historical results to imply earlier success. Keep
unrelated concerns in separate PRs; each requested quality gate and its underlying
remediation belong together. Preserve user changes and use one writer per worktree.

## Navigation and maintainable code

Use CodeGraph before broad source searches when available: check status, initialize
or sync as needed, explore relevant symbols/callers/tests, and inspect affected code
after changes. Keep `.codegraph/` in local Git `info/exclude`, not tracked state.
If unavailable, state that briefly and use `rg`.

Prefer cohesive small functions, explicit typed boundaries, early returns, and
named domain operations. Use tables or polymorphism when they clarify real variants;
do not trade readable branches for indirection just to lower an analyzer score.
Preserve evaluation order, transactions, retries, cancellation, and error behavior.
Start behavior changes with failing regression evidence when practical; otherwise
record why and the equivalent reproducible check in the plan.

Use established libraries before rebuilding protocols. Do not reintroduce copied
tool-specific skill trees such as `.claude/skills`. Preserve the managed
`.agents/skills/speckit-*` integration; shared project guidance belongs here and in
owned specs, not duplicated global instruction payloads.

## Test doubles and behavioral coverage

Prefer real value objects and lightweight working fakes. Reusable fakes need shared
contract tests against real implementations for exercised behavior. Keep real local
database and transport tests where they provide stronger evidence.

`unittest.mock` and generic mock frameworks are prohibited. The AST policy scans
tests and root `conftest.py`, including aliases and literal dynamic imports.
Follow the [test-quality contract](specs/002-reproducible-runtime/contracts/test-quality.md)
for shared Redis contracts and narrow lifecycle/failure-injection exceptions.

Do not replace mocks with a homemade mock framework: no dynamic attribute trees,
generic return-value/side-effect DSL, or call-assertion framework. Assert resulting
state, payloads, returned results, and failures. Delivery/retry counts are appropriate
when the interaction itself is the external contract. `pytest.monkeypatch` remains
appropriate for environment isolation and narrow boundary/failure injection. A
functional HTTP transport remains a fake despite an upstream “Mock” name.

Preserve applicable ingest → enrich → detect/score → persist → deliver/suppress
coverage, including failure, retry, deduplication, restart, and degraded dependencies.
Test counts and line coverage alone are not completion evidence. Redis fakes do not
replace real Redis parity, and SQLite does not establish PostgreSQL driver/migration
correctness. These are acceptance requirements, not claims that every existing test
already meets them; consult the active slice's tasks and evidence.

## Required verification

Use the checked-in lock and supported Python 3.11–3.13. Black is the formatter
(100 columns); Ruff owns lint/import rules. The independent strict type gates are
mypy (`src`, `scripts`) and Pyright (`src/polymarket_insider_tracker`). Preserve
their configured scope and strictness; do not substitute another checker silently.

```bash
uv sync --locked --all-extras --python 3.13
uv run python scripts/verify.py --profile static
uv run python scripts/verify.py --profile compatibility
uv run --env-file .env python scripts/verify.py --profile services
```

Follow the [quickstart](specs/002-reproducible-runtime/quickstart.md) for local
service setup and clean-checkout verification, including isolated compatibility
runs for each supported minor. `uv run python scripts/verify.py --help` lists the
actual underlying gate commands. The [runtime contract](specs/002-reproducible-runtime/contracts/runtime-verification.md)
owns exact membership, exits, and service/migration safety.

Vulture runs at default confidence over `src tests scripts alembic conftest.py`.
Complexipy checks functions **and module-level control flow** over that scope with
maximum cognitive complexity **5**, using the fail-closed launcher:

```bash
uv run --isolated --locked --all-extras --python 3.11 python scripts/complexipy_gate.py src tests scripts alembic conftest.py --max-complexity-allowed 5 --no-ignore --ignore-complexity=false --snapshot-ignore=true --snapshot-create=false --exclude=. --check-script=true
```

Fix underlying issues. Do not introduce baselines, grandfathering, allowlists,
exclusions, suppressions, reduced confidence, raised thresholds, non-blocking status,
or weaker types to make checks green. Do not replace full-tree enforcement with
diff/staged checks or use snapshots, report-only success, or cwd configuration to
evade the launcher. Low scores do not by themselves prove low cognitive load.

## Review and delivery

When the requested multi-tool workflow is used, the sequence is Agy first pass →
Claude Code Fable correction → independent Codex adversarial review. This is not a
mandate to run three agents for every change. Preserve immutable first-pass and
corrective commits. Record unavailable tools/models honestly. Inspect
the actual diff and independently rerun verification before accepting a worker's
report; a launch, idle signal, test count, or green summary is not proof.

Prepare a PR only after required checks and convergence pass. Verify remote required
checks at the exact head; do not bypass branch protection. Patrick's approval is
required to merge. After an approved merge, verify the resulting commit/tree and
main CI before reporting completion. Distinguish implemented, locally verified,
PR-green, approved, merged, and main-verified states in handoffs.
