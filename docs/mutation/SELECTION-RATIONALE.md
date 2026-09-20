# SELECTION-RATIONALE — R02 revised OSS mutation-runner candidate (corrected)

Target host: `pselamy/polymarket-insider-tracker` @ `c3b1c3dd`
(BP receipt: the ENFORCED repo with required checks `["Required checks",
"Complexipy complexity check"]`, strict, enforce_admins).

## Adopted tool: mutmut 3.8.0

- Official source: PyPI `mutmut` `3.8.0`, `requires_python >=3.10`
  (fetched 2026-09-20 via `https://pypi.org/pypi/mutmut/json`).
- Compatibility: host pins `requires-python >=3.11,<3.14` and runs CI on
  3.11/3.12/3.13 — mutmut's floor (3.10) covers all three. Cosmic-Ray 8.7.0
  (floor 3.9) was considered; mutmut wins on native fit because the host is a
  `src/`-layout pytest project with `testpaths=["tests"]`, which maps
  directly onto mutmut's `source_paths` / test-selection config model, and
  mutmut emits a machine-readable CI-stats JSON artifact the workflow gates on.
- Pin rationale: 3.8.0 is the current release on the official index at
  selection time. No lock entry is added here — the workflow installs the
  exact pin on the hosted runner (`uv pip install "mutmut==3.8.0"`), where
  the acquisition floor is satisfied. No local install was performed
  (disk ~38.9 GB free, below the 80 GiB acquisition floor).

## Correction vs the prior packet (review C-1/C-2)

The prior workflow invoked three `mutmut run` flags and a `mutmut junit`
subcommand that do not exist in 3.8.0. Verified against the pinned source
(read-only contents GETs at `?ref=3.8.0`, no clone, no install):

- `run` is declared as `@cli.command()` + `@click.option("--max-children")`
  + `@click.argument("mutant_names", nargs=-1)` (`__main__.py` lines 937-939):
  `--paths-to-mutate`, `--tests-dir`, `--runner` are absent.
- `paths_to_mutate` / `tests_dir` survive only as deprecated config-file keys
  (`configuration.py`), each emitting a rename warning toward `source_paths`
  and `pytest_add_cli_args_test_selection`.
- `junit` is not a registered subcommand (zero occurrences in `__main__.py`;
  prior review's code search returned 0). The CI-stats path in 3.8.0 is the
  bare `@cli.command()` on `def export_cicd_stats`, i.e. CLI name
  `export-cicd-stats` (Click hyphenates underscores), writing
  `mutants/mutmut-cicd-stats.json` — confirmed by README.rst
  ("Configuration" `[tool.mutmut]` section; "Mutation score badges" section:
  `mutmut export-cicd-stats`).
- The prior packet's claim of a docs-established "`mutmut junit` result
  output" is therefore RETRACTED. No `mutmut-results.xml` is produced or
  parsed anywhere in the corrected packet.

## Target selection: `src/polymarket_insider_tracker/redaction.py`

- Pure Python, stdlib-only imports (`re`, `urllib.parse`) — no services,
  no network, no DB/Redis needed on the mutation leg.
- Existing behavioral coverage: `tests/test_redaction.py` (110 KB adversarial
  suite) drives the real module plus real components, asserting every
  captured surface stays secret-free. This is the property a mutation run
  must actually exercise (secret-leak fail-closed), not a 5-line guard the
  existing suite already kills (review M-3).
- Repo-native budgets already cover this path: complexipy max 5 over
  `src tests scripts alembic conftest.py` (`scripts/complexipy_gate.py` +
  `complexipy` CI job), mypy strict, pyright, ruff, black, vulture — the
  mutation job adds the one missing dimension (fault detection) without
  touching any existing gate.

## Fail-closed controls in the corrected workflow

- Baseline `tests/test_redaction.py` must exit 0 first (syntax/import/
  collection errors never count as kills). This is additional to — not a
  replacement for — mutmut's own in-run guards: clean-test exit != 0 aborts
  the run ("Failed to run clean test", exit 1) and the forced-fail guard
  rejects a suite that cannot fail ("Unable to force test failures", exit 1).
- `mutmut export-cicd-stats` must produce a non-empty
  `mutants/mutmut-cicd-stats.json`; zero `total` fails the job
  (misconfiguration can never pass silently).
- `mutmut results` runs as a separate informational step with its exit code
  preserved (no `|| true`); it is non-gating display, explicitly labelled so.
- No score threshold is claimed. No requiredness is claimed.

## Why not the dotfiles coordinator

- The dotfiles checkout under this lane's read-only lease has no Python
  package layout (no `pyproject.toml`/`setup.py`/`setup.cfg`), no pinned
  mutation runner, and its `ci.yml` installs only
  `pytest/complexipy/ruff/radon`; the prior candidate's hand-seeded
  `or`→`and` mutant was proven already-killed by the existing 115-test
  handoff slice (review M-3), its kill step was a `SyntaxError` read as
  KILLED (M-1), and its test binds an absolute path outside the checkout
  (M-2). Retrying those six findings is explicitly out of scope; the prior
  4-test/custom-CI packet is DISCARDED, not repaired.

## Validation limit (explicit, no hosted run claimed)

- Executed in this lane: YAML parse of `mutation-oss-ci.yml` (jobs:
  `mutation`); TOML parse of the `[tool.mutmut]` delta; PyPI metadata fetch
  for the pin; read-only `gh api` content probes confirming every referenced
  path exists on the host
  (`src/polymarket_insider_tracker/redaction.py`,
  `tests/test_redaction.py`, `.github/workflows/ci.yml`, `uv.lock`,
  SHA-pinned actions matching the repo's own `ci.yml`); budget posture
  re-read from the host's own `pyproject.toml`; pinned mutmut source reads
  (`__main__.py`, `configuration.py`, `runners/harness.py`,
  `utils/file_utils.py`, `stats.py`, README.rst) establishing the real CLI.
- NOT executed: no hosted Actions run, no local `mutmut run`, no mutant
  score or survivor list is claimed. The candidate is a complete,
  review-ready enrollment file pair for fresh independent review, with
  fail-closed controls (baseline-green precondition, non-empty mutant-set
  gate, syntax/import/collection errors never count as kills) that the
  review lane must verify by running it hosted. Status: PROPOSED_UNVERIFIED.
