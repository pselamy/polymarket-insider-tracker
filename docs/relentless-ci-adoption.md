# Relentless CI framework adoption ledger

Reference: [`selamy-labs/relentless-ci-python`](https://github.com/selamy-labs/relentless-ci-python)
pinned at `b8b191341e3061d7600bb1666f39da5374a492d9`. The reference `quality/`
package is vendored into this repository and its `quality.verify` entry point runs
the same checked-in gate definitions locally and in the hosted "Relentless CI gate"
job.

## Staged enrollment rule

Enrollment is **staged**. `quality/checks.json` and the `quality.verify` hook
sequence contain only controls that pass on the current `main` tree with **zero
suppressions** — no `noqa`, `type: ignore`, `pragma: no cover`, ignore lists,
baselines, exclusions, lowered thresholds, or allowlists. Every control that does
not pass is listed below with its measured violation count and stays visibly
unenrolled.

`quality/checks.json` **may only ever grow** in later pull requests: enrolled
controls keep their reference order and new controls are added, never removed or
reordered to hide a failure. A control moves from *unenrolled* to *enrolled* only
when it passes with no suppression mechanism.

## How the measurements were taken

The reference `quality/` package was copied into a scratch worktree of the current
`main` commit and adapted only for identity (package name, project paths, and
`SOURCE_ROOTS` covering `src`, `tests`, `quality`, `scripts`, `alembic`, and
`conftest.py`). Each of the 27 `checks.json` commands and the 6 hooks was then run
individually against that tree with the reference tool set installed. Results below
are the observed exit code, the number of reported violations, and the first
diagnostics.

Environment: Python 3.11, `uv 0.11.21`, repo `main` at
`e39132f` plus the copied `quality/` package. Violation counts for name/size
controls are measured with an independent scan because the control fails closed on
its first violation.

## Control inventory (33 controls)

State legend: **enrolled** = runs in `quality/checks.json` or the `quality.verify`
hooks; **unenrolled** = measured, documented, not yet enforced.

| ID | Control | Exit | Violations | First diagnostics | State |
| --- | --- | --- | --- | --- | --- |
| REF-CMD-01 | `uv lock --check` | 0 | 0 | `Resolved 104 packages in 1ms` | enrolled |
| REF-CMD-02 | `python -m quality.dependency_policy_main` | 1 | 2 | `ValueError: dependency lock entry must be a table` (binary-only `psycopg-binary`, `pywin32` ship no sdist) | unenrolled |
| REF-CMD-03 | `python -m quality.component_inventory_main` | 1 | 104 | `ValueError: dependency lock entry must be a table`; no reviewed SPDX provenance policy for the 103 registry components | unenrolled |
| REF-CMD-04 | `python -m quality.runtime_support_main` | 1 | 1 | `ValueError: Python package metadata differs from reviewed runtime inventory` (no reviewed `runtime-support.json`; `ci.yml` matrix shape differs) | unenrolled |
| REF-CMD-05 | `python -m quality.workflows_main` | 1 | n/a | `mise ERROR "shellcheck" couldn't exec process` (native ShellCheck/actionlint/zizmor not provisioned) | unenrolled |
| REF-CMD-06 | `python -m quality.support_files_main` | 1 | 2 | `ValueError: runtime YAML mapping keys must be strings` (`.github/workflows/ci.yml`, `mutation.yml` use the YAML 1.1 boolean key `on:`) | unenrolled |
| REF-CMD-07 | `python -m quality.readme_example_main` | 1 | 1 | `ValueError: README example heading is missing or duplicated` | unenrolled |
| REF-CMD-08 | `python -m quality.document_links_main` | 1 | 2 | `ValueError: local documentation target is missing: probes/`; also `contracts/` | unenrolled |
| REF-CMD-09 | `python -m quality.document_spelling_main` | 1 | n/a | `mise ERROR "typos" couldn't exec process` (native typos-cli not provisioned) | unenrolled |
| REF-CMD-10 | `python -m quality.document_style_main` | 1 | 64 | `ValueError: authored Markdown style differs` (64 of 84 authored Markdown files are not pinned GFM formatted) | unenrolled |
| REF-CMD-11 | `python -m quality.incomplete_main` | 1 | 1 | `ValueError: Python module is not snake_case: alembic/versions/20260104_0000_initial_schema.py` (fails in `verify_sources` before scanning) | unenrolled |
| REF-CMD-12 | `python -m quality.repository_hygiene_main` | 1 | 4 | `ValueError: unsupported authored text type: .dockerignore`; also `.pyi`, `.ini`, `.example` | unenrolled |
| REF-CMD-13 | `uv export --locked --only-group build …` | 2 | 1 | `error: Group `build` is not defined in the project's `dependency-groups` table` | unenrolled |
| REF-CMD-14 | `python -m quality.build_constraints_main` | 1 | 1 | `FileNotFoundError: .quality-results/build-constraints.txt` (depends on REF-CMD-13) | unenrolled |
| REF-CMD-15 | `python -m quality.package_main` | 1 | 1 | `CalledProcessError`: `uv build … --require-hashes` cannot resolve `setuptools>=61.0` (reference build-constraint policy) | unenrolled |
| REF-CMD-16 | `python -m quality.duplication_main` | 1 | 92 | `ValueError: scripts/traceability.py: 447 physical lines exceeds 399` (fails in `verify_sources` before scanning; the pinned native scanner itself runs — measured directly, see the review-round-1 update below) | unenrolled |
| REF-CMD-17 | `black --check src tests quality` | 1 | 32 | `would reformat quality/build_constraints.py`, `package_build.py`, `commands.py`, `package_process.py`, `mutation_pool.py` | enrolled at the **full authored scope** after formatting `quality/` at line length 100 |
| REF-CMD-18 | `ruff check src tests quality` | 1 | 5 | `SIM105` (`commands.py`, `owned_process.py`), `SIM102` (`incomplete.py`), `ARG001` (`document_links.py`, `trusted_policy/github_read.py`) | enrolled at the **full authored scope** after fixing the 5 vendored violations |
| REF-CMD-19 | `mypy src tests quality` | 1 | 403 | `tests/tooling/test_traceability.py:86: error: Function is missing a return type annotation`; 403 `error:` lines across the full authored scope | unenrolled |
| REF-CMD-20 | `pyright` | 0 | 0 | `0 errors, 0 warnings, 0 informations` | enrolled at the existing `src` scope only; full authored scope **unenrolled** — 1513 diagnostics (1505 `tests`, 8 `scripts`) |
| REF-CMD-21 | `vulture src tests quality` | 3 | 7 | `quality/mutation_trial.py:8: unused function 'run_trial'`; 7 findings | unenrolled |
| REF-CMD-22 | `complexipy src tests quality --max-complexity-allowed 5` | 0 | 0 | all functions and module control flow at cognitive complexity ≤ 5 | enrolled at the **full authored scope** |
| REF-CMD-23 | `deptry src` | 1 | 16 | `DEP002 'psycopg' defined as a dependency but not used`; 16 findings | unenrolled |
| REF-CMD-24 | `deptry src tests quality --non-dev-dependency-groups dev --ignore DEP002` | 1 | 154 | `DEP003 'grimp' imported but it is a transitive dependency`; 154 findings | unenrolled |
| REF-CMD-25 | `python -m quality.architecture_main` | 1 | 1 | `ValueError: Python module is not snake_case: alembic/versions/20260104_0000_initial_schema.py` (fails in `verify_sources`) | unenrolled |
| REF-CMD-26 | `python -m quality.stability_main` | 1 | 1 | two seeded full-suite runs; `tests/tooling/test_gate_negatives.py::test_static_profile_passes_as_control` fails while `quality/` is unformatted | unenrolled |
| REF-CMD-27 | `python -m pytest --cov … --json-report …` | 1 | 1 | `1 failed, 3 skipped, 55 warnings` (1633 collected; warning and failure inventory not yet clean) | unenrolled |
| REF-HOOK-01 | `verify_sources` — owned scope and physical-size control | 1 | 50 | `ValueError: Python module is not snake_case: alembic/versions/20260104_0000_initial_schema.py`; 232 discovered `py`/`pyi` files, 50 exceed 399 physical lines, 5 `.pyi` stubs are outside the `.py`-only scope | unenrolled |
| REF-HOOK-02 | `verify_tracked` — NUL-delimited tracked inventory and portability | 0 | 0 | passes on the complete tracked path inventory | enrolled |
| REF-HOOK-03 | `prepare_tests` + `verify_tests` — fresh report and coverage completeness | 1 | 1 | `ValueError: Python module is not snake_case: …` (gated by `verify_sources`; report also carries 55 warnings) | unenrolled |
| REF-HOOK-04 | `verify_security` — pinned Gitleaks/OSV/Opengrep | 1 | n/a | `mise` native scanners not provisioned; fails closed before scanning | unenrolled |
| REF-HOOK-05 | `mutate` — full Cosmic Ray inventory, baseline and outcomes | 1 | 1 | `FileNotFoundError: 'cosmic-ray.toml'` (native mutation service not provisioned) | unenrolled |
| REF-HOOK-06 | trusted policy + branch protection | n/a | n/a | not locally runnable; requires app-owned policy check and authenticated review | unenrolled |

## Enrolled controls and their scope

Six controls pass with zero suppressions and are enrolled in reference order. The
authored Python scope is `src tests quality scripts alembic conftest.py`; the
reference's `black`/`ruff`/`complexipy` commands were narrower (`src tests quality`),
so where the wider scope also passes with zero suppressions the enrolled command is
extended to it. Pyright is the one enrolled control whose full authored scope does
**not** pass; it stays enrolled only at the existing `src` scope.

| ID | Enrolled command | Scope |
| --- | --- | --- |
| `REF-CMD-01` | `uv lock --check` | whole-project lock (no path scope) |
| `REF-CMD-17` | `black --check src tests quality scripts alembic conftest.py` | **full authored scope** |
| `REF-CMD-18` | `ruff check src tests quality scripts alembic conftest.py` | **full authored scope** |
| `REF-CMD-20` | `pyright` | **narrow**: existing `src/polymarket_insider_tracker` only |
| `REF-CMD-22` | `complexipy src tests quality scripts alembic conftest.py --max-complexity-allowed 5` | **full authored scope** |
| `REF-HOOK-02` | `verify_tracked` | **full**: complete tracked path inventory |

The full-authored-scope Pyright measurement is **1513 diagnostics** (1505 `tests`, 8
`scripts`, 0 `src`, 0 `quality`) under `typeCheckingMode = strict`, so REF-CMD-20 is
**not** enrolled at full scope. It is enrolled at the pre-existing narrow scope the
repository's own strict type gate already enforces (`[tool.pyright].include =
["src/polymarket_insider_tracker"]`); no suppression mechanism is used. Widening
`include` to the full authored scope is deferred until those diagnostics are fixed.

`quality/checks.json` contains exactly the five command controls in reference order
(1, 17, 18, 20, 22). The `quality.verify` hook sequence runs `verify_tracked` and
then the enrolled commands; every other hook stays unenrolled as documented above.

Defect-fails / clean-passes evidence for each enrolled control is recorded in
[`docs/relentless-ci-defect-evidence.md`](relentless-ci-defect-evidence.md).

## Owned-scope expansion

The vendored `quality/` package is authored Python owned by this repository, so the
repository's own invariant — every tracked Python file is covered by the owned
scope — now includes it. The **vulture** and **complexipy** gate scopes are extended
from `src tests scripts alembic conftest.py` to
`src tests scripts alembic conftest.py quality` in `pyproject.toml`,
`scripts/_verify_policy.py`, `scripts/complexipy_gate.py`, and the existing CI
vulture/complexipy steps. This strictly **adds** coverage; it removes no path,
lowers no threshold, and adds no exclusion, baseline, or allowlist. The existing
gates' exit semantics and required-check wiring are unchanged.

The `quality` package's functions that are used only by the reference test suite are
exercised by the vendored tests under `tests/quality/`, so vulture sees them used
without any whitelist. `quality/checks.json` and the enrolled hook set are covered
by `tests/quality/` and are bound to the ledger above.

The independent strict type gates are unchanged: mypy still covers `src` and
`scripts`, and Pyright still covers `src/polymarket_insider_tracker`.

## REF-CMD-16 update: jscpd provisioned, scanner runs, enrollment still blocked

Provisioning (2026-10-19): `mise.toml` and `mise.lock` are now checked in, copied
verbatim from the reference (`github:kucherenko/jscpd = 5.3.3` plus the reference
tool set), and `mise --yes --locked exec -- jscpd --version` prints exactly
`jscpd 5.3.3` (the receipt `quality.duplication` requires). The hosted Relentless
CI workflow gains a checksum-pinned `jdx/mise-action` step with
`install_args: --locked`, mirroring the reference `ci.yml`, so `quality.verify` and
the duplication gate run behind the same locked tool set locally and in CI.

With the native scanner available, `python -m quality.duplication_main` now fails in
`verify_sources` — the vendored `MAXIMUM_LINES = 399` physical-size control — on the
first oversized authored file (`scripts/traceability.py` at 447 lines at measurement
time; 50 authored files exceeded 399 lines in the REF-HOOK-01 measurement above).
This is the pre-existing size-debt blocker, not a missing-tool error, and it is not
fixed by suppression or by excluding the directory.

The native scanner itself was run over the full authored scope (255 authored Python
files discovered by `quality.source_scope`, staged and scanned with the vendored
policy: `--mode mild --format python --min-tokens 50 --min-lines 4 --threshold 0
--no-gitignore --workers 1`, empty config, no ignore lists). Result: **90 clone
groups, 1214 duplicated lines, 2.19% duplication** — the measured count recorded in
the ledger row above. The full clone inventory lives in
[`docs/duplication-inventory.md`](duplication-inventory.md). REF-CMD-16 therefore
remains **unenrolled** (US-004's zero-clone condition is not met), and the
`399-line` size gate must be paid down before `duplication_main` can reach the
scanner and a clean enrollment is possible.

## Review round 1 correction: duplication measured without the out-of-scope file splits

Correction (2026-10-19): the first measurement above ran on a tree that also carried
three out-of-scope file splits (`__main__.py`, `scripts/traceability.py`,
`scripts/trades_smoke.py` into eight helper modules) made only to get past the
399-line hook. That split violated this card's rules and has been reverted (commit
`43b4abb`; `git diff origin/main -- src scripts` is empty). The historical numbers
above are kept as recorded; the corrected measurement below supersedes them.

The corrected scan bypasses the size hook without modifying any vendored logic:
the authored scope is enumerated exactly as `verify_sources` does
(`quality.source_scope.discover` + `validate_path` — the same file set, minus the
`verify_size` call), the files are frozen with
`quality.duplication_inputs.stage_inputs`, and the vendored
`quality.duplication.scan(root, config, staged, output, minimum=50, deadline)` runs
the scanner with its own `SCAN_POLICY[50]` arguments. Exact raw command (from the
repo root, pinned tool set):

```
mise --yes --locked exec -- jscpd --config <empty-config.json> --mode mild \
  --format python --min-tokens 50 --min-lines 4 --threshold 0 \
  --max-size 9007199254740991 --no-gitignore --absolute --reporters json \
  --summary --summary-top <n> --workers 1 --no-colors --no-tips \
  --output <dir> <staged files...>
```

Result on the reverted tree: **258 authored Python files staged, 92 clone groups,
1230 duplicated lines, 2.16% duplication** (scanner statistics report 225 sources
after its small-file skips). All 90 clones from the first measurement reproduce
byte-identically; the two additions come from the v1->v2 endpoint work merged to
main (`tests/ingestor/test_endpoint_switch_resume.py` bootstrap, 67 tokens;
`tests/ingestor/_v2_walk_harness.py` <-> `tests/ingestor/test_v2_cursor_walk.py`
harness construction, 50 tokens). The regenerated inventory is
[`docs/duplication-inventory.md`](duplication-inventory.md).

REF-CMD-16 / REF-HOOK-01 coupling: `python -m quality.duplication_main` cannot
reach the scanner on this tree, and enrollment additionally depends on the
unenrolled source-size hook. Exact call path:

```
quality.duplication_main.main
  -> quality.duplication.verify_duplication(root)
    -> quality.duplication_inputs.duplication_inputs(root)   # freezes enrolled inputs
      -> quality.source_scope.verify_sources(root)           # REF-HOOK-01
        -> quality.source_scope.verify_size(path)            # raises on first >399-line file
```

On the current tree `verify_size` raises
`ValueError: scripts/traceability.py: 447 physical lines exceeds 399` (48 of the
258 authored files exceed 399 lines). REF-HOOK-01 is itself unenrolled, so
`duplication_main` (and therefore any REF-CMD-16 enrollment evidence produced
through it) stays blocked twice over: by the 92 measured clones and by the size
hook its own input-freezing step depends on.
