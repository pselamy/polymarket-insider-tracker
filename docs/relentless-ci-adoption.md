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
| REF-CMD-19 | `mypy src tests quality` | 1 | 520 | `tests/tooling/test_traceability.py:86: error: Function is missing a return type annotation`; 520 `error:` lines across the full authored scope (2026-10-19), all inside `tests/` | enrolled at the **non-test authored roots** (`src scripts quality alembic conftest.py`); `tests/` (520 errors) stays unenrolled until a later card fixes it |
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

Seven controls pass with zero suppressions and are enrolled in reference order. The
authored Python scope is `src tests quality scripts alembic conftest.py`; the
reference's `black`/`ruff`/`complexipy` commands were narrower (`src tests quality`),
so where the wider scope also passes with zero suppressions the enrolled command is
extended to it. Pyright is the one enrolled control whose full authored scope does
**not** pass; it stays enrolled only at the existing `src` scope. REF-CMD-19 mypy is
enrolled at the non-test authored roots (see the REF-CMD-19 step-1 update below);
the reference's `tests/` arguments stay visibly unenrolled with their measured
count.

| ID | Enrolled command | Scope |
| --- | --- | --- |
| `REF-CMD-01` | `uv lock --check` | whole-project lock (no path scope) |
| `REF-CMD-17` | `black --check src tests quality scripts alembic conftest.py` | **full authored scope** |
| `REF-CMD-18` | `ruff check src tests quality scripts alembic conftest.py` | **full authored scope** |
| `REF-CMD-19` | `uv run --isolated --locked --all-extras --python 3.11 mypy src scripts quality alembic conftest.py tests` | **all authored roots**: `src scripts quality alembic conftest.py tests` (step 5 completes `tests/`) |
| `REF-CMD-20` | `pyright` | **staged**: `src/polymarket_insider_tracker`, `scripts`, `quality`, `alembic`, `conftest.py`, `tests/quality` and four zero-diagnostic top-level test modules (see the REF-CMD-20 step 1 section); the rest of `tests` stays unenrolled |
| `REF-CMD-22` | `complexipy src tests quality scripts alembic conftest.py --max-complexity-allowed 5` | **full authored scope** |
| `REF-HOOK-02` | `verify_tracked` | **full**: complete tracked path inventory |

The full-authored-scope Pyright measurement is **1513 diagnostics** (1505 `tests`, 8
`scripts`, 0 `src`, 0 `quality`) under `typeCheckingMode = strict`, so REF-CMD-20 is
**not** enrolled at full scope. It is enrolled at the pre-existing narrow scope the
repository's own strict type gate already enforces (`[tool.pyright].include =
["src/polymarket_insider_tracker"]`); no suppression mechanism is used. Widening
`include` to the full authored scope is deferred until those diagnostics are fixed.

`quality/checks.json` contains exactly the six command controls in reference order
(1, 17, 18, 19, 20, 22). The `quality.verify` hook sequence runs `verify_tracked` and
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

## REF-CMD-19 step 1: per-root strict mypy measurement (2026-10-19)

Strict mypy (the repository's own `[tool.mypy]` strict configuration, mypy
`1.19.1`, Python-3.11 isolated locked resolution — the same environment shape as
the `strict-types` gate) was run **once per authored root**, passing the root
explicitly so every error line carries the file it belongs to:

| Root | Checked files | Errors | Dominant codes |
| --- | --- | --- | --- |
| `src` | 65 | 0 | — |
| `scripts` | 14 | 0 | — |
| `quality` | 89 | 0 | — |
| `alembic` | 5 | 0 | — |
| `conftest.py` | 1 | 1 → **0 after fix** | `no-untyped-def` |
| `tests` | 88 | **520 errors in 46 files** (deferred to a later card) | `attr-defined` 163, `arg-type` 147, `no-untyped-def` 93, `assignment` 41, `misc` 33 |

Consequence: every **non-test** authored root is strict-mypy clean. The single
`conftest.py` error was
`conftest.py:12: error: Function is missing a return type annotation [no-untyped-def]`,
fixed by annotating the `event_loop_policy` fixture with
`asyncio.AbstractEventLoopPolicy` (real typing, no suppression of any kind).

Relative to the reference row: the earlier measurement (403 errors) predates the
current vendored `quality/` tree and tool versions; the per-row REF-CMD-19 numbers
are refreshed in the enrollment update below. Today's full-scope run
`mypy src tests quality` over 263 files reports **520 errors in 46 files**, all of
them inside `tests/` (the reference example diagnostic
`tests/tooling/test_traceability.py:86: error: Function is missing a return type
annotation [no-untyped-def]` still reproduces as the first line).

## REF-CMD-19 step 1 enrollment: strict mypy over the non-test authored roots

With `quality/`, `alembic/` and `conftest.py` strict-mypy clean (update above), the
control is now enforced with zero suppressions:

- `[tool.mypy].files` is extended from `["src", "scripts"]` to
  `["src", "scripts", "quality", "alembic", "conftest.py"]`, so the repository's own
  `strict-types` static gate (which runs bare `mypy` from the checked-in
  configuration) now covers every non-test authored root. No per-module override,
  exclusion, error-code disable or ignore was added.
- REF-CMD-19 is enrolled in `quality/checks.json` in reference order (after
  REF-CMD-18, before REF-CMD-20) with the measured clean scope, mirroring the
  repository's own `strict-types` gate command shape:
  `uv run --isolated --locked --all-extras --python 3.11 mypy src scripts quality
  alembic conftest.py`. The minimum-supported single resolution matters for
  determinism: mypy's configured `python_version = "3.11"` is the minimum supported
  minor, and running mypy against a 3.12/3.13 interpreter environment makes it
  parse dependency stubs that use 3.12+-only `type` syntax against the 3.11 target
  (numpy's shipped stubs), which is why bare `mypy` in the host matrix environment
  is not the enrolled command. This keeps the enforced tool, configuration and
  resolution identical to the existing static profile gate.

Evidence (kept in `docs/relentless-ci-defect-evidence.md`):

- Clean pass: the enrolled command exits `0` with
  `Success: no issues found in 174 source files`; the full enrolled sequence
  `uv run --locked --group dev python -m quality.verify` exits `0`.
- Defect fail: a planted `quality/_defect_probe.py` containing
  `VALUE: int = "not-an-int"` makes the same enrolled command exit `1` with
  `quality/_defect_probe.py:3: error: Incompatible types in assignment (expression
  has type "str", variable has type "int") [assignment]`; the probe is deleted
  after the run.

Remaining gap: `tests/` keeps the reference arguments unenrolled — currently **520
errors in 46 files** under this strict configuration (`attr-defined` 163,
`arg-type` 147, `no-untyped-def` 93, `assignment` 41, `misc` 33 and others). That
is the later card; nothing in `tests/` was silenced to enroll this scope.

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
[`docs/duplication/`](duplication/README.md). REF-CMD-16 therefore
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
[`docs/duplication/`](duplication/README.md).

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

## REF-CMD-19 step 2: strict mypy over the first `tests/` directories (2026-10-19)

Per-directory strict mypy (`uv run --isolated --locked --all-extras --python 3.11 mypy <dir>`),
smallest first. Counts include errors reported in imported modules.

| root | errors | files |
|---|---|---|
| `tests/storage` | 3 | 1 |
| `tests/quality` | 4 | 1 |
| `tests/fakes` | 17 | 2 |
| `tests/alerter` | 30 | 6 |
| `tests/profiler` | 65 | 6 |
| `tests/integration` | 57 | 6 |
| `tests/detector` | 99 | 6 |
| `tests/ingestor` | 120 | 17 |
| `tests/tooling` | 139 | 7 |
| root `tests/test_*.py` (8 clean-of-others files with errors) | 18-84 each | - |

Cleaned and enrolled this step: `tests/alerter`, `tests/fakes`, `tests/quality`, `tests/storage`
(37 errors, no suppressions, casts, ignores or relaxed flags). Two typed boundary injections are
disclosed rather than hidden: `install_fake_w3` (`object.__setattr__` on the private `_w3` handle) and
`error.args = (payload,)` for the non-string `httpx.ConnectError` fault-injection test. The one `src/` touch is an explicit
`__all__` in `alerter/history.py` so the documented compatibility re-exports of
`_generate_dedup_key`/`_get_signals_from_assessment` are visible to strict mypy; runtime behaviour
is unchanged. Remaining unenrolled: `tests/detector`, `tests/ingestor`, `tests/integration`,
`tests/profiler`, `tests/tooling` and 8 root-level `tests/test_*.py` files.

Defect-fails: appending `x: int = "a"` to `tests/storage/test_repos.py` makes the enrolled mypy
command exit 1 (`error: Incompatible types in assignment`); the clean tree exits 0.

## REF-CMD-19 step 3: strict mypy over `tests/integration` and `tests/ingestor` (2026-10-19)

Per-directory strict mypy (`uv run --isolated --locked --all-extras --python 3.11 mypy <dir>`)
measured from `origin/main` at `1bdccbc`, smallest first (counts are for the directory alone):

| root | errors | files | step 3 |
|---|---|---|---|
| `tests/integration` | 38 | 3 | cleaned and enrolled |
| `tests/ingestor` | 72 | 13 | cleaned and enrolled |
| `tests/profiler` | 75 | 4 | unenrolled |
| `tests/detector` | 82 | 4 | unenrolled |
| root `tests/test_*.py` | 94 | 7 | unenrolled |
| `tests/tooling` | 119 | 6 | unenrolled |

Cleaned and enrolled: 110 errors (plus 9 reached through imports of root `tests/test_pipeline.py` and
`tests/test_persist_assessment.py` by the #163 characterization module), no suppressions, casts, ignores (three stale `type: ignore`
comments were removed) or relaxed flags. Fixes are typing-only: `tests/fakes/redis_await.py`
(`settled`, `as_bytes`) awaits the `Awaitable[T] | T` results redis-py declares; fakes and
harnesses accept `redis.asyncio.Redis`; `FakeClobClient` subclasses `ClobClient`; the codec tests
import `_publisher_codec` directly instead of private re-exports. The `src/` touches are annotation-only: `TradeStreamHandler` reconnect-delay parameters widen `int` to `float` (seconds; tests pass `0.01`); a `SignalDetector[S]` Protocol types the pipeline's two detector attributes and a `StreamConnection` Protocol types the handler's `_ws`, so fakes are assigned directly with no `monkeypatch.setattr` or `id()` workarounds.

Remaining unenrolled `tests/` roots: `tests/detector`, `tests/profiler`, `tests/tooling` and 7
root-level `tests/test_*.py` files.

Defect-fails: appending `PLANTED: int = "a"` to `tests/ingestor/test_gamma_client.py` makes the
enrolled mypy command exit 1 (`error: Incompatible types in assignment`); the clean tree exits 0.
Pinned jscpd clone delta against `origin/main`: 74 clones / 909 duplicated lines before and
after, measured at `origin/main` 370e77b (delta 0). Re-measured after the rebase onto `f2e2167` (raw pinned jscpd, same flags, authored `.py` scope): `origin/main` 63 clones / 766 duplicated lines over 256 files; this branch 63 / 766 over 257 files (delta 0). Six test files already above 399 lines grew by 1-12 lines each because annotating the
awaits and fakes needs real lines (disclosed as a conflict with the no-growth guidance).

## REF-CMD-19 step 4: strict mypy over `tests/profiler` and `tests/detector` (2026-10-19)

Per-directory strict mypy measured from `origin/main` at `a8d58c6` (Python 3.11 locked resolution):

| root | errors | files | step 4 |
|---|---|---|---|
| `tests/profiler` | 75 | 4 | cleaned and enrolled |
| `tests/detector` | 82 | 4 | cleaned and enrolled |
| root `tests/test_*.py` | 58 | 4 | unenrolled |
| `tests/tooling` | 119 | 6 | unenrolled |

Cleaned and enrolled: 157 errors, with no new suppressions, casts, ignores or relaxed flags. No `type: ignore` remains in the newly enrolled roots: the frozen-object mutation tests call `__setattr__` inside `pytest.raises(AttributeError)`, and the read-only-weights test asserts `DEFAULT_WEIGHTS` is not a `MutableMapping` and has no `__setitem__`. The `src/` touches
are annotation-only structural Protocols so the existing fakes satisfy real types:
`profiler.analyzer.WalletChainReader` (`WalletAnalyzer`'s client), `detector.fresh_wallet.WalletProfiler`
and `detector.size_anomaly.MarketMetadataSource`; `PolygonClient`, `WalletAnalyzer` and
`MarketMetadataSync` satisfy them structurally (parameters are positional-only so fake parameter names need not match). `tests/fakes/web3.py` `TransferLogIndex.queries`
now records narrowed `LogQuery` values instead of raw `FilterParams` TypedDicts; the funding tests
trace through a `FundingTracer` subclass instead of assigning over a method; the historical private
`funding._is_pruned_history_error` is read through `vars(funding)` with a typed `Callable`.

Remaining unenrolled `tests/` roots: `tests/tooling` and 4 root-level `tests/test_*.py` files
(`test_main.py`, `test_redaction.py`, `test_shutdown.py`, `test_terminal_error_durable.py`).

Defect-fails: appending `PLANTED: int = "a"` to `tests/detector/test_models.py` makes the enrolled
mypy command exit 1 (`error: Incompatible types in assignment`); the clean tree exits 0.
Pinned jscpd clone delta (raw command above, tracked authored `.py` scope): `origin/main` 56 clones /
672 duplicated lines over 258 files; this branch 56 / 672 over 258 (delta 0).

## REF-CMD-20 step 1: per-root strict pyright measurement (2026-10-19)

Strict pyright (`typeCheckingMode = "strict"`, pyright `1.1.411`, `enableTypeIgnoreComments =
false`) was run once per authored root with the path passed explicitly. The enforced command
is still the bare `pyright`; its scope is the `[tool.pyright].include` list in `pyproject.toml`,
which may only grow.

| Root | Before | After | State |
| --- | ---: | ---: | --- |
| `src/polymarket_insider_tracker` | 0 | 0 | enrolled |
| `scripts` | 8 | 0 | **enrolled this step** |
| `tests/quality` | 10 | 0 | **enrolled this step** |
| `quality`, `alembic`, `conftest.py` | 0 | 0 | **enrolled this step** |
| `tests/__init__.py`, `tests/test_harness_isolation.py`, `tests/test_sample_fixtures.py`, `tests/test_setup.py`, `tests/test_trades_v2_config.py` | 0 | 0 | **enrolled this step** |
| `tests/storage` | 9 | 9 | unenrolled (enrolled in step 2) |
| `tests/test_redaction_harness_characterization.py` | 1 | 1 | unenrolled |
| `tests/conftest.py` | 2 | 2 | unenrolled |
| `tests/test_persist_assessment.py` | 5 | 5 | unenrolled |
| `tests/test_pipeline_persistence.py` | 9 | 9 | unenrolled |
| `tests/alerter` | 30 | 30 | unenrolled |
| `tests/test_config.py` | 12 | 12 | unenrolled |
| `tests/test_shutdown.py` | 18 | 18 | unenrolled |
| `tests/test_terminal_error_durable.py` | 37 | 37 | unenrolled |
| `tests/fakes` | 49 | 49 | unenrolled |
| `tests/detector` | 57 | 57 | unenrolled |
| `tests/profiler` | 81 | 81 | unenrolled |
| `tests/test_redaction.py` | 80 | 80 | unenrolled |
| `tests/integration` | 86 | 86 | unenrolled |
| `tests/test_pipeline.py` | 99 | 99 | unenrolled |
| `tests/ingestor` | 143 | 143 | unenrolled |
| `tests/test_main.py` | 199 | 199 | unenrolled |
| `tests/tooling` | 268 | 268 | unenrolled |

Unenrolled total: 1505 (unchanged `tests` count). Most of the remaining small roots
(`tests/storage`, `tests/alerter`, `tests/detector`, `tests/fakes`) are dominated by
`reportPrivateUsage` — tests that read or write private attributes and methods of the
production classes. Clearing those honestly needs either a behavioural rewrite of each test
or an approved public accessor in `src`; neither a suppression nor a `src` change is in
scope for this step, so those roots stay visibly unenrolled.

Typing fixes in this step use no suppression, cast, or `Any`: `TypeGuard` helpers for
`isinstance(value, dict | list)` narrowing of untyped containers, and declared-return
accessor functions plus small Protocols for the Redis and pytest constructors whose upstream
`**kwargs` are untyped. The verify gate, its tests, and the README command moved from
`pyright src/polymarket_insider_tracker` to the config-driven `pyright`.

## REF-CMD-19 step 5: strict mypy over the last `tests/` roots (2026-10-19)

Measured from `origin/main` at `986a7c6` (Python 3.11 locked resolution), alone-per-root:

| root | errors | files | step 5 |
|---|---|---|---|
| `tests/tooling` | 118 | 5 (`test_trades_smoke.py` 72, `test_trades_smoke_v2.py` 42, others 1-2) | cleaned and enrolled |
| root `tests/test_*.py` | 59 | 4 (`test_main.py` 41, `test_redaction.py` 13, `test_terminal_error_durable.py` 3, `test_shutdown.py` 1) | cleaned and enrolled |

Top codes before: `attr-defined` 118, `no-untyped-def` 39, `union-attr` 6, `arg-type` 6, `override` 3.
Cleaned: 177 errors, with no suppressions, casts, `Any` returns or relaxed flags. `[tool.mypy] files`
and the REF-CMD-19 command now name the whole `tests` tree, so every authored root is covered. Fixes
are typing-only: smoke records are typed `SmokeRecord`; the CLI lifecycle fakes subclass `Pipeline`;
`Redis` is imported from `redis.asyncio` instead of through `pipeline`; the hostile-`args` exception
overrides `__getattribute__` (a property cannot override `BaseException.args`); the `.pyc` header is
built with `marshal`/`struct` instead of a private importlib helper. No `src/` changes.

Remaining unenrolled `tests/` roots: none.

Defect-fails: appending `PLANTED: int = "a"` to `tests/tooling/test_verify.py` makes the enrolled mypy
command exit 1 (`error: Incompatible types in assignment`); the clean tree exits 0.

## REF-CMD-20 step 2: next small test roots (2026-10-19)

Enrolled in `[tool.pyright].include` (strict, zero suppressions): `tests/storage` (9 → 0),
`tests/conftest.py` (2 → 0), `tests/test_redaction_harness_characterization.py` (1 → 0), the new
shared helper `tests/console_capture.py` (moved out of `tests/test_redaction.py`), and
`tests/test_pipeline_persistence.py` (9 → 0). Still unenrolled: `tests/test_persist_assessment.py`
(5; it scores hand-built `SignalBundle`s through the private `Pipeline._score_and_alert` and needs
its own decision on a public scoring entry point), and every larger root in the step 1 table.

Honest fixes, no suppression: `DatabaseManager.is_sync_open` / `is_async_open` and
`attach_async_engine` replace private-attribute reads and writes; `Pipeline._on_trade` became the
public `Pipeline.process_trade` (the documented per-trade entry the poller and replay callers
use); `wire_pipeline(trace_funding=False)` replaces assigning `_funding_tracer = None`.

## REF-CMD-20 step 4: shutdown test and non-pipeline fakes (2026-10-21)

Enrolled in `[tool.pyright].include` (strict, zero suppressions): `tests/test_shutdown.py` (17 → 0)
and every `tests/fakes` module except `pipeline.py` (20 → 0). Measured remaining unenrolled roots:
`tests/test_terminal_error_durable.py` 25, `tests/fakes/pipeline.py` 29, `tests/detector` 57,
`tests/profiler` 81, `tests/test_redaction.py` 58, `tests/integration` 86, `tests/test_pipeline.py` 90,
`tests/ingestor` (remainder), `tests/test_main.py`, `tests/tooling`.

Honest fixes: `test_shutdown.py` drives `GracefulShutdown` through delivered `SIGTERM`s and `wait()`
instead of reading `_shutdown_event`; the second-signal force exit runs in a subprocess and asserts
exit status `128 + SIGTERM`. Fakes use `inspect.isawaitable`, a typed `KeyLister` Protocol for
`key_count`, a `TypeGuard` row check (`sort_timestamp` is now shared by both provider fakes instead of
duplicated), `.get` plus assertions for `FilterParams`, and attribute assignment for the SDK's
`OrderBookSummary`. `tests/fakes/pipeline.py` (private wiring of `Pipeline` internals) and
`tests/test_terminal_error_durable.py` (private `Pipeline` state and worker-failure hooks) need
approved public `src` entry points and stay unenrolled.

## Merge queue flow (2026-10-20)

Every workflow (`ci.yml`, `relentless-ci.yml`, `mutation.yml`) also triggers on
`merge_group` (`checks_requested`), so the required checks `Required checks`,
`Complexipy complexity check` and the Relentless CI `Full analysis (Python 3.11)` /
`Full analysis (Python 3.13)` jobs report under the same names for queue entries.

Reviewed PRs are queued instead of merged directly. GitHub builds a temporary
`gh-readonly-queue/...` branch containing current `main` plus the PR, runs the required checks
on it, and only then lands the PR. This prevents semantic-conflict breaks, where two PRs pass
individually but fail together, as happened with 7332dd0. `ci.yml` keeps
`cancel-in-progress: false` for `merge_group` runs, and each queue entry has its own ref, so
entries never cancel each other. `tests/tooling/test_merge_group_workflows.py` fails if a
workflow loses the trigger. Enabling the queue itself is a repository setting made by the
owner, not by this change.

## Coverage step 1: per-module line+branch measurement (2026-10-19)

`coverage run -m pytest tests --ignore=tests/tooling` (Python 3.13 locked resolution,
`[tool.coverage.run] branch = true`, source `src`) over the whole suite: 1934 passed, 11 skipped.
`tests/tooling` is the gate meta-suite (it re-runs the verify profiles) and is not part of the
measured run. Total: 7238 statements, 351 missing; 1480 branches, 194 partially covered (94%).
24 of 67 measured modules were already at 100% line and branch.

| Module | Statements | Missing lines | Branches | Missing branches |
| --- | ---: | ---: | ---: | ---: |
| `__init__.py` | 1 | 0 | 0 | 0 |
| `__main__.py` | 323 | 18 | 92 | 13 |
| `alerter/__init__.py` | 7 | 0 | 0 | 0 |
| `alerter/_channel_delivery.py` | 62 | 0 | 12 | 2 |
| `alerter/_history_records.py` | 38 | 0 | 10 | 2 |
| `alerter/channels/__init__.py` | 3 | 0 | 0 | 0 |
| `alerter/channels/discord.py` | 58 | 1 | 12 | 1 |
| `alerter/channels/pacing.py` | 24 | 0 | 6 | 1 |
| `alerter/channels/response_values.py` | 30 | 1 | 8 | 1 |
| `alerter/channels/telegram.py` | 67 | 2 | 14 | 1 |
| `alerter/dispatcher.py` | 303 | 21 | 88 | 11 |
| `alerter/formatter.py` | 162 | 0 | 60 | 3 |
| `alerter/history.py` | 144 | 1 | 24 | 1 |
| `alerter/models.py` | 10 | 0 | 0 | 0 |
| `config.py` | 142 | 2 | 20 | 3 |
| `constants.py` | 2 | 0 | 0 | 0 |
| `detector/__init__.py` | 6 | 0 | 0 | 0 |
| `detector/_batch.py` | 16 | 0 | 6 | 0 |
| `detector/fresh_wallet.py` | 56 | 0 | 14 | 1 |
| `detector/models.py` | 107 | 0 | 0 | 0 |
| `detector/scorer.py` | 119 | 1 | 18 | 1 |
| `detector/size_anomaly.py` | 106 | 2 | 28 | 3 |
| `detector/sniper.py` | 161 | 4 | 38 | 5 |
| `endpoints.py` | 21 | 0 | 2 | 0 |
| `ingestor/__init__.py` | 11 | 0 | 0 | 0 |
| `ingestor/_attempt_budget.py` | 73 | 2 | 14 | 1 |
| `ingestor/_cycle_cutoff.py` | 21 | 0 | 2 | 0 |
| `ingestor/_http_attempt.py` | 124 | 0 | 14 | 0 |
| `ingestor/_publisher_codec.py` | 26 | 0 | 2 | 0 |
| `ingestor/_retry.py` | 22 | 0 | 0 | 0 |
| `ingestor/acquisition.py` | 129 | 10 | 6 | 1 |
| `ingestor/clob_client.py` | 170 | 17 | 18 | 2 |
| `ingestor/gamma_client.py` | 111 | 1 | 22 | 1 |
| `ingestor/health.py` | 378 | 8 | 68 | 6 |
| `ingestor/metadata_sync.py` | 247 | 32 | 40 | 12 |
| `ingestor/models.py` | 177 | 7 | 30 | 9 |
| `ingestor/observation_boundary.py` | 201 | 3 | 54 | 18 |
| `ingestor/page_analysis.py` | 53 | 1 | 14 | 1 |
| `ingestor/publisher.py` | 124 | 2 | 20 | 2 |
| `ingestor/readiness.py` | 24 | 0 | 6 | 0 |
| `ingestor/trade_poller.py` | 507 | 9 | 104 | 10 |
| `ingestor/trade_row_v2.py` | 16 | 0 | 6 | 0 |
| `ingestor/trade_rows.py` | 166 | 0 | 46 | 0 |
| `ingestor/trades_source.py` | 123 | 1 | 12 | 1 |
| `ingestor/v2_cursor_walk.py` | 134 | 1 | 24 | 1 |
| `ingestor/v2_page.py` | 64 | 0 | 14 | 1 |
| `ingestor/v2_smoke.py` | 35 | 35 | 2 | 2 |
| `ingestor/websocket.py` | 194 | 28 | 38 | 11 |
| `pipeline.py` | 484 | 66 | 108 | 24 |
| `pipeline_lifecycle.py` | 6 | 0 | 0 | 0 |
| `profiler/__init__.py` | 7 | 0 | 0 | 0 |
| `profiler/_funding_rpc.py` | 77 | 1 | 20 | 1 |
| `profiler/analyzer.py` | 92 | 0 | 22 | 2 |
| `profiler/chain.py` | 240 | 14 | 60 | 10 |
| `profiler/entities.py` | 51 | 0 | 8 | 0 |
| `profiler/entity_data.py` | 41 | 0 | 4 | 0 |
| `profiler/funding.py` | 112 | 3 | 30 | 0 |
| `profiler/models.py` | 110 | 0 | 8 | 0 |
| `profiler/rpc_handle.py` | 18 | 0 | 8 | 4 |
| `redaction.py` | 222 | 15 | 78 | 13 |
| `shutdown.py` | 149 | 19 | 44 | 12 |
| `storage/__init__.py` | 4 | 0 | 0 | 0 |
| `storage/database.py` | 73 | 5 | 12 | 5 |
| `storage/database_url.py` | 47 | 2 | 16 | 0 |
| `storage/models.py` | 87 | 0 | 0 | 0 |
| `storage/repos.py` | 213 | 5 | 14 | 2 |
| `terminal_errors.py` | 107 | 11 | 40 | 13 |

Closest to 100% and chosen for step 1 (staged enrollment): the 24 modules already complete plus
`alerter/_channel_delivery.py`, `alerter/_history_records.py`, `alerter/history.py`,
`alerter/formatter.py`, `detector/fresh_wallet.py`, `profiler/analyzer.py`,
`profiler/rpc_handle.py` and `profiler/funding.py`. Their gaps are one-line `Protocol` stubs
(`->exit` branches, closed by a docstring-only body rather than any exclusion) and a few
behaviours without a test.

### Coverage step 1: staged enforcement

The last two commands in `quality/checks.json` enforce the gate: `coverage run -m pytest tests
--ignore=tests/tooling` followed by `coverage report --fail-under=100 --include=<enrolled modules>`.
The enrolled list is the 32 modules now at 100% line and branch (the 24 complete modules plus the eight
brought up in this step); it may only grow. No `pragma: no cover`, omit list or threshold relaxation
is used. Dead code removed to get there: three unreachable `"wallet" in links` checks in
`alerter/formatter.py` (`_build_links` always sets `wallet`; output is byte-identical).
`detector/scorer.py` is deliberately NOT enrolled: its 3+ signal bonus branch is unreachable through
the public API (signal count maxes at 2) but removing it would change scoring semantics, which belongs
to slice 004 (PR #167). It waits for that slice; the file is unchanged from main.

Slice 004 layer 1 added two modules born at 100% line and branch (`detector/evaluation_clock.py`,
`detector/scoring_config.py`) to the enrolled list.

Defect-fails: appending an unexercised two-branch function to `endpoints.py` drops it to 83% and
`coverage report` exits 2; the reverted tree exits 0 (100%, 1687 statements, 276 branches).

## REF-CMD-20 step 3: alerter, config and persist-assessment roots (2026-10-21)

Measured from `origin/main` at `adf809b`; enrolled in `[tool.pyright].include` with zero
suppressions: `tests/alerter` (30 → 0), `tests/test_config.py` (12 → 0),
`tests/test_persist_assessment.py` (5 → 0).

Honest fixes, no suppression, cast, or `Any`:

- Circuit-breaker tests read and drive state through `get_circuit_status()` and real failing
  deliveries (a zero recovery timeout makes the next attempt the half-open probe) instead of
  `_circuit_state`.
- `AlertHistory.retention_ttl` is a new read-only property next to the existing `dedup_ttl`
  (the sibling accessor was already public; the private `_retention_ttl` was the only asymmetry).
- The duplicate-detection test records a real sent alert instead of hand-building a dedup key
  through `_get_dedup_key`.
- `test_persist_assessment.py` drives `Pipeline.process_trade` with a real fresh wallet and
  market (`FakeEth`, `market_for_trade`) instead of calling private `_score_and_alert` with
  hand-built signals; expected scores are the real scorer's (0.20 weighted / 0.50 confidence for
  the below-threshold row, 0.38 for the alert in the failure test, whose threshold is lowered to
  0.1 because the persistence contract does not depend on the threshold).
- `test_config.py` constructs settings through a typed `_load_from_environment` helper and
  `model_validate`, uses `.get` for optional `SettingsConfigDict` keys, a `Generator` return for
  the context manager, and exercises the control-character URL check through the public
  `PolygonSettings` validator rather than `_validate_http_url`.
- `key_count` in `tests/fakes/redis_await.py` replaces the untyped `dbsize()` calls; a
  `TypeGuard` narrows the Discord embed `fields` payload (it checks only `isinstance(..., list)`;
  the element shape is trusted and each lookup is asserted by the tests that read it).

Still unenrolled: `tests/test_shutdown.py` (17; signal-handler internals), `tests/test_terminal_error_durable.py`,
`tests/fakes`, `tests/detector`, `tests/profiler`, `tests/test_redaction.py`, `tests/integration`,
`tests/test_pipeline.py`, `tests/ingestor`, `tests/test_main.py`, `tests/tooling`.

## Mutation step 1: measurement beyond the redaction target (2026-10-20)

mutmut 3.8.0 (`[tool.mutmut]` in `pyproject.toml`, Python 3.11, `--max-children 8`) over the
redaction module plus four small pure-logic modules that are already under the 100% coverage gate.
One run mutated 634 mutants in 2m15s locally. "Timeout" mutants are counted by mutmut as caught
(the mutated code hung or exceeded the time limit); "no tests" means no selected test reached the
mutated function.

| Module | Killed | Survived | Timeout | No tests |
| --- | --- | --- | --- | --- |
| `endpoints.py` | 25 | 20 | 0 | 0 |
| `ingestor/_retry.py` | 21 | 1 | 0 | 0 |
| `ingestor/_attempt_budget.py` | 47 | 11 | 5 | 1 |
| `ingestor/_cycle_cutoff.py` | 21 | 0 | 0 | 0 |
| `redaction.py` (existing target) | 340 | 137 | 0 | 5 |

Chosen for step 1: `endpoints.py`, `_retry.py`, `_attempt_budget.py` and `_cycle_cutoff.py`. They
are tiny (40 to 146 lines), pure, deterministic, free of services, and encode safety-relevant
rules (the v1-retirement route match, the retry and jitter caps, the attempt-start rate budget,
the strictly increasing cycle cutoff). `detector/scorer.py` is not chosen: it is frozen until the
slice 004 decisions. `redaction.py` keeps its existing job but was not yet survivor-free (137
survivors, 5 untested mutants); it was enrolled in mutation step 2 (below). The hypothesis-based `test_v2_cursor_walk_properties.py` cannot run inside mutmut's
in-process stats run (`FailedHealthCheck: differing_executors`), so it is not in the mutmut test
selection; the plain suites that exercise the retry and cutoff modules are.

### Mutation step 1: survivors killed and the gate (2026-10-20)

New behavioural tests: `tests/test_endpoints.py` (route-level v1 matching, exact retirement
message), `tests/ingestor/test_retry_policy.py` (attempt limit, doubling, caps),
`tests/ingestor/test_attempt_budget_validation.py` (validation messages, window boundaries,
real default clock and sleep). The 20 `endpoints.py` survivors, 1 `_retry.py` survivor and the
11 survivors plus 1 untested mutant in `_attempt_budget.py` are all killed by them, except the
equivalent mutants below. Final run: `endpoints.py` 42 killed / 0 survived, `_retry.py` 22 / 0,
`_attempt_budget.py` 62 killed + 2 timeout / 0 survived, `_cycle_cutoff.py` 21 / 0.

Equivalent mutants (8 in `is_official_v1_trades_url`), each removed by a behaviour-preserving
change to the code under test, not skipped or hidden from mutmut, so the gate needs no skip list:

- `rstrip(".")` on the *constant* v1 hostname and `rstrip("/")` on the constant v1 path (six
  variants): the constants `data-api.polymarket.com` and `/trades` have no trailing dot or slash,
  so the call was a no-op; the comparison now uses `_V1_PARTS.hostname` / `_V1_PARTS.path` directly.
- `(parts.hostname or "")` with `""` mutated to `"XXXX"`: a URL with no hostname can never equal the
  v1 host. The default is replaced by an early `return False` when `parts.hostname is None`.
- `rstrip("XX.XX")` on the parsed hostname: `urlsplit().hostname` is lower-cased so it never ends
  in `X`, making the mutant equivalent to `rstrip(".")`. The trailing-dot strip is now
  `re.sub(r"\.+$", "", host)`, identical behaviour, whose pattern mutants are all killed by the
  trailing-dot test.

`mutmut` classes `timeout` mutants as detected (the mutated `AttemptBudget` hangs the existing
cancellation test); the gate accepts `killed` and `timeout` only.

Gate (`scripts/mutation_gate.py`, covered by `tests/tooling/test_mutation_gate.py`): the
mutation workflow pipes `mutmut results --all true` into the gate, which fails on any mutant of an
enrolled module that is not `killed`/`timeout`, and on an enrolled module with no mutants. The
enrolled list (`ENROLLED_MODULES`) only grows; a test pins the step-1 prefix. `redaction.py` is
still mutated by the same job (kept target) but stays unenrolled: 136 to 137 survivors and 5
untested mutants (run-to-run variance of one), a later step. Planted weakening: relaxing the
`attempt_limit` assertions to `>=` made the gate exit 1 (`_retry: 1 mutant(s) survived`); the
tests were restored. Local job time: mutmut 1m35s to 2m15s over 625 mutants with 8 workers.

## Mutation step 2: `redaction.py` enrolled (2026-10-21)

`polymarket_insider_tracker.redaction` is now in `ENROLLED_MODULES`. Starting point (step 1
measurement): 482 mutants, 137 survived, 5 untested. Final run (mutmut 3.8.0, Python 3.11,
`--max-children 8`): 429 mutants, 427 killed, 2 timeout, 0 survived, 0 no tests. The mutant count
fell because the simplifications below removed redundant code, not because mutants were skipped.

Survivors by rule before this step (143 non-killed on the re-measured run): fail-closed shape
selection (`_fail_closed_label`, `_is_fail_closed_shape`, `_nested_scheme_kind`, the `_has_*`
predicates, the unparseable and bracket netloc checks; about 75), invalid-port handling and port
labels (about 20), URL-fallback and scheme prefix (about 16), known-secret echo spellings
(`_query_credential_tokens`, `_component_spellings`, `redact_failure_with_secrets`; about 21),
query-key ambiguity (8), text-scan boundaries (5).

Kill strategy: exact-output behavioural tests (`tests/test_redaction_url_shapes.py`: every
fail-closed shape, userinfo and query rule; `tests/test_redaction_text_secrets.py`: trailing prose
delimiters, scan boundaries, one echoed spelling per credential form). Both files are in the
mutmut test selection. Plain parametrized tables were used rather than Hypothesis, because the
shapes are discrete and mutmut's in-process stats run rejects Hypothesis health checks.

Behaviour-preserving simplifications of `redaction.py` (all provably identical; none changes any
output):

- The `getattr(parts, "x", default)` plumbing over `parts: object` is replaced by a typed
  `SplitResult`; the defaults could never be used. `_is_invalid_port_shape`,
  `_is_fail_closed_shape` and the duplicated shape/label dispatch collapse into one
  `_fail_closed_label` (same check order as before).
- The port range test after `parts.port` was dead: `SplitResult.port` itself raises
  `ValueError` outside 0-65535 on 3.11-3.13. The port-tail range check in `_has_valid_port_tail`
  is likewise unreachable (an out-of-range port fails the earlier probe); a non-numeric tail
  (an empty one is the only shape that parses) is still rejected, and `partition` replaces
  `rpartition` because more than one colon already leaves a non-numeric tail.
- `_nested_scheme_kind`'s labels were only compared with `None`, and its split-nested branch was
  shadowed by the earlier check; it is now the boolean `_has_nested_scheme_shape`.
- `_safe_outer_host`: `"@" in host` cannot hold after `split("@")[-1]`, and `://`, `[` and `]`
  are all rejected by the character check of `_validated_host_label`.
- `_is_malformed_bracket_netloc` is removed: `urlsplit` rejects every unbalanced or non-address
  bracket shape itself, and the remaining hostless bracket shapes are caught by the unparseable
  and hostless-userinfo checks with the same label.
- `_fallback_redaction` partitions at the first `://` (a string without one is its own scheme
  candidate and contains `/`, which is never alphanumeric); `_safe_scheme_prefix` uses
  `isascii() and isalnum()` (an empty string is not alphanumeric).
- `_split_trailing_delimiters` is `rstrip`; the `_redact_url_match` guard was dead (every scan
  match contains `://`, whose `/` is not a trailing delimiter).
- Three one-character literals (`quote(..., safe="")`, the host punctuation, the scan boundary
  set) are named module constants. mutmut does not mutate module-level assignments, and the mutant
  `safe="XXXX"` is equivalent for `quote` because `X` is already unreserved; the behaviour of each
  constant is pinned by tests (percent-encoded `/` echoes, hosts with `.`, `-` and `:`, and each
  boundary character, including that `X` stays scannable).

Equivalence evidence: `tests/test_redaction_baseline_equivalence.py` replays 900 URL-shaped and
punctuation-soup strings (`tests/data/redaction_baseline_main.json`) against the outputs of
`redact_url`, `redact_text` and the known-secret echo masking on the pre-change module. A wider
differential run over 106,836 generated inputs (all schemes, userinfo, hosts, ports, brackets,
nested schemes, queries) found 0 differences between the old and new module.

One test depends on the interpreter's URL-parser hardening: `ht\ttp://u@/p` expects the parsed
scheme `http`, which needs the tab/newline stripping of recent 3.11+ patch releases.

Planted weakening: replacing the exact `redact_url(raw) == masked` assertion in
`tests/test_redaction_url_shapes.py` with a prefix check made the gate exit 1
(`polymarket_insider_tracker.redaction: 22 mutant(s) survived`); the assertion was restored.
Mutation job time locally: mutmut about 1m10s for 631 mutants across all five modules.

## REF-HOOK-04 step 1: reference security gate mapping (2026-10-21)

Reference: `selamy-labs/relentless-ci-python` at `b8b1913`, read-only clone under `/tmp`. The
reference `quality/security.py`, `quality/security_reports.py`, `quality/security.yml`,
`quality/gitleaks.toml`, `quality/gitleaks.ignore`, `quality/osv-scanner.toml` and the three `mise.toml`
pins are already byte-identical in this repository (vendored); nothing is enrolled yet.

| Reference element | Detail at `b8b1913` | This repository |
| --- | --- | --- |
| Hook | `quality.pipeline` calls `verify_security(root, policy)` after `verify_sources`; it deletes the four stale reports, then runs the three scanners | `quality/pipeline.py` and `quality/security.py` identical; REF-HOOK-04 unenrolled |
| Secrets | `gitleaks git .` (all history) and `gitleaks dir .` (tree), 8.30.1, `--redact --ignore-gitleaks-allow`, `quality/gitleaks.toml`, empty `quality/gitleaks.ignore`; report must be empty | pins and files identical |
| Dependencies | `osv-scanner scan source --lockfile uv.lock --all-vulns --all-packages`, 2.6.0, `quality/osv-scanner.toml` (no ignores or severity filters); `verify_audit` checks every lockfile package was scanned and none has a vulnerability | pins and files identical |
| SAST | `opengrep scan --config quality/security.yml --error --strict --disable-nosem --taint-intrafile --no-git-ignore --max-target-bytes 0` (1.30.0) over the exact `verify_sources` inventory; `verify_sast` checks the scanned-file set equals the inventory and no findings | identical; three Python rules apply (dynamic evaluation, unsafe deserialization, input-to-shell) |
| Provisioning | `mise --yes --locked exec --` with hash-locked `mise.lock` | tools provision and run locally |
| CI | `.github/workflows/ci.yml` uploads `static-security.json` and `workflows-security.json` | not yet wired |
| Suppressions | none (`--disable-nosem`, empty ignore files) | none allowed |

Measured here, calling the reference scanner functions directly because `verify_sources`
(REF-HOOK-01) still fails earlier on the tree (non-snake_case `alembic/versions` name, 50 files
over 399 lines) and would stop `verify_security` before any scan:

| Scanner | Result |
| --- | --- |
| `gitleaks git` (432 commits) | 11 findings, all in history: test fixtures with fake tokens (`tests/test_redaction.py` x5, `tests/tooling/test_gate_negatives.py` x2, `tests/tooling/test_secret_redaction.py`, `tests/alerter/test_dispatcher.py`, `tests/profiler/test_chain.py`) and one false positive on `extend_channel_claim(... ttl=...)` in historical `alerter/dispatcher.py` |
| `osv-scanner` | 60 advisory groups across 16 of 129 locked packages (aiohttp, anyio, certifi, filelock, h2, hpack, idna, mako, multidict, pydantic-settings, pygments, pytest, python-dotenv, requests, urllib3, virtualenv, web3) |
| `opengrep` (298 files) | 6 findings, all `python-unsafe-deserialization` in tests: `pickle.loads` round trips (5) and `yaml.load(..., Loader=yaml.BaseLoader)` (1) |

Open questions for the reviewer (no suppression is permitted): the gitleaks history findings are in
immutable commits and cannot be fixed by a code change; the gate also cannot run end to end until
REF-HOOK-01 is enrolled.

Correction/receipt (step 2): the 6 opengrep findings above were fixed in tests, with no suppression.
The five `pickle.loads` round trips now assert the module and name `pickle.dumps` references
(`tests/pickle_reference.py` reads the opcode stream with `pickletools`, never unpickling) and that
resolving that name yields the same object; `yaml.load` became
`BaseLoader(...).get_single_data()`. The `AlertRecord` test no longer runs `pickle.loads`: it asserts
the referenced class and that the reduced state equals the dataclass fields, which is weaker than a
full load round trip. The 6 findings in the table are the pre-fix measurement.

## REF-HOOK-04 step 3: security gate enrolled (2026-10-21)

Decisions applied (review round 1): `quality/checks.json` gains
`uv run --locked --group dev python -m quality.security_main` (appended; nothing reordered).
`quality.security_gate` enforces opengrep and gitleaks (`dir` over the tree, `git` over
`<cut-over>..HEAD`, cut-over `db93cf3c644d3a477038f422b82c91ddad6f59b4` in
`quality/gitleaks-cutover.txt`) and runs osv-scanner report-only (17 packages and 60 advisory
groups; the earlier "16 packages" count omitted one: the list above it names 17). The reference
`verify_security` hook stays unenrolled because `verify_sources` (REF-HOOK-01) fails on
50 oversized files and the alembic names; the gate runs independently instead. The full-tree
scan found 8 matches in the current files (also present in history, so part of the 11 above), all
removed by renaming test identifiers (see `docs/security.md`). The vendored `quality/security.py` is
unchanged; the deviations are documented in `docs/security.md`.
