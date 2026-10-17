# Independent parser correction review

**Verdict: SHIP, strictly for SEC003/004 pure-parser preparation at `e4c21a434a1244f197e970c8079b2af1802e593e`.**

The previously reproduced exception-context disclosure is corrected. This review certifies neither an accepted native finding nor the complete Story5/security extension. Source/worker/authentication/hosting/gate/governance activation, merge and main acceptance remain outside this verdict. No later control is treated as complete.

## Artifact and preserved evidence

- Original baseline: `523b0a35466e799c36e85e3bc5ea046cf3263c31`.
- Rejected first pass: `1d978b70b3215424bde025c589b478f6098ca133`.
- Corrected exact head: `e4c21a434a1244f197e970c8079b2af1802e593e`.
- Independently reviewed the exact first-pass-to-correction diff in a new clean detached clone, `work/security-parser-review/correction/`; the writer's primary checkout was read only. CodeGraph status and exact parser node navigation preceded source inspection.
- The first-pass REVISE report is unchanged at `work/security-parser-review.md`, SHA256 `3d1ad8b5a9287e2d1d51a78aa0f1b96479c741f83cf9539dea0445c5d71d67cd`. This separate report supersedes only its rejected implementation verdict for the new exact head, preserving the original finding and evidence.
- The correction changes three parser modules solely by adding explicit suppressed causes to 27 formerly bare domain-error raises, adds 13 regressions, makes the deep-JSON test portable while requiring a safe failure, and appends an honest corrective-preparation note. Parser models/package marker and exact governance proposal/contract remain unchanged.

## Independent refutation of the fix

`correction/.venv/bin/python correction_context_probe.py` from owned scratch passed. The probe AST-enumerates every direct `NativeReportError` raise across the parser, requires explicit `from None`, and uses runtime tracing to establish that **all30/30 raise sites actually execute** under active sensitive caller exception handling, including a nested sensitive cause. All30 reject with suppressed context. Both normal formatted traceback and `logger.exception` output exclude private context. Two success controls under active caller exception handling also succeed without inheriting the private context into model repr.

This covers JSON duplicate/nonfinite/size/decoder/root/row-budget errors; exit type/relationship, duplicate identity and mode errors; object/field/text/encoding/integer/coordinate/entropy/tags errors; path relative/component/control failures; line/column order; historical/tree commit failures; fingerprint mismatch; empty identity; and unredacted/unsupported enrollment. No unexercised domain raise site remained. Only safe counts/booleans/categories were emitted. Evidence: `correction_context_probe.py`, `correction-context-summary.json`.

A separate AST comparison found the three parser modules structurally identical to the first pass after removing raise causes. `scripts/__init__.py` and `_secret_models.py` are byte-identical. Thus the accepted-input/schema/identity/output model behavior is unchanged by the fix; exception rendering is the material behavior change.

The updated deep-JSON control accepts exactly `invalid report JSON` or `finding object schema`, with `NativeReportError` and suppressed context mandatory. This reflects decoder differences and cannot turn the invalid input into success. The isolated focused runs below verified it across3.11/3.12/3.13.

## Independently rerun checks

All candidate commands ran from the exact correction clone unless otherwise noted. Output is saved under `work/security-parser-review/`.

| Check | Exact command or probe | Outcome |
| --- | --- | --- |
| Locked preparation | `uv sync --locked --all-extras --python 3.11` | exit0; separate correction environment |
| Focused plus coverage | `uv run --locked --all-extras --python 3.11 pytest tests/tooling/test_secret_report.py tests/tooling/test_secret_schema.py --cov=scripts._secret_json --cov=scripts._secret_models --cov=scripts._secret_report --cov=scripts._secret_schema --cov-branch --cov-report=term-missing` | exit0;121 passed;174/174 statements,60/60 branches;0.19s |
| Python3.12 focused | `uv run --isolated --locked --all-extras --python 3.12 pytest tests/tooling/test_secret_report.py tests/tooling/test_secret_schema.py` | exit0;121 passed;0.09s |
| Python3.13 focused | `uv run --isolated --locked --all-extras --python 3.13 pytest tests/tooling/test_secret_report.py tests/tooling/test_secret_schema.py` | exit0;121 passed;0.06s |
| Native full static | `uv run --locked --all-extras --python 3.11 python scripts/verify.py --profile static` | exit0; all existing gates passed;50.52s |
| Native full-scope cyclomatic | `uv run --locked --all-extras --python 3.11 python -m ruff check src tests scripts alembic conftest.py --select C901,E902 --config 'lint.mccabe.max-complexity=10'` | exit0;all passed |
| Explicit strict parser/test mypy | `uv run --locked --all-extras --python 3.11 mypy --strict scripts/_secret_json.py scripts/_secret_models.py scripts/_secret_report.py scripts/_secret_schema.py tests/tooling/test_secret_report.py tests/tooling/test_secret_schema.py` | exit0;no issues in6 source files |
| Explicit strict parser/test Pyright | `uv run --locked --all-extras --python 3.11 pyright scripts/_secret_json.py scripts/_secret_models.py scripts/_secret_report.py scripts/_secret_schema.py tests/tooling/test_secret_report.py tests/tooling/test_secret_schema.py` | exit0;0 errors/warnings/informations |
| Hostile active context | `correction/.venv/bin/python correction_context_probe.py` | exit0;30/30 domain raise sites,30 rejection cases,2 successful controls;no traceback/logger context disclosure |
| Broader adversarial probe | `correction/.venv/bin/python correction_adversarial_probe.py` | exit0;458 assertions retained;leakage/schema/exit/mode/commit/collision/tag/coordinate/budget/digest/path cases |
| Import effects | `correction/.venv/bin/python correction_import_probe.py` | exit0;5 imports,no forbidden writes/network/process/environment/cwd effects or stdout/stderr |
| Real installed native schema probe | `correction/.venv/bin/python correction_native_schema_probe.py` | exit0;Gitleaks8.30.1;fresh disposable synthetic dir/git each exit42,one row,both untouched reports parsed |

Static profile retained the full existing lock/Black/Ruff/mypy/native-src-Pyright/Vulture/Complexipy5 scope and thresholds. Separate C90110 and strict6-file gates add evidence without substituting for native gates. The Pyright available-version notice is informational. Native schema probes use a controlled custom rule and do not qualify the protected default policy or scanner artifact provenance. No Match, Secret, or native candidate values were emitted.

Logs: `correction-focused.log`, `correction-focused312.log`, `correction-focused313.log`, `correction-static.log`, `correction-c901.log`, `correction-mypy.log`, `correction-pyright.log`; safe probe summaries use the `correction-` prefix.

## Independent RED and collection preservation

A separate owned clone, `correction-red/`, was checked out at the immutable first pass. Only the corrected test module was copied into it. Using the first-pass environment, ran:

`../candidate/.venv/bin/python -m pytest tests/tooling/test_secret_report.py::test_rejections_suppress_active_caller_exception_context tests/tooling/test_secret_report.py::test_mode_rejection_suppresses_active_caller_exception_context`

Exit1, **all13 selected controls failed** on the unchanged first-pass parser, as expected. The exact correction passes them. Evidence: `correction-red.log`. Original baseline/first-pass clones used for collection were not modified.

Updated actual collection command:

`uv run --locked --all-extras --python 3.11 pytest --collect-only -q -o addopts=''`

Exit0; **1,719 IDs** collected in55.51s. Set comparison against the earlier independently collected exact baseline and first-pass sets retained **all1,598 original baseline IDs and all1,706 first-pass IDs**, removed0, and added exactly13 active-context regression IDs in the existing new `test_secret_report.py` module. Overall additions relative to baseline are121. No donor-module rename or unperformed donor reconciliation is inferred.

Sorted correction ID SHA256: `ddf113ebf318ada43103b3488504401b4a55b87f672333f99941af5ef455d4b8`.

Evidence: `correction-collection.txt`, `correction-collection-comparison.json`, with preserved original `baseline-collection.txt` and `candidate-collection.txt`.

## Verdict limits

No blocking defect remained in the exact pure-parser preparation after this rereview. The SHIP verdict is limited to SEC003/004. It does not accept H0–H10 or any other native finding, authenticate a scan/source/blob or human decision, activate the proposed amendment, qualify protected workers/transport/approval/merge freshness/hosting, integrate a secrets gate, grant access changes, authorize merge, or certify main. SEC005–015 remain required.

The root reviewer owns full committed-source compatibility and service/matrix/convergence checks; they were not duplicated or independently certified here. Focused3.11–3.13 tests and collection do not replace that work. No Linux matrix or real repository scan was performed. No writer-checkout edits, real finding dispositions, external messages or access mutations occurred. The correction clone remains tracked-file clean at the exact reviewed head.

## Exact reviewed code hashes

| File | SHA256 |
| --- | --- |
| `scripts/__init__.py` | `c4f8f6da8e68f3e641155dcb964d80a228a9c57a2a0f664276b07575b826e5fd` |
| `scripts/_secret_json.py` | `f25d2a1b632b504051d58a6f57425c4ebb97e8a48f4d27c665cbdad05425f674` |
| `scripts/_secret_models.py` | `35bd9d71f939dad910fa16e4a1dc63aab17f592ea7a7d66ecd7fe642a1a1129d` |
| `scripts/_secret_report.py` | `f95c5b21fd929abd175b2de792e5862137a160118e41f2b83e3bfdf56143e68a` |
| `scripts/_secret_schema.py` | `b68f5a0ccab982101d5c857bec77d69dcda822478eb3334e1c07364e7d9c485e` |
| `tests/tooling/test_secret_report.py` | `1425a81a2aef1b66f1a6f4f30ef21698c2e6da4fca7c7d4bf0e3fbc0ad92cf64` |
| `tests/tooling/test_secret_schema.py` | `b09645a32ff65857b38336b7852c3b1f81fe43c7e1deeac959d262dd9e0a6d41` |
