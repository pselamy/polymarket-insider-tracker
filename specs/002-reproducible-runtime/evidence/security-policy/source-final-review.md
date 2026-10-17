# SEC005 exact-artifact byte-guard re-review

**Verdict: SHIP — SEC005 source-binding preparation only, at exact commit
`96b0faf9d4eb7d4fa21e5b4c2a298c673c0291d4`.**

Candidate tree: `435e14d3578376f154c4c91494cf314b4b2340bf`.
Reviewed against rejected base `fb5c792d18e0d9a3e084ce858995589e12534020`, tree
`c989b0e9284f139a9cce536716fff16b3936872f`.

The sole remaining raw-byte integrity/privacy defect is corrected. I found no remaining actionable
blocker in this declared preparation layer after independently attempting to refute the guard and its
adjacent behavior. This is component review, not SEC005 task closure: root must finish exact committed
compatibility and required evidence before checking the task complete. Full worker/authentication,
actual scan or finding acceptance, governance, OS sandbox, Linux provisioning, hosting, merge and
release acceptance remain outside this verdict.

## Correction and independent counterexamples

Only `_secret_json.py` changes runtime behavior relative to the rejected base. Its cohesive input
helper requires `type(payload) is bytes` before invoking length or decoding; the existing size check
and complete strict decoder remain. The other eight parser/source runtime modules are byte-identical
to the reviewed base. The added process test changes no runtime transport behavior.

I re-ran the exact previously preserved raw-byte counterexamples at this new committed archive:

- Subclasses wrapping invalid JSON `b"not-json"` and invalid UTF-8 `b"\xff"`, with decoding redirected
  to `"[]"`, now raise constant `NativeReportError("report bytes schema")`.
- The raising decoder now raises that same safe domain error before the decoder executes. Synthetic
  candidate private text and an unrelated active caller private exception are absent from traceback
  and standard exception logging; context suppression is true.

A separate independent harness passed **29/29 assertions**, including stronger method tripwires:

- Both overridden `__len__` and `decode` invocation counts are **zero**. Redirecting and raising
  subclasses, mutable bytearray, memoryview, string, None, bool and int inputs fail safely before
  their behavior can alter parsing or the digest.
- Ordinary exact-byte clean reports work in both real modes. A complete synthetic native finding
  preserves its full qualified fields, ordered tags and exact raw-byte SHA256; inconsistent exits,
  missing/unknown fields, invalid secret marker, boolean coordinate and duplicate finding fail.
- Ordinary empty/invalid JSON, invalid UTF-8, non-array root, trailing input, duplicate JSON key and
  nonfinite number failures remain safe.
- An actual **16MiB** valid report envelope with trailing JSON whitespace passes and retains its full
  digest; **16MiB+1** fails without truncation. The raw array decoder accepts exactly **100,000** rows;
  **100,001** fails at the public report boundary. The exact-row test uses nulls solely to qualify the
  JSON envelope capacity, not as valid native findings or accepted scan evidence.

Every negative checks a bounded domain category, context suppression and private sentinel absence
from both formatted traceback and standard exception logging inside an active unrelated exception.
The earlier defect's underlying nominal bytes can no longer disagree with the text being decoded.

## Real timeout/reap regression and focused matrix

The added InterruptedWait regression uses a typed real Popen subclass for narrow failure injection.
It forces a private-bearing `subprocess.TimeoutExpired` at the wait boundary while retaining the
real child, constant error translation and actual cleanup. It asserts the child has a return code
and its PID is absent after the call. This is a bounded failure-path control, not a generic mock
framework. The existing real silent and stdout-EOF hang tests remain and passed.

All **288** focused checked-in controls passed independently at the exact clean archive on each
supported Python minor:

| Interpreter | Result | Measured focused time |
| --- | --- | --- |
| Python 3.11.15, with branch coverage | 288 passed | 13.31s |
| Python 3.12.13 | 288 passed | 13.11s |
| Python 3.13.14 | 288 passed | 12.99s |

Commands used the checked-in lock and isolated all-extras environments:

```text
uv run --isolated --locked --all-extras --python <minor> pytest -q \
  tests/tooling/test_secret_report.py tests/tooling/test_secret_schema.py \
  tests/tooling/test_secret_git.py tests/tooling/test_secret_source.py \
  tests/tooling/test_secret_identity.py tests/tooling/test_secret_tree.py \
  tests/tooling/test_secret_process.py tests/tooling/test_secret_source_boundaries.py
```

For 3.11 I additionally measured all nine parser/source modules with a standalone scratch coverage
config setting branch coverage and empty exclusion/partial-branch lists. The JSON result independently
confirms **604/604 statements, 190/190 branches, zero missing lines/branches, zero excluded lines**.
The fixed raw finding repr and timeout handler are included; the project's repr exclusion is not used.
This is complete component coverage, not whole-project or full security proof.

Independent Black and standard Ruff passed all **18** parser/source/test/fixture files. Strict mypy
with `--disallow-any-explicit --python-version 3.11` and strict Pyright 3.11 with an explicit isolated
interpreter passed the same exact scope. Full repository-owned Ruff C901<=10/E902 and the canonical
fail-closed full-scope Complexipy<=5 launcher passed. Exact file lists, argv and zero exits are retained
in the owned static logs/config. The guard was not accepted by weakening types, complexity or scope.

I did not independently rerun the whole-project compatibility/services/installed/native scanner or
hosted profiles for this bounded correction. Root's exact committed 3.11 compatibility remains
separate. No author summary or pre-final test-only typing result was substituted for the exact-head
matrix, types or coverage reported here.

## Reused deep evidence and unchanged boundaries

I used CodeGraph before exploration, then reviewed a fresh owned `git archive` of the exact commit.
Per-file SHA256/byte comparisons confirm all eight runtime modules other than `_secret_json.py` are
unchanged from the previous exact review. I reused the preserved **12/12** real source/process groups,
**85/85** direct nominal controls and **58/58** expanded canonical/mode/path/kind family assertions
for those unchanged bytes, rather than claiming they were newly rerun or expanding the threat scope.
The current 288-control matrix also exercises their retained checked-in regressions.

That evidence retains full raw commit/root/nested-tree/blob OID-chain verification and direct types;
literal paths and tree/history/full-byte separation; complete canonical field/domain/digest identities;
exact nominal/primitive/mode validation; safe error/log/traceback handling; real native no-replacement,
no-lazy-fetch/no-transport environment; bounded streaming input/output, timeout/nonzero and child reap;
and aggregate limits. The selected executable, store and scope are still declarations, not authority.

Source-processing capacities remain 16MiB object/hash input, 64-byte simple metadata, 100,000 tree
entries, 256 path components, 10 seconds per command, 64MiB aggregate raw objects, 1,024 commands and
30 seconds per resolution. These do not replace worker OS/full-run isolation. Arbitrary Python with
memory/code replacement privileges is not made a protected caller by this validation layer.

## Evidence and preservation

New scratch is solely `work/security-source-review-byte-guard/`:

- `source-test-manifest.json`: exact candidate/base commits and trees, per-file SHA256/byte sizes for
  all nine source modules, nine focused test/fixture files, governance/contracts/preparation/lock/config,
  plus explicit eight-module unchanged checks. SHA256:
  `f9a6137e7920ffad3085539a761ad5c48f93df45a1edf4bcaaed232287cd7eb8`.
- `prior-raw-byte-probes.py`, `raw-byte-boundary-results.json`, log: the previous exact refutations
  rejected safely here. Result SHA256:
  `1cd22fa821bf43f935114fba363030274cd5f8d7a80ea3c1cabad6a69ccbabc9`.
- `byte-boundary-probes.py`, `byte-boundary-results.json`, log: independent 29/29 adjacent checks.
  Result SHA256 `49168071ef4aa8bd82b533089f0a032c6ed3bc51364a587f0562ec2ea867786b`.
- `coverage.ini`, `coverage-py311.json`, coverage data and three focused logs: exact matrix and
  zero-exclusion component coverage. Coverage JSON SHA256:
  `7686cb4121e7cca6da5c7429ae5ce47e277e56e2d935ebb9c4f6fdb252fd35be`.
- `component-static.log`, `static-files.txt`, `pyright-review.json`, `c901-full-scope.log`,
  `complexipy-full-scope.log`: independent static/strict/complexity commands and outcomes.
- `prior-evidence-preservation.json`, `review-evidence-manifest.json`: preservation checks and hashes
  for every new owned script/result/config/log.

All three earlier REVISE reports and their evidence, including historical reviewer harness mistakes
and retries, were verified byte-identical before and after this review:

| Prior report | Report SHA256 | Preserved evidence records |
| --- | --- | --- |
| Initial | `4b65c0b9be17fd07fcba9ed580e52b7a473e3a12599d18dd96b9d600bf0736f2` | 17 |
| First correction | `cb7299c886c90d3ebf5e63fc962ad10e18e97eacc8731587d849e0439dbc6f86` | 21 |
| Second correction | `e0204386d6b7969c8cbd38b8027eb1977f1fc75c26e52aeb44c0c9059e4d824f` | 27 |

The new review had no failed or relabeled harness attempts. No primary files, refs, config, access,
network/provider state or user repository was changed. Experiments used synthetic values, disposable
local repositories and bounded real child processes. No real native finding scan, credential or native
Secret/Match rendering was used. No child agent or other Codex chat was contacted.

**SHIP remains strictly limited to this exact SEC005 source-binding preparation.** Complete source
enrollment/export and scanner/report provenance, authentic human decisions, protected worker OS
sandbox, supported Linux Git provisioning and actual required hosting checks remain separate blocking
SEC006–015 obligations. Root final verification and task closure still follow this review.
