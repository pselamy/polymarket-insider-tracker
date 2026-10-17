# SEC005 second correction adversarial review

**Verdict: REVISE — SEC005 source-binding preparation only, at exact `fb5c792d18e0d9a3e084ce858995589e12534020`.**

Candidate tree: `c989b0e9284f139a9cce536716fff16b3936872f`.
Rejected base: `351405c7c586a74e02fc6a4db8d36e22aaae85e8`, tree
`3fea0959ce0cc2ba7062b98d82c8e7c1f14d877a`.

The original source/nominal identity counterexamples now reject correctly. One adjacent strict raw-byte
parser counterexample, identified during root verification and independently reproduced here, remains
blocking. The requested filename does not signify final acceptance: this exact commit remains REVISE.
SEC005 stays unchecked until correction, exact re-review and root verification. No full security,
worker/authentication, native scan, finding, governance, Linux provisioning, hosting, merge or release
acceptance is granted.

## Blocking finding: nominal byte subclasses can replace the raw report before JSON decoding

**D1 [P2]** — `scripts/_secret_json.py:27-34`, consumed by
`scripts/_secret_report.py:36-42`.

`decode_array` calls `len(payload)` and `payload.decode` without first requiring exact bytes. A byte
subclass therefore changes the parsed text while `parse_report` hashes the original underlying raw
bytes:

```python
class RedirectBytes(bytes):
    def decode(self, *args, **kwargs):
        return "[]"

parse_report(RedirectBytes(b"not-json"), ScanMode.TREE, 0)
parse_report(RedirectBytes(b"\xff"), ScanMode.TREE, 0)
```

Both calls returned an accepted zero-row NativeReport. The digest equals SHA256 of the invalid
original raw bytes in each case. The original bytes are neither the UTF-8 JSON array that was parsed
nor a valid strict report. This violates the declared pure parser's strict raw UTF-8 JSON boundary.
The lower-level module is unchanged from the prior parser baseline, but the combined preparation
cannot claim that boundary sound while this counterexample remains.

A second subclass with `decode` raising `RuntimeError("RAW_BYTES_SYNTHETIC_PRIVATE")` also escaped as
an unsuppressed RuntimeError. Both that synthetic private value and an unrelated active caller's
private sentinel appeared in formatted traceback and standard exception logging.

Required correction: reject anything other than exact immutable bytes before `len` or `decode`, with
a constant safe NativeReportError and suppressed context. Add raw invalid-JSON/invalid-UTF8 redirect,
mutable input and raising-method tripwire controls inside an active unrelated private handler. The
subclass decode method must never run. Preserve normal qualified byte-report behavior and all native
controls. This is a nominal input/data consistency issue, not a claim of authenticated scanner
execution or exploitation through actual protected process stdout.

Independent evidence: `work/security-source-review-final/raw-byte-boundary-probes.py`,
`raw-byte-boundary-results.json` and log. The result contains two accepted-invalid-byte cases and the
unsafe exception/log case. Result SHA256:
`2fdc2bf5b8c690c8d3f88e609d19a4d6d2aae61f6a2b919ac661b97162413b47`.

## Independently closed counterexamples

I read the exact correction through CodeGraph after checking its index status, reviewed the source,
contract and added tests, and tested a separate owned `git archive` export. The expanded family harness
passed **58/58**, with zero overridden validator/canonical/foreign-attribute methods invoked:

- Exact module-level identity validation rejects subclass and foreign nominal inputs before their
  behavior can run. Explicit base construction/canonical/hash calls and inherited hash reject every
  tested post-init, `_validate` and canonical override. Base hashing anchors base canonical bytes.
- All forged enum bodies (`dir`, `git`, arbitrary and empty) reject at direct metadata, report,
  identity-construction, canonical and hash boundaries. The two actual singleton modes still parse.
- Entirely uninitialized, partially absolute, non-string cached-render and invalid-parts native Path
  states reject safely. Exact supported commit/tree/blob object kinds read the correct native raw
  bodies; bool/None/unsupported strings/text subclasses/bytes/int/list kinds reject safely.
- Every negative above checks domain failure, bounded category, suppressed context and both formatted
  traceback and standard logging while an unrelated private exception is active.

The earlier independent direct-boundary harness passed **85/85**: missing and partially initialized
finding/scope/location/reader/identity slots, direct digest revalidation, exact string/tuple/frozenset
metadata, native Path subclass rejection, process argument/bytes/capacity/deadline validation and fixed
raw finding repr. Native Path subclass method invocation count was zero.

The independent source/process regression harness passed **12/12 groups**: raw complete commit,
root-tree, nested-tree and blob corruption; direct native types; literal ordinary NFC/NFD/pathspec-like
paths; exact tree-export/history/head binding; strict raw tree framing/order; complete 18-field
canonical JSON/domain/digest; valid restricted repr; actual native replacement/lazy-fetch/transport
guards; hostile inherited environment; exact bounded input/stdout, streaming overflow, timeout,
nonzero/stderr and actual child reap; aggregate bytes/commands/time/depth/entry limits. None produced
an acceptance flag or partial identity on exhaustion.

The measured limits remain 16MiB object/hash input, 64-byte simple metadata, 100,000 tree entries,
256 path components, 10 seconds per command, 64MiB total raw objects, 1,024 commands and 30 seconds per
resolution. Resolution budgets bound this layer and do not establish an OS worker sandbox.

## Independent exact-source verification

Checked-in focused controls passed at this exact archive on every supported Python minor:

| Interpreter | Result | Measured focused time |
| --- | --- | --- |
| Python 3.11.15 | 283 passed | 13.31s |
| Python 3.12.13 | 283 passed | 14.52s |
| Python 3.13.14 | 283 passed | 13.97s |

Each used the candidate lock and isolated all-extras environment:

```text
uv run --isolated --locked --all-extras --python <minor> pytest -q \
  tests/tooling/test_secret_report.py tests/tooling/test_secret_schema.py \
  tests/tooling/test_secret_git.py tests/tooling/test_secret_source.py \
  tests/tooling/test_secret_identity.py tests/tooling/test_secret_tree.py \
  tests/tooling/test_secret_process.py tests/tooling/test_secret_source_boundaries.py
```

Independent Black and standard Ruff passed the 15 exact source/fixture/test files listed in
`static-files.txt`. Strict mypy with `--disallow-any-explicit --python-version 3.11` and strict Pyright
3.11 with an explicit isolated interpreter passed the same scope. Full repository-owned Ruff
C901<=10/E902 and the canonical fail-closed full-scope Complexipy<=5 launcher passed. Exact command
argv and zero exits are retained in `component-static.log`, `c901-full-scope.log` and
`complexipy-full-scope.log`. These checks and green native controls do not resolve D1.

The original byte-parser schema prefix remains byte-identical; the constant NativeFinding repr is
unchanged from the first correction. All eight existing focused test/fixture files are unchanged;
the boundary file retains the additional controls. The report's shared mode guard intentionally changes
to singleton validation. Per-file hashes verify these facts.

I did not independently run whole-project compatibility, services, installed behavior, live scanner,
Linux native Git provisioning or hosting profiles here. Root is separately verifying committed-source
compatibility. Author-reported coverage and other gate summaries are not used as proof in this report.

## Reviewer harness corrections and preservation

The first source/process rerun recorded 11/12 groups because its synthetic 0.3-second child timeout
expired under concurrent load before the child's PID marker existed. The production boundary returned
a timeout; the reviewer harness could not establish its PID assertion. The retry raised only this
synthetic test allowance to a bounded 1.0 seconds (3-second upper elapsed assertion), then passed 12/12
including actual missing child PIDs after termination. This does not change production limits.

The first new family harness also incorrectly treated the fixture's native Git return value as a
CompletedProcess instead of bytes and aborted before final output. That reviewer error was corrected
and the completed 58/58 result verified. Both first-attempt scripts/logs/results are preserved with
`-first-attempt` names. No candidate defect or successful result is inferred from either invalid
reviewer attempt. My premature source/process progress statement was corrected to root before verdict.

The two prior reports and all prior evidence were checked before and after this review and remain
byte-identical:

- Initial report SHA256:
  `4b65c0b9be17fd07fcba9ed580e52b7a473e3a12599d18dd96b9d600bf0736f2`; all 17 initial evidence records match.
- First-correction report SHA256:
  `cb7299c886c90d3ebf5e63fc962ad10e18e97eacc8731587d849e0439dbc6f86`; all 21 first-correction evidence records match.

New scratch is solely `work/security-source-review-final/`:

- `source-test-manifest.json`: exact commit/tree and per-file SHA256/byte sizes for nine source
  modules, nine focused test/fixture files, governance/contracts/preparation/lock/config and preserved
  prefix/test/model assertions. SHA256:
  `dee92310c05bb5b902406e121d2b3f5403d33c0b9e3b0d4c13b44bfd19580966`.
- `source-regression-probes.py`, `independent-probes.json`, log: corrected source/process assertions.
  Result SHA256 `6add4cc40724542785d32648aade194276dfabf3500aebc33bc480e1400f9ab4`.
- `boundary-probes.py`, `boundary-results.json`, log: 85 direct boundary controls. Result SHA256
  `f7eee49937dc79c017dd589f33f1f9b4a7113ddbf314ab9d3329d3ae10c6590c`.
- `canonical-family-probes.py`, `canonical-family-results.json`, log: 58 expanded family controls.
  Result SHA256 `6392dd464ae0e83c38ff1a33fe201d7921f310d41011854de1e31f05e53cab1c`.
- `raw-byte-boundary-probes.py`, `raw-byte-boundary-results.json`, log: D1 counterexample.
- Three focused-minor logs, exact static config/commands/logs, retained reviewer first attempts,
  `prior-evidence-preservation.json` and `review-evidence-manifest.json`: complete evidence trail.

No primary files, refs, config or access state were changed. All experiments used synthetic values,
owned disposable bare repositories or local bounded processes; no network endpoint, provider,
credential, user repository, real scanner or native Secret/Match rendering was involved. No delegation
to another agent or message to another Codex chat was used.

The executable/store/scope remain declarations. Complete enrollment/export and scanner/report
provenance, authentic human decisions, OS worker sandbox, supported Linux Git and required hosted
checks remain SEC006–015 obligations. The tested source layer has survived the prior concrete
refutations; the combined preparation's raw-byte contract remains refuted until D1 is corrected and
independently re-reviewed at its next exact commit.
