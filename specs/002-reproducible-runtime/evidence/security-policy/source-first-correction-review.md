# SEC005 first correction adversarial review

**Verdict: REVISE — SEC005 source-binding preparation only.**

Exact candidate: `351405c7c586a74e02fc6a4db8d36e22aaae85e8`, tree
`3fea0959ce0cc2ba7062b98d82c8e7c1f14d877a`.
Rejected base: `14902995b7656e31f788ba821218f56f5fabeb0e`, tree
`aeeac8a9ce561d81354e4aaacd1871938ccd6b76`.

This is the immutable review of the first correction, not of a later commit. The original R1/R2
examples are corrected, but the additional counterexamples below prevent accepting the full
boundary correction. SEC005 remains unchecked pending correction, re-review and root verification.
No full security, worker, finding, governance, Linux provisioning, hosting, merge or release acceptance
is granted.

## Independence and preservation

I used the adversarial-review and complexity-budgets skills, checked CodeGraph status and explored
the correction before manual diff/test inspection. I exported a separate clean archive of the exact
candidate under `work/security-source-review-corrected/candidate`; all probes and logs are owned new
scratch under `work/security-source-review-corrected/`. No primary source, refs, config, access or
remote state was modified. No provider, credential, user repository, real scan or network effect was
used. Native Git remains Apple 2.50.1 (Git-155); all executions here used Python 3.11.15.

The initial report still has SHA256
`4b65c0b9be17fd07fcba9ed580e52b7a473e3a12599d18dd96b9d600bf0736f2`.
All 17 initial evidence-manifest records were checked before and after this review and remain
byte-identical. `initial-evidence-preservation.json` records those checks. The original byte-report
schema prefix is unchanged; the model diff adds only a constant NativeFinding repr. All eight prior
focused test/fixture files remain byte-identical; the 42-case boundary file is additional.

## Blocking counterexamples

### C1 [P2] Canonical validation and hashing use overridable instance methods

Locations: `scripts/_secret_identity.py:82-104,132-136`.

`canonical_bytes` calls `self.__post_init__()`, which calls `self._validate()`. A subclass can override
either method with `pass` and inherit the base serialization implementation. Both variants accepted
`repository_id=True`, invalid policy/blob digests and `schema_version=7`, then returned canonical
bytes and a SHA256. An explicit base `FindingIdentityV1.__post_init__` call on an uninitialized
subclass overriding `_validate` also returned without rejecting it.

```python
class BypassPostInit(FindingIdentityV1):
    def __post_init__(self):
        pass

identity = BypassPostInit(True, "invalid-policy", valid_row, head,
                         "invalid-oid", "invalid-digest")
object.__setattr__(identity, "schema_version", 7)
identity.canonical_bytes()  # accepted by inherited base implementation
identity.sha256            # also available
```

The inherited SHA256 property separately calls `self.canonical_bytes()` virtually. A subclass
returning `b"forged-noncanonical-payload"` from that method caused the inherited property to return
exactly SHA256 of the normal domain prefix plus those arbitrary bytes, bypassing field serialization.

Required correction: use a non-virtual exact-class validator at construction, base canonical and base
hash entrypoints, and anchor the serialization used by the base hash property. Add regressions for
both post-init/validator overrides, inherited hash with canonical override, and explicit base calls.
This does not promise to constrain an entirely overridden arbitrary Python method or authenticate a
caller; it ensures that the base implementation validates the nominal input it actually processes.

Evidence: `boundary-results.json` two failed identity-subclass cases;
`adjacent-entrypoint-results.json` inherited-digest and explicit-base-init cases.

### C2 [P2] A forged ScanMode instance passes validation and later leaks private exception context

Location: `scripts/_secret_schema.py:161-164`; canonical access at
`scripts/_secret_identity.py:110`.

The mode guard still accepts `isinstance(value, ScanMode)`. This admits an uninitialized enum value:

```python
fake_mode = str.__new__(ScanMode, "dir")
row = replace(valid_tree_row, mode=fake_mode)
validate_native_finding(row)  # returns row
identity = FindingIdentityV1(17, policy_digest, row, head, blob_oid, blob_digest)
identity.canonical_bytes()   # AttributeError: enum .value is absent
```

The same result occurred for forged values containing `"git"` and `"private"`. All three lack
`_value_`, are accepted as a mode, then fail on `.value` with `__suppress_context__ == False`.
Formatted traceback includes an unrelated active private exception sentinel.

The byte parser also accepted `parse_report(b"[]", fake_mode, 0)`; rendering the returned report then
raised unsuppressed AttributeError. The parser's normal byte behavior remains unchanged, but its
mode argument needs the same qualified singleton check as direct metadata.

Required correction: require identity with exactly the two real enum singletons at both public
boundaries, before returning models or performing mode-dependent semantics. Add active-context
traceback/log regressions for forged enum values through direct metadata, identity and parser.

Evidence: `enum-path-results.json` three forged-enum results; `enum-parser-result.json`.
This is a nominal-value counterexample, not a claim that ordinary parsed JSON creates enum objects.

### C3 [P2] A partially initialized native Path passes location validation, then fails unsafely at use

Locations: `scripts/_secret_process.py:22-49,144-195`.

The correction safely rejects an entirely uninitialized native Path and does not call subclass
methods. However, an exact native PosixPath with only the fields needed by `is_absolute()` passes:

```python
p = object.__new__(PosixPath)
object.__setattr__(p, "_root", "/")
object.__setattr__(p, "_drv", "")  # deliberately leave _parts unset on Python 3.11
location = GitLocation(p, Path("/owned"))  # accepted
validate_location(location)              # accepted
run_git(location, ())                    # AttributeError from str(p)
```

`str(p)` is evaluated after validation, outside an AttributeError translation boundary. No child is
spawned, but the exception is unsuppressed and the active unrelated private sentinel appears in its
formatted traceback. This uses no custom Path subclass, overridden method or arbitrary executable.
It is the same partial-slot-state lens applied to the now explicitly validated native Path input.

Required correction: establish safe renderability of native location values during validation or
translate malformed native-path state at its use boundary. Do not turn location validation into a
claim of binary/store ownership, authenticity or executable provenance. Add a supported-version-safe
partial-native-state regression; the particular internal fields above are Python 3.11-specific.

Evidence: `partial-path-probe.py`, `partial-path-result.json`.

### C4 [P2] The public raw-object reader does not validate its declared literal object kind

Location: `scripts/_secret_git.py:95-107`.

`GitObjects.read(head, True, ResolutionBudget())` reaches `kind.encode()` after a real native object
type read and raises unsuppressed AttributeError. The formatted traceback includes an unrelated
active private exception. The enum-like `ObjectType` annotation has no runtime enforcement.

Required correction: require an exact supported literal kind before native use, with a safe constant
failure for unsupported types/values. The actual `resolve` path constructs its own accountant and
uses supported constant kinds; I found no ordinary bind-path failure here. Arbitrary forged
ResolutionBudget internals are deliberately outside this source/auth metadata review and were not
promoted into a requirement.

Evidence: `adjacent-entrypoint-results.json` native-read-invalid-kind case.

## What the correction does establish

- The original absent-slot finding/scope/location/reader/identity examples now return safe domain
  failures with suppressed private active context. Per-field partial-slot checks for those five
  models also passed; uninitialized identity hashing passed.
- The original redirected string-path and permissive frozenset-membership attacks are rejected.
  Exact strings, tuples/frozensets and their contained text are enforced at source boundaries.
- Direct identity schema/version, repository ID, digest fields, nested finding, canonical and hash
  revalidation reject the tested malformed exact-class cases.
- Native Path subclass methods were invoked **zero** times. Completely uninitialized native Path
  rejects safely; C3 is the narrower partially initialized case.
- The raw malformed/uninitialized NativeFinding repr is a fixed restricted string, including private
  text deliberately inserted into invalid mode/coordinate slots.
- Malformed process argv, tuple/text subclasses, bytes/bytearray/memoryview/subclasses, bool/zero/
  excessive/subclass capacity and invalid/nonfinite/bool/subclass/huge/expired deadlines fail safely.

The independent corrective-boundary harness recorded **83 passing checks and 2 failing checks**
(the two canonical override variants). Subsequent targeted probes reproduced C1's hash/base-call
variants plus C2–C4; these are additional evidence, not folded into an invented aggregate pass count.

All **12 independent raw-source/process regression groups passed** again: complete commit/root-tree/
nested-tree/blob corruption and direct-type rejection; literal ordinary NFC/NFD and pathspec-like
paths; exact export drift/history/head semantics; strict raw tree framing/order; complete canonical
field/domain identities; safe valid repr; actual native no-replace/no-lazy/no-transport behavior;
fixed environment; exact input/stdout caps, streaming overflow, timeout/nonzero and actual PID reap;
aggregate bytes/commands/path/entry/deadline limits. The copied independent script changes only its
commit labels and a zero-output probe to use positive capacity 1, matching the new explicit capacity
contract. It does not change the tested production code.

## Independent verification and evidence

The exact corrective clean archive passed **266/266** focused checked-in controls in **10.13s**:

```text
uv run --isolated --locked --all-extras --python 3.11 pytest -q \
  tests/tooling/test_secret_report.py tests/tooling/test_secret_schema.py \
  tests/tooling/test_secret_git.py tests/tooling/test_secret_source.py \
  tests/tooling/test_secret_identity.py tests/tooling/test_secret_tree.py \
  tests/tooling/test_secret_process.py tests/tooling/test_secret_source_boundaries.py
```

Independent Black/standard Ruff, strict mypy with `--disallow-any-explicit --python-version 3.11`,
and strict Pyright 3.11 all passed the exact 14 source/fixture/test paths listed in
`static-files.txt` and `pyright-review.json`. Full owned scope Ruff C901<=10/E902 and the canonical
fail-closed full-scope Complexipy<=5 launcher passed. Commands used the candidate's locked isolated
3.11 environment; `component-static.log` records exact argv and zero exits, including the explicit
isolated Pyright interpreter. These quality results do not resolve the behavioral counterexamples.

No author coverage, supported-minor/full compatibility, service, installed/scanner or hosted result
is accepted on trust. I did not independently rerun those broader profiles for this review; root's
committed-source compatibility is separate. The author-reported no-exclusion 587/184 coverage was not
substituted for the failing controls above.

All new evidence is in `work/security-source-review-corrected/`:

- `source-test-manifest.json`: exact commit/tree, per-file byte counts/SHA256 for all nine source
  modules, all nine focused test/fixture files, governance/contracts/preparation/lock/config, plus
  unchanged prior test/schema/model assertions. SHA256:
  `ade9c4467ae84f0dab86ac7bf59dee74ec64c863d9034525585f807179c366d0`.
- `source-regression-probes.py`, `independent-probes.json`, `source-regression-probes.log`: 12/12
  independent source/process groups. Result SHA256:
  `bd5477ee082d84169f2b322d813b9cbe2984596fb497fd52a5bab94693030a70`.
- `boundary-probes.py`, `boundary-results.json`, `boundary-probes.log`: 85 direct boundary checks.
  Script SHA256 `5357d26718d29cee94be7fad6db830f6d9697819ccb5c4986e537ec12b0057bd`;
  result SHA256 `588c00bfbc6b43fe510e99b6f2c1bee39a8bd1e5fb5fe7f14b14475c71d5ec5d`.
- `adjacent-entrypoint-probes.py`/`adjacent-entrypoint-results.json`,
  `enum-path-probes.py`/`enum-path-results.json`, `enum-parser-result.json`,
  `partial-path-probe.py`/`partial-path-result.json`: exact supplemental counterexamples.
- `focused-py311.log`, `component-static.log`, `c901-full-scope.log`,
  `complexipy-full-scope.log`, `static-files.txt`, `pyright-review.json`: independent verification.
- `initial-evidence-preservation.json` and `review-evidence-manifest.json`: initial preservation
  checks and hashes for every new owned script/result/config/log.

The chosen executable, store and scope remain declarations. Complete enrollment/export provenance,
scanner execution/report provenance, authentic human decisions, worker OS isolation, supported Linux
Git provisioning and actual required hosting checks remain SEC006–015 work. None is accepted here.
A later correction needs its own exact-artifact review; this report stays REVISE for 351405c.
