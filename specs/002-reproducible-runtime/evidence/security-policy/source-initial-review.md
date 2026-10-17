# SEC005 initial exact-source adversarial review

**Verdict: REVISE — SEC005 source-binding preparation only.**

Reviewed candidate `14902995b7656e31f788ba821218f56f5fabeb0e`, tree
`aeeac8a9ce561d81354e4aaacd1871938ccd6b76`, against base
`e793028e5ab097f2d09e373a9c8617ca8e3e3d37`, tree
`f0b5769211d34ff1563f59753cfb728078ff9b14`. This report is the immutable first-pass
review and does not evaluate any later correction. SEC005 must remain unchecked pending correction,
independent re-review and root verification. No full security, native worker, finding, governance,
hosting, merge or release acceptance is granted.

## Independence and source

I used the adversarial-review and complexity-budgets skills, attempted to refute the implementation,
and treated uncertainty as REVISE. I checked CodeGraph status and explored relevant source before
manual source exploration. I made separate owned `git archive` exports of the exact base/candidate
under `work/security-source-review/{base,candidate}`. I read repository AGENTS, constitution,
`native-source-binding.md`, `native-secret-policy.md`, `source-binding-preparation.md`, SEC tasks,
relevant research results and all seven focused test modules plus fixture. I wrote no primary source,
refs, config, remotes or access state.

All independent experiments use owned disposable local bare repositories or real bounded local
processes. No network endpoint, provider call, credential, user repository, live scanner or real native
Secret/Match rendering was involved. The local native Git was Apple Git 2.50.1 (Git-155); Python was
3.11.15. Capable supported Linux Git provisioning is a later worker dependency, not established here.

## Blocking findings

### R1 [P2] Missing slots escape the constant safe-error boundary and reveal active private context

Relevant locations: `_secret_schema.py:167`, `_secret_identity.py:48,83`,
`_secret_process.py:123`, `_secret_git.py:108`, `_secret_source.py:36`.

Exact-class checks do not ensure a slotted dataclass is initialized. Examples:

```python
validate_native_finding(object.__new__(NativeFinding))
validate_scope(object.__new__(SourceScope))
run_git(object.__new__(GitLocation), ())
bind_finding(valid_row, valid_scope, object.__new__(GitObjects))
object.__new__(FindingIdentityV1).canonical_bytes()
object.__new__(FindingIdentityV1).sha256
```

Each tested entrypoint raises `AttributeError` instead of `NativeReportError`. When called inside an
unrelated `RuntimeError` handler containing a synthetic private sentinel, the new exception has
`__suppress_context__ == False`, and `traceback.format_exception` includes the unrelated private
sentinel. Standard logging with `exc_info` also includes it for the finding/scope cases. The public
object-valued validators and source boundary explicitly promise safe validation of nominal metadata;
this is an unhandled malformed-state case in that boundary, not an executable/store authentication
claim.

Required correction: validate initialized fields at these declared public nominal boundaries and
translate malformed-state failures into a constant domain category with `from None`. Add real
regressions inside an active unrelated private handler, checking type, category, suppression and both
formatted traceback/log output. Direct canonical identity entrypoints should validate the complete
v1 data before serialization; they must not assume successful construction occurred.

Evidence: `security-source-review/independent-probes.json` group
`forged_nominal_missing_slots`; `additional-forgery-probes.json`; `identity-forgery-probes.json`.

### R2 [P2] Primitive subclasses can defeat exact literal path and retained-history semantics

Relevant locations: `_secret_schema.py:43,148,160`, `_secret_identity.py:15,26`,
`_secret_git.py:108`, `_secret_source.py:17,36`.

`_text` accepts `isinstance(value, str)` and returns the original object. A manually constructed exact
`NativeFinding` can therefore carry a `str` subclass with different method behavior:

```python
class RedirectPath(str):
    def split(self, sep=None, maxsplit=-1):
        return ["note.py"]

row = replace(valid_row,
    path=RedirectPath("absent.py"),
    native_fingerprint=f"{head}:absent.py:rule:1")
```

In an actual owned repository containing only `note.py`, `bind_finding` accepts this row, resolves
`note.py`, and emits canonical JSON claiming `"path":"absent.py"` with `note.py`'s OID and full
SHA256. Native `cat-file -e <head>:absent.py` confirms the claimed file does not exist.

Likewise `_retained` accepts a `frozenset` subclass overriding `__contains__` to always return True.
A `SourceScope` whose underlying set contains only HEAD then accepts a real historical commit outside
that underlying declared set; the returned identity binds that outside commit. The nominal scope
class is exact, but its admitted primitive does not provide the declared immutable membership
semantics. Tuple and text subclasses expose analogous method/iteration drift surfaces.

Required correction: reject non-exact immutable primitives at the direct source metadata/scope
boundary (in particular strings, tuples and frozensets), rather than retaining subclass instances.
Cover literal path and scope membership counterexamples against real native repositories. Preserve
the already qualified byte-report parser's normal behavior. Do not treat a rejected subclass as
proof of execution or origin authenticity.

Evidence: `security-source-review/independent-probes.json` group `forged_primitive_path`;
`additional-forgery-probes.json` retained-membership result.

## Additional boundary observations

- An object deliberately mutated with `object.__setattr__(identity, "schema_version", 7)` serializes
  as schema 7 without revalidation. This supports revalidating the direct canonical entrypoint,
  including schema/version and exact field inventory. Frozen dataclasses are data and cannot
  authenticate an arbitrary Python caller; no such authentication is demanded.
- Manually constructing an invalid `NativeFinding` with private text in `mode` or a coordinate
  exposes that text in its autogenerated repr. Those states cannot be produced by validated parser
  or binding output, whose repr tests passed. This is a deliberately malformed in-process object,
  not evidence that ordinary native reports leak. The report distinguishes it from R1's explicit
  validator failures. If all nominal objects are intended to be safe before validation, the raw
  model repr needs a fixed safe representation too.

## Independent positive and negative evidence

The independent script records 12 assertion groups: 10 passed, while the two groups above failed
as intended to expose the defects. Supplemental scripts extend those same findings.

| Area | Independent assertions and result |
| --- | --- |
| Raw OID chain | Actual commit/root-tree/nested-tree/blob bodies overwritten under their old OIDs; every corruption rejected with exact `native object digest mismatch`. Nested-tree mapping changed to a different valid blob. |
| Direct types | Annotated tag/tree/blob in commit position, blob in tree position, and tree in regular-file blob position rejected before identity. |
| Literal paths | Dash, pathspec-like colon/glob, semicolon/brackets, NFC and NFD names bound literally and distinctly for ordinary exact strings. Unrelated non-UTF8 tree sibling accepted without normalization. |
| Tree/history separation | Missing/changed/newline/trailing tree bytes rejected; full correct bytes accepted. Historical old commit resolved its old body, absent membership rejected, supplied tree bytes rejected. Identical blob in a later head has a different tree identity. |
| Raw trees | Complete consumption, invalid/padded modes, malformed framing, dot/dotdot/slash names, duplicates, incorrect order and Git directory ordering exercised. |
| Canonical identity | Independently constructed exact 18-key sorted compact UTF-8 JSON matched bytes; exact `polymarket-native-finding-v1\0` domain SHA256 matched; ten independent field changes changed digests; no acceptance property. |
| Privacy | Valid restricted finding/identity reprs omit private text; ordinary malformed exact primitive failures suppress an unrelated active private exception. Missing-slot exceptions fail as R1. |
| Native guards | Real replacement commit ignored. Missing promisor blob not fetched under guarded calls. Deliberately unguarded local-only positive control fetched from owned origin and then bound correctly. Guarded explicit local fetch rejected. |
| Environment/argv | Exact no-replace/no-lazy/global Git argv guards observed in real child. Hostile Git/config/alternate/helper/trace/loader/HOME/credential variables did not reach it; protocol deny, devnull config/grafts and no prompts confirmed. Python/macOS inserted its own `__CF_USER_TEXT_ENCODING`; no candidate variable reached the child. |
| Streaming/process | Exact 0/64/4096-byte stdout caps accepted; endless output rejected while streaming; exact 16MiB input accepted and +1 rejected; silent and stdout-EOF hangs timed out; stderr flood/nonzero rejected. Actual child PIDs were absent after return, proving kill/reap for tested children. |
| Aggregate budgets | One-file resolution accepted exact 206 raw bytes and 10 commands; one less rejected. Zero total deadline rejected; 257 path components rejected; reduced actual tree-entry cap exact/+1 exercised. |

The checked-in controls independently passed **224/224** under isolated locked Python 3.11:

```text
uv run --isolated --locked --all-extras --python 3.11 pytest -q \
  tests/tooling/test_secret_report.py tests/tooling/test_secret_schema.py \
  tests/tooling/test_secret_git.py tests/tooling/test_secret_source.py \
  tests/tooling/test_secret_identity.py tests/tooling/test_secret_tree.py \
  tests/tooling/test_secret_process.py
224 passed in 10.16s
```

This includes all 121 prior parser controls and 103 new controls, but misses R1/R2. Independent
Black and standard Ruff passed for the 12 changed source/fixture/test paths. Independent strict mypy
3.11 and strict Pyright 3.11 passed those 12 files. Full repository-owned Ruff C901<=10/E902 and the
canonical fail-closed full-scope Complexipy<=5 launcher also passed. These checks constrain code
quality; they do not refute the semantic counterexamples.

Exact capacities read from the candidate are: object/hash input 16MiB; simple metadata 64 bytes;
100,000 tree entries; 256 path components; 10 seconds per command; 64MiB aggregate raw objects;
1,024 commands; 30 seconds aggregate resolution. Exhaustion returned no partial identity in the tested
paths. Process children are not an OS sandbox; grandchild/resource/worker isolation remains later work.

Commands/config for independent static evidence:

```text
black --check <the 12 source/fixture/test files listed in pyright-review.json>
ruff check <those same 12 files>
mypy --strict --python-version 3.11 <those same 12 files>
pyright --project pyright-review.json --pythonpath <the isolated 3.11 interpreter>
ruff check src tests scripts alembic conftest.py --select C901,E902 \
  --config lint.mccabe.max-complexity=10
python scripts/complexipy_gate.py src tests scripts alembic conftest.py \
  --max-complexity-allowed 5 --no-ignore --ignore-complexity=false \
  --snapshot-ignore=true --snapshot-create=false --exclude=. --check-script=true
```

All commands ran through `uv run --isolated --locked --all-extras --python 3.11` using the clean
candidate archive/lock. An initial reviewer-authored Pyright config used absolute `include` paths,
which Pyright ignored and consequently scanned the entire review workspace. That invalid harness run
is retained separately as `pyright-invalid-config.log` and is not candidate evidence. Corrected relative
includes and an explicit isolated interpreter produced the scoped zero-error result without exclusions
or weakened strictness.

I did not rerun full supported-minor compatibility, service, scanner, installed/hosted or Linux gates
for this first-pass component review. Root is independently running committed-source compatibility.
The author's 510-statement/160-branch coverage claim was not used as proof or independently asserted
here. No child-agent delegation was used for this independent grading task.

## Exact evidence and retained obligations

All scratch/evidence below is under owned `work/security-source-review/`:

- `source-test-manifest.json`: per-file byte counts and SHA256 for all nine parser/source modules,
  all eight focused test/fixture files, governance, contracts, preparation note, pyproject and lock.
  It confirms the three prior parser modules/two prior test files are byte-identical to base and
  the prior schema bytes remain an unchanged prefix. SHA256:
  `5633f342b05a7889f05b1e3970cd5f6598d6eba4bd5c35965aae7b1862df042e`.
- `probes.py`: independent executable assertions; SHA256
  `cec87df491df5371e4d37a0351277f0df1ab62d36daa4c66c0764a01484e7145`.
- `independent-probes.json`: measured group results; SHA256
  `edb1bb38eec6d5e1b227cd072bad902aecfa2591c670e1350d6183b1b6dc3a62`.
- `additional-forgery-probes.{py,json}` and `identity-forgery-probes.{py,json}`:
  retained scope/location/reader/canonical and malformed repr observations.
- `focused-py311.log`, `black.log`, `ruff-component.log`, `c901-full-scope.log`,
  `mypy-component.log`, `pyright-component.log`, `pyright-review.json`,
  `complexipy-full-scope.log`: independent test and static evidence.
- `review-evidence-manifest.json`: SHA256/byte counts for every retained script/result/config/log,
  including the invalid reviewer-config attempt.

The executable/store/scope choices are declarations, not authority. Candidate-owned executables or
Git configuration, complete enrollment/export provenance, scanner execution/report provenance,
authentic human decisions, protected worker OS sandbox, supported Linux Git provisioning and required
hosting checks remain separate SEC006–015 obligations. None is approved by this review, even if R1/R2
are corrected. The tested raw-object, literal-string, digest, native guard and resource controls appear
sound within their stated measurement role; the missing safe-state and primitive semantics prevent
shipping this initial component preparation.
