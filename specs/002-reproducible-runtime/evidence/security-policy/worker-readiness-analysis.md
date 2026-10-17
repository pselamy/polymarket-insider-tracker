# Specification Analysis Report — SEC006 native-worker readiness

**Scoped verdicts:**

- **SHIP** — bounded archive/executable/config byte-measurement preparation, and the honest `NOT_READY` documentation checkpoint. These activities are already authorized preparation; no new permission is needed to prepare a concrete correction for review.
- **REVISE** — full-worker design readiness using unmodified Gitleaks 8.30.1 plus a trace-diagnostic veto and before/after manifests. This mechanism does not establish complete native processing. SEC006 must remain unchecked.

Reviewed 2026-10-17 13:48 UTC at repository HEAD `75d133296a3da63704fe67dda8c3640abf548401`, with the new worker research/readiness contract and append-only plan/research/tasks changes. SEC005's accepted source-layer checkpoint is retained. No worker implementation exists or is accepted by this report. No finding, amendment activation, protected artifact substitution, access change, required hosted check or merge is approved.

## Findings, ordered by consequence

These findings concern the proposed completion mechanism, not a claim that the newly written readiness checkpoint conceals them. The checkpoint correctly records a blocker and prohibits treating measurements as SEC006 completion.

| ID | Category | Severity | Exact source / requirement | Finding | Smallest correction |
| --- | --- | --- | --- | --- | --- |
| W1 | Incomplete native source processing | HIGH | Pinned Gitleaks `sources/files.go:151–162`; FR-023/026, SC-012/013; readiness contract:67–76 | The native trace event is emitted **before** `os.Open`. A non-permission open failure then returns nil without a failure diagnostic. Input bytes can remain identical before and afterward while this particular read failed. A log grammar accepting the ordinary attempt event cannot distinguish complete scanning from this failure. | Propagate every enrolled-file open failure as a run error, through the asynchronous source group and detector to the CLI. Do not merely add another log pattern. |
| W2 | Silent asynchronous history-parser failure | HIGH | `go-gitdiff v0.9.1`, `gitdiff/parser.go:18–70`, especially :35–45 and :52–58; Gitleaks `sources/git.go:114–124`; FR-023/026, SC-012/013 | `Parse(io.Reader)` returns a file channel and an error only from initial parser setup. Its goroutine subsequently advances past non-EOF header errors, discards patch-header errors, or returns on text/binary parsing error; channel closure carries no final error. Gitleaks sees channel exhaustion, not a proof that every required patch was parsed. No diagnostic veto can observe a diagnostic that is never emitted. | Add a strict, completion/error-reporting API to the pinned native parser dependency and consume its terminal status in Gitleaks. Retain ordinary native parsing/detectors; do not add an application fallback scanner. |
| W3 | Errors do not reach the native final result | HIGH | Gitleaks `sources/files.go:173–189`, `sources/git.go:299–304,361–429`, `detect/detect.go:228–232,266`; `semgroup v1.2.0`, `semgroup.go:48–77`; FR-026, SC-012/013 | Asynchronous source errors accumulate in `semgroup.Group`, but the source paths use a separate `sync.WaitGroup.Wait`; they do not collect the group's error result. The detector callback also logs fragment errors and returns nil. Git child `Wait` errors are debug-only. Trace can veto some observed cases, but native completion still is not a propagated invariant. | Join source-task errors, parser/stream errors, cancellation and child wait status into the error returned to the existing CLI summary/exit path. Reap/stop all children without hanging on an abandoned pipe. Preserve a fatal result even when findings exist. |

**No CRITICAL constitutional violation is asserted:** the current documents do not claim the incomplete mechanism works and no gate is activated. There are 3 HIGH unresolved full-worker design findings, no MEDIUM/LOW finding, and no blocker to the limited measurement work.

Pinned source references: [directory source](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/sources/files.go#L141-L190), [Git source](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/sources/git.go#L298-L430), [detector source boundary](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/detect/detect.go#L207-L266), [asynchronous diff parser](https://github.com/gitleaks/go-gitdiff/blob/7765724e69ab7954ccf71b4d45ff5d82446e952e/gitdiff/parser.go#L18-L70), [semaphore-group error collection](https://github.com/fatih/semgroup/blob/de404588ab3d4b4ca6f8afba200b5b74994879f8/semgroup.go#L48-L77).

## Why manifests and trace cannot supply the missing proof

A complete immutable manifest proves the expected source existed and retained its bytes. A fresh report hash proves which report bytes were obtained. A successful outer process exit proves that process returned successfully under its current error handling. None proves the scanner completed each enrolled read, parsed every native history fragment, or completed all detector work.

W1 supplies a direct indistinguishability case: preflight succeeds, the native open fails with a transient non-permission error, and a later manifest read succeeds. There need be no file mutation. The initial trace event exists in both the successful and failed attempt. W2 supplies a separate history-layer loss channel. The research's observed permission/open/Git-child failures establish useful negative controls, but are not exhaustive proof against these silent paths.

The printed aggregate byte/commit summaries are not an alternative completion protocol. Byte accounting happens before the detector's rule loop, and cancellation can leave partially inspected work; the commit count expressly excludes some no-addition commits. Even a newly designed byte equality check would need separate qualification of scheduling, duplicates, cancellation, native parsing and exact source coverage. No such proof exists in the proposed design. A richer qualified native completion protocol could be an alternative, but trace matching plus unchanged inputs is not one.

Keep the diagnostic veto as defense in depth. Raw trace remains restricted evidence: the console logger and native trace fields can contain arbitrary candidate text despite report redaction. Do not make ordinary error rendering or a secret-bearing log grammar the sole authority for completeness.

## Smallest concrete native correction boundary

Prepare a narrow patch against Gitleaks commit `83d9cd684c87d95d656c1458ef04895a7f1cbd8e`, retaining the complete native detector implementation, all report fields, coordinate semantics, default detector rules and both required modes. The removal-only protected config remains a separately pinned input; it is not a replacement detector. No detection regex, keyword, entropy threshold, required-rule behavior, path/history scope or native report row may be weakened or deduplicated.

1. **Strict parser terminal status.** Gitleaks' pinned `go.mod` names `github.com/gitleaks/go-gitdiff v0.9.1`; the current tag resolves to commit `7765724e69ab7954ccf71b4d45ff5d82446e952e`. Add an opt-in strict API to that dependency, for example a context-aware parse function with a file stream plus an explicit terminal error/completion result. That name/signature is a proposal, not an existing API. The worker's native build must use it and reject non-EOF parse/read/header failures instead of recovery or silent channel closure. Preserve the existing `Parse` API for compatibility, but never fall back to it after a strict failure. Verify ordinary legitimate Git preambles and no-addition/deletion-only commits remain valid; do not reject them merely for lacking an addition fragment.
2. **Propagate errors through source completion.** Fix W1 and W3 in `sources/files.go`, `sources/git.go` and the error callback in `detect/detect.go`. Collect the actual source-group errors; `Sema.Go` returns no error value in v1.2.0, while task errors are available through `Group.Wait()`. A separate wait group is not an error join. Check final context cancellation, including cancellation while waiting for tasks, and incorporate native Git wait status. The corrected path must cancel/drain/stop and reap as appropriate so strict-parser failure cannot strand a Git child blocked on stdout. Keep the existing CLI nonzero failure path and never emit a successful completion receipt on partial work.
3. **Keep known unsupported-source refusal.** Nonempty active root ignores, unsupported modes/content, archives without qualified container/inner-source binding, missing objects and any incomplete read remain whole-run failures. Do not delete/rename enrolled files or switch to per-file/stdin aggregation. The minimal error patch need not add archive support or change detector semantics. An optional later root-ignore loading correction is distinct from the completion fix; conservative refusal already preserves safety.
4. **Make the resulting artifact concrete.** Preserve original source, exact patch bytes, dependency revision/module checksums, build recipe/toolchain/regex-engine flags and separate Linux/Apple binary hashes. Give the derived scanner its own explicit artifact identity and protected scanner-policy digest; it is not the untouched official release executable. Keep findings schema/position compatibility checks against the pinned baseline. Prepare its reviewed qualification/contract update before selecting it for actual worker use.

The dependency's [`LICENSE`](https://github.com/gitleaks/go-gitdiff/blob/7765724e69ab7954ccf71b4d45ff5d82446e952e/LICENSE) is MIT with Billy Keyes' copyright notice; Gitleaks' [pinned license](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/LICENSE) is MIT with Zachary Rice's notice. Preserve the applicable notices in source/build distributions. The native dependency correction need not become copied Python protocol code or a custom fallback detector.

### Concrete native test APIs and controls

Use the dependency's existing `gitdiff/parser_test.go` package-level access and [`TestParse`, `TestParseNextFileHeader`, `TestParserInvariant_Advancement`](https://github.com/gitleaks/go-gitdiff/blob/7765724e69ab7954ccf71b4d45ff5d82446e952e/gitdiff/parser_test.go) as integration points. `Parse(io.Reader) (<-chan *File, error)` is the existing public API; `newParser`, `ParseTextFragments`, `ParseBinaryFragments` and `ParsePatchHeader` are available within that native package for focused tests. A small concrete reader that yields a valid first patch then a late read error, malformed/truncated text or binary hunk provides meaningful failing evidence. Assert terminal failure after earlier valid output, not only the synchronous constructor error.

For Gitleaks, retain `sources.Source.Fragments(ctx, yield)` and `Detector.DetectSource` as the observable boundaries. Add focused native tests for a non-permission file-open error, late reader failure, source-task failure, context cancellation during/across task completion, silent nonzero Git child, parser failure after a valid finding and a blocked child on early parser exit. Narrow fault injection is appropriate where the real kernel condition is not reproducible; use concrete readers/source values and owned disposable Git repositories rather than a generic mock framework. Native `sources/git_test.go` is commented out at this pin, so its presence is not execution evidence.

Then prove complete clean, tree-only, history-only, donor-only and merge-only native controls; exact ordered report/schema metadata; every skip/failure veto; no partial completion result; and unchanged input manifests. Run both scopes even after exit42 and preserve a failure if either scope is incomplete. Exact source/set provenance and final authenticated execution receipts remain worker obligations, not implications of the native patch alone.

## Artifact trust and actual human decision boundaries

The original native contract says a **platform-qualified release artifact**; it does not expressly mandate signed upstream provenance. Do not invent that requirement, and do not infer it from an unsigned checksum list. Matching official API/checksum values establishes the measured publication/bytes, not a signed builder or a reproducible source-to-binary relationship.

A complete contract must explicitly choose the intended trust authority: an independently reviewed protected import of the exact official bytes, an authenticated upstream provenance statement if one can be verified, or a separately qualified protected build/import of the concrete native correction. If protected import is chosen, state plainly that the trusted reviewer/import boundary is the authority and pin repository/release/asset/source/platform/archive/binary identities. If authenticated build origin is promised, checksum agreement cannot satisfy it. Corrected builds also need their exact source/patch/dependency/build identities and qualification receipts.

This is a concrete security trust-policy choice for eventual artifact substitution/activation, not a reason to request permission before preparing the patch, its tests and reviewable contract. No new human policy decision is needed now to measure archives, verify removal-only config equality, refuse incomplete work, research/prepare the narrow native correction or run safe local synthetic controls. Existing implementation-for-review approval covers that preparation. Actual substituted-artifact qualification, protected import authority, access/credentials, required hosting, governance and real findings keep their reserved review/authorization gates after a concrete artifact exists. All 11 human-owned criteria remain unchecked.

## Coverage summary

Task coverage remains assigned; full-worker design readiness does not follow from that assignment.

| Requirement | Has tasks? | Relevant tasks | Scoped assessment |
| --- | --- | --- | --- |
| FR-023 | Yes | SEC005, SEC006, SEC008, SEC013 | Complete native source/history processing is blocked by W1/W2. |
| FR-024 | Yes | SEC003–005, SEC008, SEC012 | Accepted source binding is retained; worker must prove actual scanned source, not just declared metadata. |
| FR-025 | Yes | SEC009, SEC010, SEC012, SEC015 | Human/issuer authority remains separate and unqualified. |
| FR-026 | Yes | SEC003–006, SEC008, SEC013 | Existing parser does not cure native completion errors W1–W3. |
| FR-027 | Yes | SEC003–006, SEC008, SEC012 | Restricted raw diagnostics must not enter ordinary output; separate complete report remains required. |
| FR-028 | Yes | SEC009, SEC011–013, SEC015 | Conservative unsupported/incomplete failure is preserved; no real finding accepted. |
| FR-029 | Yes | SEC001, SEC007, SEC014 | Donor and all existing test/evidence obligations remain. |
| FR-030 | Yes | SEC006, SEC009, SEC010, SEC013–015 | Protected execution/transport, actual hosting and full approved matrix remain open. |
| SC-011 | Yes | SEC003, SEC008, SEC012, SEC013 | No controlled or real finding acceptance is established. |
| SC-012 | Yes | SEC003, SEC005, SEC006, SEC008, SEC009, SEC012, SEC013 | Native silent-failure controls must be added before a completion claim. |
| SC-013 | Yes | SEC005–008, SEC013, SEC014 | Input inventory plus attempted scans does not prove full consumption. |
| SC-014 | Yes | SEC009, SEC010, SEC013, SEC015 | Forgery/privileged execution/independent hosting qualification remains. |
| SC-015 | Yes | SEC010–015 | Exact review, matrix/services, real decisions, hosting and legitimate merge/main remain required. |

Unmapped SEC tasks: none. Requirements with zero assigned task: none. Detailed W1–W3 remediation still needs explicit native patch/build tasks and failing controls in the final SEC006 plan; the current broad task and readiness checkpoint do not purport to supply them.

## Managed method and independent measurements

Used the repository's managed analyze procedure with explicit slice activation and ran the prerequisites command once with `--json --require-spec --require-tasks --include-tasks`. It selected slice 002; required documents exist. No analyze hooks were registered. CodeGraph status reported current, and CodeGraph preceded source exploration. Read US5/FR-023–030/SC-011–015, relevant constitution/AGENTS guidance, contracts, SEC tasks, worker research, native sources and the new readiness/plan/research/task appends. The complexity guidance remains applicable to implementation; no new functions exist to score in this read-only design review.

Independently remeasured both local Darwin arm64 and Linux x64 archives and their contained executable bytes without extracting or executing them. Both archive hashes and binary hashes match the root measurement record; each archive has exactly the three stated regular members. Reparsed original and protected TOML: all 222 rule records and other global fields agree after only allowlist removal, and no global/rule allowlists remain. These are byte/semantic measurements only. Neither Linux nor Darwin executable was executed by this reviewer, no signed provenance was verified, and no real repository/native findings were scanned.

Pinned upstream Gitleaks source and go-gitdiff/semgroup sources were read directly; tag-to-commit lookups were read-only. No refs, config, access, credentials or repo files changed. The sole reviewer write is this new report. No raw Match/Secret, native report or candidate value is reproduced. `git diff --check` passed. Historical tracked changes are append-only and no implementation source is changed at this checkpoint.

## Metrics and next actions

- 8 functional requirements plus 5 buildable success criteria reviewed; task assignment coverage 13/13.
- SEC tasks: 15; checked preparation/source tasks: 5; open tasks: 10.
- Full-worker design blockers: 3 HIGH; CRITICAL: 0; MEDIUM: 0; LOW: 0.
- Measurement/readiness-document scope: SHIP. Complete unmodified-release worker mechanism: REVISE.

Continue directly with a concrete pinned native error-propagation/strict-parser patch plan and meaningful failing native controls, retaining all detectors, source/history/report scope and existing IDs. Update the owned worker contract/model/plan/quickstart/tasks to describe the actual corrected mechanism and qualification boundary, then run managed analysis before the full worker implementation. A diagnostic matcher or byte-only helper cannot be the completion mechanism or a substitute SEC006 milestone. Do not ask for a new human decision merely to prepare these already authorized fixes.

## Reviewed artifact hashes

Repository paths are relative to its root; work paths are relative to the parent chat workspace.

| Artifact | SHA256 |
| --- | --- |
| `specs/002-reproducible-runtime/contracts/native-worker-readiness.md` | `88dc8312fd844729424a45f5a69aae6cba90f9ab010675d893030db99d515c42` |
| `specs/002-reproducible-runtime/plan.md` | `30c46a202b71d9fc54ffbc99d9748ffb394f5501ef6fdfa9e93d76f3315d2089` |
| `specs/002-reproducible-runtime/research.md` | `acc972f81997eb0b64b02cf82141f3bfe8b18ea50b03cdf686ed4d5c87a21e5e` |
| `specs/002-reproducible-runtime/tasks.md` | `f8d34074ffdad6c4b9f3e0d46510cf810c164ebcb080ae7f84c9817b558bfb1f` |
| `specs/002-reproducible-runtime/evidence/security-policy/research/worker/research.md` and `work/security-worker-research.md` | `f5f60c90b157f56f3c359018d2de6b73400e024471d780a32ed6961abefa0feb` |
| `work/security-worker-research/root-asset-measurements.json` | `2026271a4f170542ecb0c3dbc18f6cb0fe4db2c72b38357f6f7fa8141974504f` |
