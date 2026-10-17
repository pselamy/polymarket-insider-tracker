# Native completion correction for review

Status: proposed preparation plan resolving W1-W3; not an active scanner substitution or complete
SEC006 worker. Original source, release artifacts and tests remain preserved. All native detector
fields/regexes/thresholds and report coordinates/schema remain; no fallback or source/history omission.

## Immutable source and build inputs

- Gitleaks:83d9cd684c87d95d656c1458ef04895a7f1cbd8e (v8.30.1).
- Native parser dependency: github.com/gitleaks/go-gitdiff v0.9.1,
  commit7765724e69ab7954ccf71b4d45ff5d82446e952e. Keep its MIT/Billy Keyes notice.
- Semaphore dependency: github.com/fatih/semgroup v1.2.0 and existing Go module checksums.
- Gitleaks' MIT/Zachary Rice notice remains. Derived artifacts retain separate source/patch/module/
  build identities; never label them untouched official release bytes.
- Use an isolated official checksum-verified Go1.27.2 build toolchain and private module/build cache,
  fixed credential-free environment, locked readonly module inputs and `gore2regex` production flag.
  Recheck build/regex engine identity. The project's minimum Go1.24.11 is not a signer/build pin.

The initial exact upstream full `gore2regex` test build fails in two rule-generator files because
they import stdlib regexp while config.Rule uses the native selected engine type. First correct
only those two imports to the existing project regexp package; no rule text/field changes. Preserve
the failed baseline. Run full default and production-engine suites, not an excluded package set.

## Strict native parser API

Add an opt-in synchronous callback API, proposed `WalkStrict(ctx, reader, yield) error`, in the
pinned parser dependency. Existing exported Parse behavior/signature remains available for legacy
callers; the corrected scanner must use strict parsing and may never fall back after failure.

- Return success only after complete native stream exhaustion and every callback completed.
  Propagate header/hunk/binary/read/callback/context errors, including failures after valid output.
- Reuse the existing native parser and grammar. Factor shared file-header selection so the legacy
  recovery branch remains compatible while the strict path rejects its parser errors. Preserve
  no-addition/deletion-only commits, merge preambles and ordinary legitimate native metadata.
- Retain the first non-EOF reader error in parser state so lookahead/helper advancement cannot lose
  an error before the terminal result. EOF itself is normal only when existing native file/hunk
  parsing completed; truncation or malformed grammar is failure, never an empty successful suffix.
- Parse valid native patch metadata without discarding parse failures. Test commit boundaries and
  full-length identities across no-addition commits and multiple files; retain ordered output.
- Cancellation is checked between parsing and callback steps. A reader blocked in Read still belongs
  to its caller: the native command owner must cancel/close that pipe. Do not promise arbitrary
  io.Reader cancellation from a context alone.

The API returns a processing error, not authenticated source/scan authority. Concrete late-error
readers and malformed streams establish failing evidence against the old asynchronous observer;
the corrected strict endpoint must fail the same scenarios. Tests retain their IDs and explicitly
distinguish baseline observer limitations from the new completion API; no fake success adapter.

## Native source and process completion

Directory processing must return every walk/stat/open/read/close failure and callback failure.
Collect actual semgroup.Wait errors, replacing the separate wait group as an error authority.
Keep open/read failure injection at a narrow internal IO boundary with real readers/files and
unchanged public source interfaces; no generic mock system or candidate-controlled callback authority.

The detector's source callback must propagate a fragment error rather than log it and return nil.
Check the parent cancellation result after work completes so partial detector work cannot be clean.

Native Git command ownership must supply the strict parser with an owned pipe, a cancellable child
context and a complete terminal result. It must collect parser error, stderr error, source-task
errors, parent cancellation and actual Git Wait status. On early failure cancel/close/stop and reap
without leaving a parser blocked on output, an unread pipe or a child blocked on stdout. Drain all
started detector tasks, propagating their errors; do not cancel successful pending work simply
because stdout reached EOF. Keep error/result channels bounded and terminal completion unambiguous.

Use the existing native final error/exit path, with failure dominant even when a finding was already
produced. The outer worker still runs both modes, retains complete redacted native reports and
constant ordinary diagnostics, and treats incomplete native work as failure. No native deduplication.

Known unsupported-source refusal and trace veto remain defense in depth. This correction does not
silently add archive binding, remove an ignore file or qualify all OS isolation. Root-ignore support
is a separate optional correction; refusing nonempty active ignores already fails safely.

## Ordered implementation and evidence

1. Preserve untouched upstream baseline tests and exact library/scanner source identities. Record
   the two import build failures, then the minimal import correction and both full engine builds.
2. Write concrete old-parser late reader/malformed header/text/binary counterexamples and new strict
   completion tests. Implement the strict API with the existing parser; retain all legacy tests.
3. Add failing source tests for non-permission open/read/close/task/callback failures and cancellation.
   Implement directory/detector propagation and actual semaphore-group joins.
4. Add failing native Git tests for silent nonzero child, parser failure after valid output, bounded
   early failure while the child writes forever, EOF-then-hang and cancellation during completion.
   Implement strict stream ownership, terminal status, task joins and actual child cleanup.
5. Run full native suites in both engine modes and dependency tests; format/vet and repository-native
   language analyzers. Keep new helper/test files <=399lines and new helpers cognitive<=5/C901-style
   cyclomatic<=10 where the Go analyzers apply. Record existing upstream complexity separately;
   analyzer output does not authorize broad unrelated churn or any Python-gate exception.
6. Build exact Linux x64 and Darwin arm64 artifacts with recorded flags/toolchain/module/patch hashes.
   Execute available platform controls honestly; cross-compilation is not runtime qualification.
   Compare clean/tree/history/donor/merge and native report/coordinate behavior with the pinned
   baseline; only intended failure propagation or separately reviewed stronger config may differ.
7. Independently refute exact source/build/test artifacts, then prepare the revised scanner
   qualification/import contract and actual worker plan for review. Real substituted-artifact import,
   protected execution, gate activation, human findings/ratification/hosting/merge remain reserved.

Meaningful controls must cover partial successful output followed by a failure, all ordered findings,
valid empty input, legitimate preambles and deletion-only commits. A compile-only missing-API failure
does not replace a runtime reproduction of the old error-loss behavior. No test/ID removal, threshold
weakening, unsafe candidate execution or real native finding scan is authorized by this preparation.

Run managed analysis before implementing this correction. SEC006 and SEC007-015 remain open;
accepted SEC005 source-binding code and every existing product/Relentless requirement remain intact.
