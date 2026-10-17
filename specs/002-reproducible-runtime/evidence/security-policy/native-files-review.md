# Independent adversarial review — NC003 native file/detector completion

**Verdict: SHIP for the NC003 file/detector component at `bc836bb1cff5feca40cf3e57131b810c3f59682c`.**

Reviewed against `a990152f2651c61664e5d3f5e28350c8397cfe58`. No blocking correctness, scope or test-weakening defect survived this review. Confidence is high for this bounded component. This verdict does not accept NC004 Git lifecycle work, a full SEC006 worker, any scanner substitution/import, protected execution, finding disposition, governance activation, hosting or merge.

Review completed 2026-10-17 14:33 UTC. The independent grader attempted to refute the result using source/diff inspection, fresh full suites, an archived-parent RED replay and additional runtime probes. No author-owned repository file was edited.

## Findings

No actionable blocking findings. Existing unsupported-source/native skip behavior and arbitrary blocked-reader cancellation limits are recorded below; they are not certified by this NC003 verdict.

## What was independently checked

- `Files.scanTargets` now returns traversal/stat/symlink-resolution errors. Non-permission opener failures reach `targetFragments`' result rather than a silent nil (`sources/files.go:60–74,99–105,158–169`). The opener seam is private, narrow, and defaults directly to `os.Open`; it is not a candidate-controlled execution mechanism.
- `fragmentsWithOpener` waits on the actual `semgroup.Group` and joins its errors with traversal and final context state (`:144–155`). A failed semaphore acquisition is not confused with a started task. Started tasks are drained before return, including when one file has already failed.
- File read failures remain failures even if the consumer callback returns nil. Read errors accompanying bytes, errors discovered during safe-boundary read-ahead and zero-progress reads are not converted into clean completion (`sources/file.go:184–217,269–271`). `yieldSourceError` also retains a distinct callback failure through `errors.Join`.
- Opened regular-file readers close after `File.Fragments`, and close failure and cancellation are joined into the task result (`sources/files.go:165–169`). The EOF callback path retains both callback error and cancellation (`sources/file.go:242–244`).
- The detector callback returns the fragment error, and `DetectSource` joins final parent cancellation (`detect/source_completion.go:39–55`; `detect/detect.go:209–219`). Timer/log-context extraction preserves the prior operation; no native finding parser, detector, coordinate or report field was redefined.
- Independently compared the core `DetectContext`, `detectRule`, `processRequiredRules`, `AddFinding`, `Findings`, `checkCommitOrPathAllowed` and `checkFindingAllowed` function source between base and reviewed commit: each is byte-identical. Report/config/baseline/allowlist files are outside the eight-file diff. Existing finding assertions are unchanged; the upstream test changes add typed missing-root/cancellation expectations plus a missing deferred cancel.

## Fresh independently executed evidence

All commands ran against an archived copy of the exact reviewed commit, with the task-private Go wrapper, fixed environment, readonly module mode and Go `1.27.2 darwin/arm64`. Raw test output stayed in restricted local logs; no native Secret/Match or candidate value is reproduced here.

| Check | Result |
| --- | --- |
| `test -mod=readonly -count=1 -json ./...` | Exit 0; 291 distinct test IDs, seven tested packages passed, no test failures. |
| `test -mod=readonly -count=1 -tags=gore2regex -json ./...` | Exit 0; the same 291 IDs, seven tested packages passed, no test failures. |
| `vet -mod=readonly ./...` | Exit 0; empty diagnostic log. |
| `vet -mod=readonly -tags=gore2regex ./...` | Exit 0; empty diagnostic log. |
| `gofmt -l` over the eight changed Go files | No unformatted files. |
| Fresh `gocognit -exact-recursion -json -over=-1 sources detect` | Exit 0; no fallback/warnings. New production helpers have maximum cognitive complexity 4. |
| Fresh `gocyclo -over=-1 sources detect` | Exit 0; new production helpers have maximum cyclomatic complexity 5. |
| New author-owned completion files | 43, 55, 58 and 163 physical lines; each below 399. |

The analyzers ran over the actual archived code. Existing large upstream functions remain advisory findings, not a full-tree complexity pass. In particular, modified native `fileFragments` and `scanTargets` retain substantial upstream complexity. No Python gate threshold, scope or exception changed.

Compared the independent default test ID set with the untouched upstream default baseline: **all 275 baseline IDs remain, and 16 IDs are added**. Test-ID count is supplementary evidence, not the reason for acceptance.

### Independently reproduced RED

Archived the exact base and added only the reviewed public-boundary RED test files (`sources/completion_test.go` and `detect/completion_test.go`). Ran the original read-failure/missing-root/detector-error/cancellation controls without changing base implementation.

The command exited 1 with failures in both source and detector packages. Four top-level controls fail; including the read-failure test's two subcases, the JSONL records **six failed test events**. This is an actual runtime regression reproduction, not a missing-API build failure. The corrected exact-commit full suites independently pass those IDs.

### Additional adversarial controls

Added one reviewer-only test file in the archived copy after the full-suite runs. Four independent runtime controls passed:

1. A real concrete reader produces data, then fails during read-ahead; the consumer also returns a different error. Both errors remain observable.
2. A concrete reader produces data, then repeatedly makes zero progress during read-ahead. The source returns `io.ErrNoProgress` rather than clean completion.
3. A reader returns final bytes with EOF; its consumer both cancels and fails. Both callback failure and cancellation survive.
4. Two real files start under a two-slot native semaphore group. One closes with an injected close failure while the second remains blocked in its reader. The source cannot return before the second is released; afterward it retains the failure, both callbacks and both actual file closes.

The probe command used `-count=1 -timeout=30s -run=TestReviewNC003 ./sources` and exited 0. The probe file is an external review artifact, not part of the accepted commit or the 291-ID full-suite result. All eight reviewed files were compared to committed bytes after testing and remained identical.

## Limits and retained obligations

- This component waits for started readers; it does not make an arbitrary blocking `io.Reader` interruptible. The cancellation test deliberately releases the blocked reader before requiring the join to finish. Actual worker process/resource deadlines and owned Git-pipe interruption remain later qualification work.
- Native empty/oversized/binary/archive/symlink/allowlist skip behavior is preserved. Archive extraction/inner-reader error handling is not newly qualified here. The full worker must apply its declared unsupported-source refusal, source inventory and diagnostic veto; none of those skip paths may become a clean full-coverage claim.
- Git command completion, strict parser consumption, child cancellation/pipe cleanup and wait-status propagation are outside this NC003 diff and require NC004 review. This verdict does not clear those earlier full-worker blockers.
- Fresh runtime evidence is Darwin arm64 only. Linux runtime qualification, full platform artifacts and authenticated protected import/execution remain open. No real repository secret scan, external notification, credential, ref or access operation was performed.
- Fresh full-suite tests cover committed code; the four additional probes ran in the default engine only. Their IO/group behavior does not invoke detector rules. Both complete committed suites were independently run in their respective engine modes.

## Exact reviewed identity and hashes

- Commit: `bc836bb1cff5feca40cf3e57131b810c3f59682c`.
- Base: `a990152f2651c61664e5d3f5e28350c8397cfe58`.
- Exact Git archive SHA256: `577aa39470350ac14aa10f88fe0932a2471747117aecaf469168d1f592f5b046`.
- Binary-capable unified diff SHA256 (20,959 bytes): `05910a722f86f260f209e49d19c64e4b04e3b99b49a302140ae1a4a5499fdfaf`.

| Reviewed file | SHA256 |
| --- | --- |
| `detect/completion_test.go` | `9cf5b95869a32664b201f3e0c30f0f5072ada1fea26714fea95fc2ff69ce28f6` |
| `detect/detect.go` | `41c0b123c0987c163f0c3503a8149c098b5332b12d6c6cfb866ec4b08d6b935c` |
| `detect/detect_test.go` | `6e1ea4ba8f260e8328490cc6fa3cefdca34227456e8a173c39f13a7957889665` |
| `detect/source_completion.go` | `523e77275d0873310d9d2866b0e316eff9e1d4c4be850bda0be8c903bb39b117` |
| `sources/completion_test.go` | `de6b350f2cabdba9ba6ee3f74dc59f260e65b758edcb3c266bc64ba4047da263` |
| `sources/file.go` | `bb896e1b3d1f9c09bc67e9d74771c4c6ace7585e0763ab96e8126a38ee44e381` |
| `sources/files.go` | `fb6c3af7311322ec7201b9d9b34518f09ff77eea0a4738731f6b73af8c6a512f` |
| `sources/files_completion_test.go` | `5a08db82849f0ff5c8fdb7ae19890decc8eff41652b72ee49ea0c07290c1058a` |

Evidence paths below are relative to the parent chat workspace's `work/` directory.

| Independent evidence | SHA256 |
| --- | --- |
| `security-native-files-independent-default-tests.jsonl` | `531adc6fd2cd6ca53788353eae357b9fca926536a97f09641e1b50b00882ec4f` |
| `security-native-files-independent-production-tests.jsonl` | `3f7513f10c6a78d613296f9d12d76ba3b6ef5530333861b2317522097ccd74ba` |
| `security-native-files-independent-parent-red.jsonl` | `2577dd5620d51d01e91fc508567e4c210f68a6e7cca2bb273b7f055c331063a0` |
| `security-native-files-independent-adversarial.jsonl` | `9d52aa6b70b7d7086902509dd9d968d2d090ec67695773f93ba136571813d8df` |
| `security-native-files-independent-cognitive-exact.json` | `0be30c8cf03d90c9425e9c811e3dc5b3b760d0f31b1c30fbced9efe27331210a` |
| `security-native-files-independent-cyclomatic.log` | `392fc486bd05bafd6f90fc9713f8dbe72861a07dca1e4dd088d4715954f2e226` |
| Both independent vet logs (empty) | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `nc003-review-bc836bb/sources/review_nc003_test.go` | `7473ce14173630b445f8d09725a9d3692bb398a77a80f31a5012c5952c5334f1` |
| `run-native-go.py` | `7a5b20ee403459fa375718d5cbb1d6c3e2321b44c13a31b8e5ec7947ae0f145a` |

CodeGraph was used before source exploration, followed by inspection of the exact archived diff. The adversarial-review and complexity-budgets guidance were read and applied. This report accepts only the hash-bound NC003 component and its verified correction, with the retained limitations above.
