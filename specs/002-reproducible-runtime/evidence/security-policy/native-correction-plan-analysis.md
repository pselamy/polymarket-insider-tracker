# Specification Analysis Report — native completion correction

**Verdict: SHIP for preparing the failing controls and minimal native correction described by NC001–NC006.**

This is a readiness verdict within the user's existing implementation-for-review authorization. It is not acceptance of the correction, a substituted scanner artifact, a complete SEC006 worker, authenticated execution, finding approval, governance activation, required hosting or merge. The earlier W1–W3 findings remain implementation obligations until the actual corrected artifacts pass their controls.

Reviewed primary repository HEAD `c283c45f3ffa94ab235b81b3dc7aeb10e894d17f` plus the owned dirty documentation listed below. The native checkouts are clean at Gitleaks `83d9cd684c87d95d656c1458ef04895a7f1cbd8e` and go-gitdiff `7765724e69ab7954ccf71b4d45ff5d82446e952e`. No native correction code was present or changed during this review.

## Findings

| ID | Category | Severity | Location(s) | Summary | Recommendation |
| --- | --- | --- | --- | --- | --- |
| — | — | — | — | No active inconsistency, security ambiguity, coverage gap or constitutional conflict established against this bounded preparation plan. | Proceed with the specified RED controls and correction; independently verify the resulting code and artifacts. |

There are **0 CRITICAL, 0 HIGH, 0 MEDIUM and 0 LOW active plan-readiness findings**. This does not relabel the unmodified scanner as complete or close W1–W3 on documentary evidence.

## Adversarial assessment

**The endpoint is an appropriate minimal correction boundary.** The proposed synchronous callback walker removes the old file-channel closure ambiguity while reusing native parsing. `native-completion-correction.md:26–47` explicitly retains the legacy public `Parse` behavior and requires the corrected scanner to use the strict path without fallback. The existing parser does have the shared helpers the plan needs; no nonexistent helper or unverified public signature must be guessed. Exact callback naming/types can be fixed in the first endpoint tests as a routine implementation choice, without changing the specified context/reader/callback/error boundary.

**Sticky read errors address a real lower-level loss path.** The pinned parser has three-line lookahead in `parser.Next`/`shiftLines`; helpers advance it while handling headers and fragments. The plan requires the first non-EOF error to remain available even if later advancement reaches apparent EOF (`:35–37`). Implementation must preserve the error when a reader returns bytes and an error together, and when a later read returns EOF; terminal success must not overwrite that earlier error. This is already covered by the required late-reader controls rather than left as a new policy decision.

**Header and EOF compatibility are expressly retained.** The plan requires the strict candidate path to propagate errors currently swallowed by header selection, while the legacy recovery path remains compatible (`:32–39`). It also requires valid empty input, no-addition/deletion-only commits, ordinary preambles, full commit identities and ordered multi-file output (`:97–99`). The native parser's no-newline markers, empty/mode-only files and existing text/binary fixture grammar should remain in the retained compatibility corpus. EOF after a valid native construct is distinct from EOF before a declared hunk is complete. Strictness should expose native grammar errors; it should not invent a narrower detector or silently discard a valid suffix.

**Runtime RED is meaningful and honestly scoped.** A new API's absence may initially prevent compilation, but the contract explicitly says this is insufficient (`:44–47,78–79,97–99`). A clearly labeled old-`Parse` observer can collect its current channel and synchronous error, then fail the required late-reader/malformed-after-valid expectations at runtime. Preserve that observer's source, endpoint, command and results. After implementing the new endpoint, move the same scenarios/test IDs to it and keep the untouched legacy tests. That demonstrates the new strict path fixes the completion obligation; it must not be reported as a change to legacy `Parse`, and the observer must not synthesize success or errors that the real endpoint did not return.

**Cancellation ownership is plausible and testable.** The plan explicitly disclaims cancellation of an arbitrary blocked `io.Reader` from context alone (`:40–42`). The native command owner must interrupt its own pipe. Its terminal result must include parser/stderr/task/context/wait errors, and early failure must close/stop/reap without blocked pipe producers (`:59–64`). Normal stdout EOF must not cancel correctly scheduled detector work. This separates successful input exhaustion from completion of downstream processing, which the previous unmodified design lacked.

The implementation controls should make those already stated obligations observable: callback or parser failure while a producer writes continuously; cancellation while a bounded file send or source-group acquisition is blocked; stderr read/framing failure; EOF with a child still alive; and parent cancellation between the final callback/task and success publication. Use bounded deadlines and assert terminal failure plus stopped/reaped children and settled tasks, not merely that a cancellation function was called. A general callback that ignores cancellation need not become magically interruptible; the owned scanner adapter must use cancellation-aware sends and its actual command deadline.

**Actual source errors will reach the existing CLI failure path.** The plan covers walk/stat/open/read/close/callback failure, actual `semgroup.Wait`, detector fragment-error propagation and final parent cancellation (`:51–57`). `semgroup.Go` returns no error value at this pin, so collecting the group's real terminal error rather than a separate `sync.WaitGroup` is the correct distinction. Git parser and child errors remain failure-dominant even after valid findings (`:59–68`). The next code review must verify all branches; the document alone proves none passed.

**The two build repairs are bounded prerequisites.** CodeGraph confirms the Airtable and Perplexity generator files import stdlib `regexp`, while `config.Rule` uses the project's selected regexp type. Changing only those imports to the existing project wrapper is a concrete build-compatibility correction, not detector-policy latitude. Preserve their expressions and every other rule field exactly. NC001 and the contract require the failed baseline to remain, followed by complete default and production-engine suites; an excluded-package run cannot substitute for the latter (`:19–22,76–77,85–92`).

**Artifact and authorization claims stay honest.** Source/patch/dependency/toolchain/flags/engine/platform identities and separate derived build hashes are required; the output may not be called untouched official release bytes (`:9–17,89–95`). Local builds and checksum agreement do not create protected import authority. The model and quickstart preserve that distinction. No additional human permission is needed now to prepare this concrete correction. Actual artifact substitution/import qualification, protected execution, individual findings and activation keep their existing gates after the reviewable artifact exists.

## Coverage summary

Coverage means planned controls/remediation exist, not implementation acceptance.

| Requirement / prior gap | Has task? | Task IDs | Assessment |
| --- | --- | --- | --- |
| Buildable production regex engine; native fields/detectors retained | Yes | NC001, NC005 | Two import-only repairs; both complete engine suites and rule/schema comparison. |
| W2: late native read/header/hunk/metadata errors | Yes | NC002 | Runtime old-endpoint counterexamples, strict walker, sticky read errors, legacy tests retained. |
| W1: silent non-permission open; source completion | Yes | NC003 | Narrow IO boundary plus real files/readers; every source failure propagated and joined. |
| W3: task/detector/child errors; cancellation | Yes | NC003, NC004 | Actual group result, strict pipe ownership, failure-dominant terminal result and child cleanup. |
| FR-023 / SC-013: complete native source/history | Yes | NC002–005 within SEC006; SEC008 remains | No source/history narrowing, fallback or deduplication; complete-worker proof still required later. |
| FR-024 / SC-012: exact native identity/schema/source | Yes | NC002, NC005; accepted SEC005 retained | Header/coordinate/schema/order compatibility and partial-output failures must pass on actual code. |
| FR-026 / SC-012: complete bounded execution and failures | Yes | NC002–005 | Missing-API compilation alone cannot establish RED; runtime source/process controls required. |
| FR-027: safe ordinary output, retained native reports | Yes | NC003–005 and existing SEC tasks | Restricted raw logs/reports, constant ordinary summaries; no exposure permission granted. |
| FR-029: original IDs/evidence/donor obligations | Yes | NC002, NC005; SEC007/014 | No test-ID or scenario removal; original donor/history remains enrolled. |
| FR-030 / SC-015: platforms, artifact review and real completion | Yes | NC005, NC006; SEC009–015 | Linux/Apple runtime qualification remains distinct from cross-builds and local byte measurements. |
| FR-025/028 and SC-011/014: human authority and acceptance | Yes | Existing SEC009–015, retained by NC006 | No new authority/acceptance mechanism in this correction. |

All six NC substeps have a requirement or corrective obligation; none is unmapped. Their order is dependency-consistent: NC001 → NC002 → NC003 → NC004 → NC005 → NC006. Each remains unchecked and cannot close SEC006 by itself. All 13 original US5 FR/SC requirements retain assigned SEC tasks; no original requirement or human checklist criterion is removed.

## Constitution and managed workflow

No preparation-scope constitutional conflict was established. The change preserves research-only behavior, native detector/source/history obligations, exact report/source semantics and existing application compatibility/gates. Tests precede behavior changes. The plan records native analyzer evidence without using upstream complexity as permission for unrelated churn or weakening Python gates. Full native source/build review and supported platform qualification remain before actual substitution/worker claims. All 11 human-owned criteria remain unchecked.

Ran the managed prerequisites once with explicit `SPECIFY_FEATURE_DIRECTORY=specs/002-reproducible-runtime` and `--json --require-spec --require-tasks --include-tasks`; it selected slice 002 and found required artifacts. No extension hooks were registered. CodeGraph preceded source exploration in both indexed native checkouts; their indexes reported current. The primary CodeGraph index also reported current. Relevant managed analyze, complexity and technical-integrity guidance was already read and applied.

Inspected the new contract, append-only plan/tasks/quickstart/model changes, previous W1–W3 analysis, original US5 requirements and the pinned parser/native source call paths. The root's additional clarification of the old-observer harness and strict/private header helpers agrees with the written contract. No repository source, tests, refs, config or access changed in this review. This externally saved report is the only reviewer write.

## Baseline evidence and verification limits

Independently inspected recorded JSONL terminal events: the untouched parser suite has one passing package and no package failure; the full default Gitleaks suite has seven passing packages and no package failure; the selected production runtime suite has five passing packages and no package failure. The full production-engine invocation has generator config/rules build failures while seven other package tests pass. Thus the generator packages fail before their tests, rather than the entire invocation running zero tests. No green full-production result is inferred.

These are recorded baseline results, not fresh implementation validation by this reviewer. No correction exists yet and no new test execution or native finding scan was performed. The owned Go wrapper uses a fixed environment and task-owned runtime/module/build locations; the contract still requires readonly module inputs and recorded build/engine identity in actual commands. No raw native finding, Match/Secret or candidate value is reproduced.

`git diff --check` passed. The four historical tracked document changes are exact append-only extensions of HEAD; native source checkouts are clean at the pinned revisions. No newly implemented function exists to score with a language analyzer during this plan-readiness review.

## Metrics and next action

- Original requirements: 8 FR plus 5 buildable SC; assigned coverage retained for 13/13.
- New native-correction tasks: 6; mapped: 6; checked: 0.
- Active plan-readiness ambiguities/duplications/critical issues: 0/0/0.
- Scoped verdict: SHIP to RED controls and minimal correction preparation; no SEC006 completion verdict.

Proceed directly with NC001, preserving its failed build baseline, then the runtime RED controls and dependency-ordered correction. Keep the original observer/endpoint evidence and existing IDs. Independently review the exact code, source/artifact hashes, native failure controls and complete engine/platform results before advancing to a revised worker/import qualification contract. No further preparation-permission question is warranted.

## Reviewed document SHA256

Repository paths are relative to `specs/002-reproducible-runtime/`.

| Document | SHA256 |
| --- | --- |
| `contracts/native-completion-correction.md` | `690bfa883119a66899656ce79b1e287b2f3d2ba2f23b125d107aed3de6f68108` |
| `plan.md` | `576052954dc3d7f4a8d9bdb8269b3c2b5e9d5d679d3ded6347aeb380b418be80` |
| `tasks.md` | `830199e13b88372694d4e9d227b85a51c9a949c41ebd3b1c479be505266a6d46` |
| `quickstart.md` | `1022cd14e3a53e52908bc0927baa8a2a6803c86cbff8dfde6b4786c7856ddc6d` |
| `data-model.md` | `73426620883b488f1d60c60b209e7e78e4775e97d5021063d86b65357cd75504` |

Baseline JSONL SHA256: parser `ac284e9d781cb885d9073a242d8b7a550e4816a873d1c9d06a0b153e51c3c432`; full default scanner `067612ce6fcd3cf98e3d321e7b4640c0a9157c023d10ca76f6b9386467749a89`; full production invocation `5e9d0cd319827c94b2e46d6f3bc2363951eace6157c8d46af918b1a1ca1647d2`; production runtime packages `d4e45c457ba19c0eb1b1ce8dc3900d922ec989b38adc2749b8cec4c27e0b94bd`.
