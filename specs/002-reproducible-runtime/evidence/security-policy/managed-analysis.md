# Specification Analysis Report

**Final verdict: SHIP — isolated pure-parser preparation (SEC003–SEC004) only.**

Independent adversarial analysis of story 5, FR-023–FR-030, SC-011–SC-015 and SEC001–SEC015. Reviewed at 2026-10-17 00:16 UTC in `work/polymarket-insider-tracker`, branch `codex/polymarket-secrets-reconciliation`, unchanged HEAD/baseline `523b0a35466e799c36e85e3bc5ea046cf3263c31`.

The first reading was **REVISE** for one HIGH disclosure ambiguity. The author corrected the contract, model, plan and SEC003 controls; I independently reread those exact on-disk corrections. There are now **0 active CRITICAL, 0 active HIGH, 0 active MEDIUM and 0 active LOW findings** against readiness to prepare the pure parser. This verdict does not authorize additional scope: it evaluates preparation already approved by the user.

This is not implementation acceptance, scanner cleanliness, per-finding approval, amendment ratification, trusted-service qualification, gate enrollment, access activation, PR readiness, merge approval or complete security acceptance. All 13 substantive implementation/qualification tasks remain open. No parser implementation exists in the repository at this review point.

## Findings and correction verification

| ID | Category | Severity | Location(s) | Summary | Recommendation / verified disposition |
| --- | --- | --- | --- | --- | --- |
| A1 | Security ambiguity | HIGH, resolved | `contracts/native-secret-policy.md:38–61`; `data-model.md:133–144`; `plan.md:348–352`; `tasks.md:470` | The original packet described retained path/rule/fingerprint/tags as safe metadata without distinguishing restricted identity data from ordinary display. A well-formed untrusted report can place arbitrary text in these retained fields, so excluding Match/Secret alone does not meet FR-027 or make default object repr safe. | **Verified repaired:** exact identity text is retained only for restricted processing/evidence; ordinary output permits fixed categories, enum mode, counts and digests; finding repr omits every candidate-controlled text field; report repr omits findings; internal serialization is not an authorized renderer. SEC003 now explicitly requires sentinel/repr/formatted-error/log controls for all retained text; carry this obligation into SEC004 implementation. |

The correction preserves exact identity rather than normalizing or deleting fields. It adds no approval mechanism, hidden acceptance or native detector exception. No other concrete blocker to the narrow increment was established. Unknown native schema, unsupported coordinates or source forms remain failures, not silent widening.

## Coverage summary

Coverage below means **a planned task exists**, not that the requirement has passed. SEC002 is general document preparation and is omitted from most rows to avoid disguising implementation gaps with paperwork.

| Requirement key | Has task? | Task IDs | Notes |
| --- | --- | --- | --- |
| FR-023 | Yes | SEC005, SEC006, SEC008, SEC013 | Full source/history/worker completeness is later work; parser does not prove it. |
| FR-024 | Yes | SEC001, SEC003, SEC004, SEC005, SEC008, SEC012 | Parser preserves exact coordinates/collisions; independent full-blob binding and actual decisions follow. |
| FR-025 | Yes | SEC009, SEC010, SEC012, SEC015 | Authenticated human authority, revocation and hosting remain unqualified and default deny. |
| FR-026 | Yes | SEC003, SEC004, SEC005, SEC006, SEC008, SEC013 | Exact exit/report state machine is in the first increment; execution/freshness/scope provenance is later. |
| FR-027 | Yes | SEC001, SEC003, SEC004, SEC008, SEC012 | Corrected output/repr policy is a first-increment obligation; retained restricted data is not safe display data. |
| FR-028 | Yes | SEC009, SEC011, SEC012, SEC013, SEC015 | Exception is proposed, inactive and unusable by parser; exact governance and individual decisions still needed. |
| FR-029 | Yes | SEC001, SEC007, SEC014 | Donor preservation, original main test IDs and appended traceability remain explicit. |
| FR-030 | Yes | SEC006, SEC009, SEC010, SEC013, SEC014, SEC015 | Worker/issuer isolation, actual hosted requiredness, supported matrix and merge remain later tasks. |
| SC-011 | Yes | SEC003, SEC008, SEC012, SEC013 | Parser controls cover parsing/collisions only; controlled acceptance needs the full boundary. |
| SC-012 | Yes | SEC003, SEC005, SEC006, SEC008, SEC009, SEC012, SEC013 | All negative-control domains are assigned; none is certified by this review. |
| SC-013 | Yes | SEC005, SEC006, SEC007, SEC008, SEC013, SEC014 | Inventory, retained-history completeness and collection/evidence retention remain to execute. |
| SC-014 | Yes | SEC009, SEC010, SEC013, SEC015 | Native trust/forgery/revocation/privileged-execution controls are deferred, not waived. |
| SC-015 | Yes | SEC010, SEC011, SEC012, SEC013, SEC014, SEC015 | Full completion requires exact review, checks, human decisions, actual hosting, legitimate merge and resulting main. |

**Zero-coverage requirements: none. Unmapped SEC tasks: none.** SEC001/SEC002 cover research and document preparation; their two checked boxes neither satisfy human domain review nor close implementation requirements. The 11 reviewer-owned SPQ items are all unchecked, as required.

## Constitution alignment

No active constitutional conflict was established for SEC003–SEC004 preparation:

- Root `AGENTS.md` and `.specify/memory/constitution.md` are byte-identical to the baseline. The amendment remains a separately labeled proposal. No blanket prohibition is bypassed by preparing a parser that cannot approve findings or register a check.
- The pure parser accepts bytes, enum mode and process exit only; it has no policy/approval input, filesystem freshness claim, CLI, network effect, aggregate gate registration or success property accepting nonempty findings (`native-secret-policy.md:8–30`).
- Unsupported/malformed inputs fail with bounded domain errors and suppressed parser context. Native columns remain opaque; full source binding is separate. Same-fingerprint rows remain ordered and distinct; complete duplicate metadata fails (`:32–65`).
- The plan retains existing native gates and adds explicit locked Python 3.11 strict mypy/Pyright inspection of new scripts, cognitive complexity ≤5, cyclomatic complexity ≤10 and file-size bounds (`plan.md:368–371`). Focused checks are incremental evidence and cannot substitute for convergence.
- Direct user approval permits preparing the reviewed direction and implementation for review. This analysis does not replace human ownership of domain checklist decisions or grant any reserved approval.

The future trust/hosting design is not certified. In particular, protected transport, release provenance, merged-record mapping, actual human identities and a conclusive native stale-approval/merge-freshness barrier still need SEC006/SEC009/SEC010/SEC015 evidence. Their default-deny activation boundary is explicit, so their present absence does not prevent an isolated parser with no acceptance path.

## Method and evidence limits

Read the repository guidance, ratified constitution, slice index, managed `.agents/skills/speckit-analyze/SKILL.md`, appended spec/plan/tasks/model/research/quickstart, both security contracts, human checklist, owned gap entry and source-bound research notes. Ran the managed prerequisite command once with explicit `SPECIFY_FEATURE_DIRECTORY=specs/002-reproducible-runtime`; it returned the intended slice and all required artifacts. No extensions hook file exists.

Used CodeGraph before source exploration. Product `codegraph status` reported its index current, and the pinned reference was explored through CodeGraph. The native scanner schema/fingerprint/redaction claims were checked against official pinned [Finding definition](https://github.com/gitleaks/gitleaks/blob/v8.30.1/report/finding.go), [JSON reporter](https://github.com/gitleaks/gitleaks/blob/v8.30.1/report/json.go), [detector](https://github.com/gitleaks/gitleaks/blob/v8.30.1/detect/detect.go) and [configuration translation](https://github.com/gitleaks/gitleaks/blob/v8.30.1/config/config.go). These support the report boundary; they do not establish artifact provenance or universal scan completeness.

Verified that the original spec, plan, tasks, model, research, quickstart and author checklist remain exact byte prefixes of their current versions. All changes to those historical documents are append-only. No raw report, Match, Secret or candidate value was reproduced. No native repository rescan, external side effect or implementation check was performed. The complexity-budgets skill was consulted; there are no newly implemented functions to measure in this document-readiness review. Required analyzers remain implementation gates.

The reviewer made no repository edit. The author made the identified documentation correction; this report is the reviewer's only saved artifact.

## Metrics

- Functional requirements: 8.
- Buildable success criteria: 5.
- Total reviewed requirements: 13.
- Total SEC tasks: 15; preparation boxes checked: 2; remaining tasks: 13.
- Planned requirement coverage: 13/13 = 100%.
- Zero-coverage requirements: 0.
- Active ambiguity count: 0; repaired security ambiguity count: 1.
- Duplication count: 0.
- Active CRITICAL issues: 0; active HIGH issues: 0.
- Initial verdict: REVISE (1 HIGH). Final scoped verdict after independent reread: SHIP.

## Next actions

Proceed within the already approved preparation scope to SEC003 RED tests, then SEC004 pure parser. In addition to the specified schema/exit/bounds/coordinate/collision cases, place inert test sentinels in every retained textual identity field and verify they never appear in repr, exception rendering or captured ordinary output. Test malformed decoder/deep-input failures through the same bounded domain-error boundary. Verify frozen/ordered results and absence of approval, CLI and gate integration.

Rerun independent implementation review and the applicable checks on actual code. Do not advance this verdict to a claim about SEC005–SEC015, ratify the proposal, approve findings, publish a partial green gate or activate hosting. No remediation permission is requested: the sole identified written ambiguity has already been corrected by the authorized author, and the independent analysis is complete.

## Reviewed document SHA256

Paths are relative to `specs/002-reproducible-runtime/`.

| Document | SHA256 |
| --- | --- |
| spec.md | `185948bc603f52d2bcd887cb19a8c9829d8d0829f3df42cc63cda2c2fe7c8ec9` |
| plan.md | `ad3c2fa992766e4caf459da1d1bdd35e628248b76f2bbcacd6dc9785aac559af` |
| tasks.md | `4fd52880e3eb867302272eb80a8350f95f5248221986aeb3aa0ac10607adf69d` |
| data-model.md | `6fa8ffae07dc2ec367a191d2f4400a9df9160adaf3a43b0f3a27cd0505868549` |
| research.md | `dccc5d60b3668f947efc46f2e102602896998c162f3f391e6a8c621c7d0bb40b` |
| quickstart.md | `b95e1bbcf488ae30a406febb88966721f5eca7c21533d6e0972fa4d5c248e924` |
| contracts/native-secret-policy.md | `728c609613a65ddfdebcc90daefa09443cefa64712dec7873270bf44982b207d` |
| contracts/security-amendment-proposal.md | `35d2d68d0673da8d03a158e5d2adc1bc4e4d4036f0d3b69b5788444775da561b` |
| checklists/security-policy.md | `8c64c4608dfeeb1aa00d2ec1e6d89673c7000064d945c08a52ca1bdf994dc735` |
