# Specification Analysis Report

**Verdict: SHIP — authorized SEC005 source-binding preparation only.**

Independent managed `speckit-analyze` and adversarial review completed at 2026-10-17 00:50 UTC against HEAD `e793028e5ab097f2d09e373a9c8617ca8e3e3d37` plus the owned documentation refinements identified below. No SEC005 implementation files existed at review time. This verdict establishes readiness to write the planned failing controls and implementation for review; it does not mark SEC005 complete.

There are **0 CRITICAL, 0 HIGH, 0 MEDIUM and 1 LOW findings** against this preparation boundary. No new approval, finding disposition, governance activation, access change, hosted check, merge or full security acceptance is granted. SEC006–SEC015 and all human-owned checklist markers remain open.

## Findings

| ID | Category | Severity | Location(s) | Summary | Recommendation |
| --- | --- | --- | --- | --- | --- |
| I1 | Inconsistency | LOW | `contracts/native-source-binding.md:14`; `data-model.md:153` | The same executable/object-store location input is called `GitObjects` in the contract and `GitLocation` in the model. The declared fields and authority boundary agree, so this is naming drift rather than an unresolved security decision. | Use one name in the implementation and align the other document. No implementation or activation permission is required by this finding. |

No duplication, zero-coverage requirement, task-order contradiction or unresolved source-security design blocker was established. The earlier Git-only physical-line-count note remains preserved research; the later contract and research decision explicitly decline to adopt it, so it is not an active conflicting requirement.

## Coverage summary

Coverage means a task is assigned, not that its acceptance evidence exists. SEC002's document-preparation box is not used as substitute implementation coverage.

| Requirement key | Has task? | Task IDs | Notes |
| --- | --- | --- | --- |
| FR-023 | Yes | SEC005, SEC006, SEC008, SEC013 | SEC005 resolves exact declared source. Complete enrollment, native execution and integration remain later work. |
| FR-024 | Yes | SEC001, SEC003, SEC004, SEC005, SEC008, SEC012 | Full native coordinates/fields, exact mode-specific source and complete blob identity are specified; decisions remain separate. |
| FR-025 | Yes | SEC009, SEC010, SEC012, SEC015 | Independent human authentication and protected hosting are neither implemented nor inferred by source values. |
| FR-026 | Yes | SEC003, SEC004, SEC005, SEC006, SEC008, SEC013 | Source failures/bounds fail without partial identity; fresh complete scan/execution binding follows. |
| FR-027 | Yes | SEC001, SEC003, SEC004, SEC005, SEC008, SEC012 | SEC005 carries restricted text/bytes and constant diagnostics; full native/report/disposition presentation remains later. |
| FR-028 | Yes | SEC009, SEC011, SEC012, SEC013, SEC015 | Unsupported source fails. Proposed exception and actual individual decisions remain inactive. |
| FR-029 | Yes | SEC001, SEC007, SEC014 | Existing test IDs, donor/history and historical evidence are retained obligations. |
| FR-030 | Yes | SEC006, SEC009, SEC010, SEC013, SEC014, SEC015 | Whole approved Python/OS scope and actual independent hosting remain required. |
| SC-011 | Yes | SEC003, SEC008, SEC012, SEC013 | SEC005 cannot accept a controlled or real finding. |
| SC-012 | Yes | SEC003, SEC005, SEC006, SEC008, SEC009, SEC012, SEC013 | Source/tree/identity/process negatives are specified for SEC005; other negative domains stay assigned. |
| SC-013 | Yes | SEC005, SEC006, SEC007, SEC008, SEC013, SEC014 | Declared membership validation does not prove complete retained history or source enrollment. |
| SC-014 | Yes | SEC009, SEC010, SEC013, SEC015 | Source constructors provide no human, execution or App authority. |
| SC-015 | Yes | SEC010, SEC011, SEC012, SEC013, SEC014, SEC015 | Review, matrix/services, actual decisions/hosting, legitimate merge and resulting main remain required. |

**Unmapped tasks:** none among SEC001–SEC015. **Zero-coverage requirements:** none. The six US5 acceptance scenarios remain mapped to the parser/source, worker, issuer, governance, native-control and hosting/convergence tasks; no source-layer shortcut replaces that journey.

## Adversarial source-semantics checks

- **Direct types and complete chain:** the contract requires SHA1 storage, direct commit/tree/blob types and native OID recomputation from each complete raw body, without peeling tags or trusting a final blob alone (`native-source-binding.md:31–59`). The first commit header supplies the root pointer. Every traversed tree is independently measured, and the already verified bytes supply path selection. This addresses the preserved nested-tree corruption example and avoids a verify-then-reread lookup.
- **Canonical binary trees:** exact stored modes, raw framing, full consumption, duplicate raw names and strict Git order are explicit. Requested UTF-8 components are compared to raw names; unrelated sibling names need not decode. The order rule agrees with Git's directory-aware comparison, and the decoder is deliberately stricter than native canonicalization. See the pinned upstream [tree comparison](https://github.com/git/git/blob/v2.50.1/tree.c) and [raw decoder](https://github.com/git/git/blob/v2.50.1/tree-walk.c). Unsupported selected modes and malformed trees fail; they are not omitted from an accepted identity.
- **No fabricated tree binding:** `tree_bytes` is mandatory for tree rows and must equal the full regular-file blob at the exact scanned head; history rejects that argument and binds the exact native commit in the declared retained set (`:19–29`). A byte string establishes equality only. The protected worker must later prove where those bytes came from. Head-specific tree identities and mode separation prevent cross-head or history-to-tree carry.
- **Metadata is not provenance:** positive repository ID, policy digest, head and retained commits are frozen validated declarations (`:9–17`). The fresh protected store, exact source enrollment, retained-history completeness, immutable scanner export and authenticated execution remain worker qualifications. The source reader does not claim arbitrary candidate Git configuration or executable paths are trusted.
- **No hidden platform exclusion:** all commands require the fail-visible global `--no-lazy-fetch`, plus empty `GIT_ALLOW_PROTOCOL` and a newly constructed environment (`:63–68`). Git 2.46's [option implementation](https://github.com/git/git/blob/v2.46.0/git.c) establishes a concrete capability baseline. Vendor/artifact qualification is still required; the local Apple result does not qualify Ubuntu. The contract preserves Ubuntu/Linux, Apple and Python 3.11–3.13 obligations (`:101–107`), with capable Ubuntu Git a visible SEC006 dependency. Unsupported executables fail instead of silently weakening the transport boundary.
- **Bounds are actual failures:** 16 MiB bodies/hash input, 64-byte metadata, 100,000 tree entries, 256 components and 10 seconds per command are fixed. A resolution also caps raw bytes at 64 MiB, native commands at 1,024 and elapsed time at 30 seconds (`:70–81`). Streaming output/deadline enforcement, discarded stderr and kill/reap behavior are required. No prefix hashing or partial identity is permitted. These limits do not claim full native memory/process sandboxing; the worker still owns it.
- **Safe diagnostics:** candidate text and source bytes remain restricted, including repr and canonical evidence. Failures must use constant categories with suppressed context even inside unrelated private exception handlers (`:19–22,70–74,85–99`). This explicitly carries forward the corrected parser's private-context lesson. The planned controls include sentinel/repr/log/traceback and process-boundary failures.
- **Exact identity and serialization:** the concrete field inventory already exists in `evidence/security-policy/research/security-native-research.md:137–154`: schema version, repository, policy, mode, native/source commits, path, empty symlink path, rule, all four positions, native fingerprint, ordered tags, blob OID and full blob SHA256. The new contract preserves those fields, sorted-key compact UTF-8 JSON with `ensure_ascii=False`, no nonfinite values and the `polymarket-native-finding-v1` NUL-terminated domain prefix (`native-source-binding.md:83–90`). Set serialization/provenance is explicitly later work. The implementation should carry this exact inventory into its serializer and a golden-byte control, including the constant empty symlink field absent from `NativeFinding`'s retained data model.
- **Qualified source positions preserved:** the existing LF count plus one upper bound remains, including a possible terminal empty position; columns remain opaque positive metadata. The new design expressly rejects the Git-only note's alternate physical-line count (`:50–55`; `research.md:290–295`). The pinned [Gitleaks location implementation](https://github.com/gitleaks/gitleaks/blob/v8.30.1/detect/location.go) supports retaining byte-position behavior and its last-line fallback; it does not independently qualify every detector/decoder. SEC008 still owns complete scanner-coordinate controls.

These are written design checks, not implementation test results. Preserved probe summaries were inspected but not rerun, and their local preparation results are not treated as protected provenance or a full OS matrix.

## Constitution alignment

No constitutional conflict was established within this boundary. Root governance is unchanged. The approved direction explicitly permits preparation for review, and the new source code will have no CLI, gate registration or finding-acceptance path (`tasks.md:519–530`). This does not require governance activation merely to prepare code.

The refinement keeps research-only behavior, no external effects, compatibility and all required gates. Failing meaningful controls precede implementation. The plan keeps independent strict mypy/Pyright at the 3.11 floor, full native checks, cognitive complexity at most 5, cyclomatic complexity at most 10, and at most 399 lines per new file. The complexity-budgets guidance was consulted; no newly implemented functions exist to measure in this document-readiness review. Analyzer execution remains required on the implementation, without thresholds, exclusions or baseline changes.

Human domain checklist ownership remains intact: all 11 SPQ criteria are unchecked. Direction preparation, actual implementation verification, amendment ratification, per-finding decisions, trusted-service provisioning, required hosting and merge remain distinct.

## Managed method and evidence limits

Read repository AGENTS, constitution, slice index, managed analyze skill, US5/FR-023–030/SC-011–015, task/refinement ledger, plan/model/research/quickstart, both native contracts, owned gap, prior managed analysis and native/source research. Explicitly ran once:

```bash
SPECIFY_FEATURE_DIRECTORY=specs/002-reproducible-runtime .specify/scripts/bash/check-prerequisites.sh --json --require-spec --require-tasks --include-tasks
```

It selected slice 002 and returned the required documents. `.specify/extensions.yml` was absent before and after analysis. CodeGraph was used before source exploration; its index reported current. Existing parser models/schema were inspected to check the integration contract, not requalified beyond their independently accepted SEC003/004 checkpoint. No new source code, native repository scan, service test, external action or repository edit was performed. This externally saved report is the reviewer's only write.

`git diff --check` passed. The five modified historical documents are exact append-only extensions of HEAD; the specification, root AGENTS and constitution remain byte-identical. No raw native report, Match/Secret or repository candidate value is reproduced here.

## Metrics

- Functional requirements: 8; buildable success criteria: 5; total requirements: 13.
- SEC tasks: 15; checked preparation/parser tasks: 4; open tasks: 11.
- Planned requirement coverage: 13/13 = 100%; unmapped tasks: 0.
- Active ambiguity: 0; duplication: 0; inconsistency: 1 LOW.
- CRITICAL: 0; HIGH: 0; MEDIUM: 0; LOW: 1.

## Next actions

Proceed within existing approval to SEC005's failing real-Git source, canonical-tree, identity and bounded-process controls, then the five owned source modules. Align I1 during that preparation. Pin the exact identity keys/bytes in a golden control and include malformed direct-constructor inputs in safe-error tests; a typed value is not authenticated or intrinsically valid evidence.

Before checking SEC005 complete, run the specified Python 3.11–3.13 controls, full native static/types/complexity checks, retained-ID comparison, committed-source compatibility and independent exact-code review. Keep SEC006–SEC015, full OS/service qualification, current-tree export provenance, complete retained-history enrollment, authentic scan/report binding, individual decisions and actual required hosting visibly open. No remediation permission request is needed for this completed read-only analysis.

## Reviewed document SHA256

Paths below are relative to `specs/002-reproducible-runtime/`.

| Document | SHA256 |
| --- | --- |
| `spec.md` | `185948bc603f52d2bcd887cb19a8c9829d8d0829f3df42cc63cda2c2fe7c8ec9` |
| `plan.md` | `2e88440a8dfcf8e467885386c8934a38794077a8597f30ac7da59b6fe6e317bb` |
| `tasks.md` | `2b109b1a1b9a6708880f84a69998cb406495bff7d7ae9c3e9140283b05d94697` |
| `data-model.md` | `0f8995432adea8c53cf61dd9d6b0c387ecd9c9f663e788518f6e460e5fd35f3a` |
| `research.md` | `2d9eada94eabb629365179fd96c02f5c4acba8ae7ec89e39e0faa9cfa83e4748` |
| `quickstart.md` | `a5e25c44be87f8ca61597c8ac3975ca050b533560f8a223d181b7a5068951727` |
| `contracts/native-source-binding.md` | `4c01d4801418076b7ca79783c8236a8bd0adfa1d66f2935010ac90f93436a009` |
| `contracts/native-secret-policy.md` | `728c609613a65ddfdebcc90daefa09443cefa64712dec7873270bf44982b207d` |
| `evidence/security-policy/research/security-native-research.md` | `b8a1cc468e5d92a1a12716e9b3235d43d27ed69afb5c30934f7c0747e3447275` |
| `evidence/security-policy/research/source-binding/research.md` | `d49a91947429e382c954ce0ff71061eb3eed38a44fde065ec3e621e68f763c10` |
| `evidence/security-policy/research/source-binding/probe-results.json` | `2770eafb0ca8aedc5a792dd50b694b59f6fcf82bb1ce7f398ac7f985552a2132` |
| `evidence/security-policy/research/source-binding/native-hash-results.json` | `26c6e1291dc36d8b7a3a7f512d86e29d445fb722f31d3c276d7e5ab424ced032` |
| `evidence/security-policy/research/source-binding/bounds-results.json` | `1f5887ea00569dc825a4e7c2e3736a4b239b13ace5d2c2db533a0b6cd691f084` |
| `checklists/security-policy.md` | `8c64c4608dfeeb1aa00d2ec1e6d89673c7000064d945c08a52ca1bdf994dc735` |
