# Native Gitleaks report and disposition boundary

Research prepared 2026-10-16/17. Repository source remains unchanged at
`523b0a35466e799c36e85e3bc5ea046cf3263c31` on
`codex/polymarket-secrets-reconciliation`. Reference source is
`relentless-ci-python@b8b191341e3061d7600bb1666f39da5374a492d9`.

**State: implementation preparation only.** Patrick approved preparation of the
reviewed policy direction. No individual finding, acceptance registry, implementation,
merge, or complete security control is approved by this research.

## 1. Evidence and authority

- **Primary, read this run:** direction approval receipt, unchanged decision packet,
  amendment draft, metadata structure of the 19-row adjudication record, existing native
  report metadata, reference scanner/report/command code, local native CLI help, and
  independently resolved Git source blobs.
- **Copied primary receipt, not rerun:** `outputs/polymarket-security-exit-qualification.json`
  establishes native 8.30.1 exits: clean 0, current/history findings 42 when explicitly
  configured, fatal missing configuration 1. Its installed executable digest is observed;
  release provenance is not independently established by that receipt.
- **Synthetic, run this research:** narrow ASCII, UTF-8, multiline, duplicate-fingerprint,
  EOF/newline/CRLF and decoded-input controls, solely to resolve coordinate/identity
  questions. These do not adjudicate product findings or prove the whole security control.
- **Derived proposals:** the strict schema and acceptance rules below. They must enter
  the managed Spec Kit packet and reserved human implementation validation.

The direction approval binds decision-packet SHA256
`09005ab481006d8a3c2f1226d71f5930922afbe83525df925db82158d3ae0127`.
It explicitly excludes approval of H0-H10, merging, history/ref removal, detector
exclusions, credential verification/rotation and external messages. Do not reuse its
approval as finding acceptance.

## 2. Minimal separation of artifacts

Use three distinct artifacts; none substitutes for another:

1. **Native scan evidence:** untouched native redacted JSON reports for both `git` and
   `dir`, plus trusted execution receipts and source-scope manifests. Retain every row.
2. **Proposed disposition identities:** candidate-reviewable metadata only, naming
   exact findings and source blobs. This has no authority to grant acceptance.
3. **Authenticated approved dispositions:** obtained from the protected policy boundary,
   bound to an exact protected policy revision and authenticated native human review.
   The candidate cannot author its own acceptance authority.

Produce a fourth, deterministic **result summary** by joining artifacts 1 and 3 by exact
identity. Preserve the native finding count, every finding's identity and its separate
disposition. Do not replace the report with an empty array, remove approved rows, or call
an approved nonempty native report a clean native scan.

The existing adjudication JSON is source-role review evidence only. It lacks end-line and
column coordinates, does not bind the full scanner/command policy, and represents tree
source revision as a commit even though native tree `Commit` is empty. H0/T0-style labels,
`corresponding_history_id`, classification prose and `policy_acceptance` text are not
approval keys. Construct any future proposed registry from fresh validated native rows
and independently resolved blobs, then obtain separate exact human approval.

## 3. Scanner and command policy binding

Define one protected `ScannerPolicyV1` object, serialized with deterministic JSON and
hashed as `scanner_policy_sha256`. Its required fields are:

| Field | Required meaning |
| --- | --- |
| `schema_version` | Exact supported policy schema, initially `1` |
| `scanner_name`, `scanner_version` | Exactly `gitleaks`, `8.30.1` |
| `executable_sha256`, platform | Qualified native artifact identity for the actual platform |
| `configuration_sha256` | Digest of the exact protected configuration bytes |
| `ignore_file_sha256` | Digest of the exact empty ignore file |
| `reference_revision` | Exact reviewed policy-source revision |
| `command_policy` | Exact argv templates, cwd policy, environment policy and scan modes |
| `scope_policy_version` | Exact current-tree and retained-history enrollment contract |

Do not put a timestamp, run ID, output filename, temporary absolute path or current head
in this reusable policy digest. Those belong to the run receipt. The policy changes if
the scanner, native built-in rules, configuration, ignore bytes, flags, environment or
scope contract changes. A version string alone is insufficient.

Keep the native default detectors and reference configuration. Both commands use an
immutable source root as cwd and literal target `.`; this makes native relative paths and
fingerprints stable. Require explicit `--redact=100`, `--ignore-gitleaks-allow`, the protected
config and empty ignore-file paths, `--exit-code 42`, and JSON report output to a separate
fresh evidence directory. Set explicit `--max-target-megabytes 0` so file-size skipping
cannot be introduced by a changed default. Bind native decode/archive settings as policy
even where retaining the pinned defaults (currently decode depth 5, archive depth 0).
The approved contract does not silently promise archive traversal.

For history, bind an explicit full-history/all-enrolled-refs log policy. A proposed argv
value is `--log-opts=--all --full-history --no-ext-diff --no-textconv`, qualified against the
pinned native binary and trusted Git. Never add pre-commit, staged, date, path, count or
revision-range narrowing. A main-only diagnostic is additional evidence, not replacement.
No baseline, enabled-rule subset, inline allow, nonempty ignore file or source-path
exception may enter the command. The reference already excludes protected generated
directories; do not expand those exclusions or enroll authored content inside them.

Use a protected environment allowlist: no candidate-controlled PATH, Git configuration,
Gitleaks configuration environment variables, external diff/textconv programs or shell
command expansion. Do not execute candidate application code to obtain evidence. Exact
worker isolation and executable release verification belong to the trusted-boundary
design; the local installed binary digest alone does not finish that work.

## 4. Strict native report parser

Parse once into immutable typed `NativeTreeFinding` or `NativeHistoryFinding` objects.
Do not pass arbitrary dictionaries into approval matching.

- Require a newly created regular JSON report file in the unique run directory; reject
  symlinks, preexisting output and paths outside that directory. Read the exact bytes and
  retain their SHA256. A missing file or zero-byte file is not an empty report.
- Require UTF-8 JSON with one root array, no trailing data, duplicate object keys,
  nonstandard NaN/Infinity values or coercion of booleans/numeric strings to integers.
- Proposed capacity limits: 16 MiB per native JSON report and 100,000 rows. Overflow is a
  visible failed/incomplete scan, never truncation or acceptance. These are explicit new
  implementation limits for validation, not existing approved defaults.
- Pin the observed 8.30.1 field schema. Required fields are `RuleID`, `Description`,
  `StartLine`, `EndLine`, `StartColumn`, `EndColumn`, `Match`, `Secret`, `File`,
  `SymlinkFile`, `Commit`, `Entropy`, `Author`, `Email`, `Date`, `Message`, `Tags`,
  `Fingerprint`; `Link` is an optional string (observed in history, absent in tree).
  Reject missing, unknown or incorrectly typed fields rather than silently ignoring a
  schema change. `Entropy` must be a finite JSON number; `Tags` is an array of strings;
  other metadata fields are strings. Empty strings are permitted only where native mode
  legitimately emits them (not rule, path or fingerprint).
- The pinned redacted reports have `Secret` equal to the native redaction marker.
  `Match` is not wholly replaced: it retains source context. Therefore do not render,
  copy, hash into a disposition identity, or export either field into ordinary summaries.
  Keep the unmodified native redacted report as controlled evidence; do not claim every
  surrounding source byte has been sanitized merely because `--redact=100` was used.
- Never use author, email, commit message, description or links as authenticity evidence.
  They are opaque source-controlled metadata and are not required in the safe summary.

`RuleID`, path, all four native coordinates, native fingerprint and mode-specific commit
are retained exactly. No trimming, case folding, Unicode normalization, path rewriting or
line/column offset correction is allowed to broaden an approved identity.

## 5. Exact finding identity and independent source resolution

Required common `FindingIdentityV1` fields:

```text
schema_version = 1
repository_id                      # independently obtained immutable native repository ID
scanner_policy_sha256
mode = "dir" | "git"
native_commit                      # exact native value; empty only for dir
source_commit                      # independently selected immutable source revision
path                               # exact native repository-relative File
symlink_path                       # exact native SymlinkFile; empty in enrolled v1 scope
rule_id
start_line, end_line, start_column, end_column
native_fingerprint
tags                               # exact native array; no grammar-level approval
source_blob_oid                    # independently resolved Git blob object ID
source_blob_sha256                 # SHA256 of the exact complete source bytes
```

The protected run receipt separately binds the current candidate head, base, repository,
source tree and complete history manifest. This avoids making reusable historical
identities depend on a scan's timestamp or report row ordering.

For **history**, require a full 40-hex SHA1 native commit in this repository's currently
verified SHA1 object format; independently verify its commit object and membership in the
frozen enrolled history manifest. Require `source_commit == native_commit`. Resolve the
exact path in that commit's tree to a regular-file blob (`100644` or `100755`), read with
trusted Git, and calculate blob SHA256 independently. Do not trust a candidate-supplied
blob hash or find similar text at HEAD. A missing commit/path, unreadable/non-blob object,
unqualified symlink/submodule or disagreement fails acceptance; do not silently try a
parent or alternate revision. Any future removed-line/parent mapping requires separate
native qualification and an explicit schema extension.

For **tree**, require native `Commit == ""`; independently select the exact scanned
candidate commit as `source_commit`, and resolve that commit's path/blob. Require the
immutable scanner input's file bytes to equal that blob. The initial proposal makes tree
approval head-specific as well as blob-specific. Carrying tree dispositions across later
heads, even for unchanged blobs, would need an explicit protected-policy decision; it is
not implied by historical acceptance.

Use exact repository-relative POSIX paths from the protected `.` invocation. Reject
absolute paths, NUL/control characters, backslashes, empty components, `.`/`..`
components, or lexical escape from the source root. Reject rather than normalize an
unexpected absolute scanner path. Validate the enrolled Git path and object directly;
do not resolve a candidate-controlled filesystem symlink. The current v1 source policy
requires `SymlinkFile == ""`. New object formats/platforms require explicit qualification.

Validate the native fingerprint against the pinned format using the already parsed exact
fields: history `commit:path:rule:start_line`; tree `path:rule:start_line`. Do not split a
fingerprint and infer authority from it. It is a consistency field, not a unique finding ID.

An optional computed `identity_sha256` is the SHA256 of canonical UTF-8 JSON for this
entire typed identity. It is a convenience key; retain the complete fields for inspection.
Do not substitute a hash of `Secret` or `Match`.

## 6. Qualified coordinate invariants and ambiguity

Metadata from all 19 existing report rows was inspected without candidate values.
Multiple rows have native `EndColumn` equal to source-line length plus one. The historical
PAT's native columns differ by one from its literal source positions. Controlled native
8.30.1 cases reproduced the reasons not to equate columns with Python string slices:

| Control | Observed native result |
| --- | --- |
| ASCII first line | Known PAT source column 10; native 10, end 49 |
| UTF-8 prefix | Known PAT character column 13; UTF-8 byte/native column 17 |
| PAT after initial newline | Actual character/byte column 1; native column 2 |
| Multiline assignment | Lines 1-2, end column 39 for a 38-character end line |
| Indented multiline assignment | Start column 81, end column 39, lines 1-2 |
| Two PAT-shaped fixtures on one line | Two retained rows, identical native fingerprint,
  different columns (1-40 and 42-81) |
| Encoded fixture | Outer-source coordinates with native decoded/base64 tags |

Consequently:

- Require integer coordinates with `start_line >= 1`, `end_line >= start_line`, and
  positive start/end columns. For a single-line range, require `end_column >= start_column`.
  For multiline ranges, end column may be smaller than start column.
- Validate line range against independently resolved complete bytes using the pinned
  LF-oriented source convention, preserving a possible terminal empty line. Do not
  require columns to be within character-count or UTF-8-byte line lengths; do not slice,
  adjust or rederive matched text from them. Full-blob binding provides the source identity.
- Zero columns were observed in **none** of the real rows or the qualified ASCII,
  Unicode, multiline, EOF, CRLF, blank-prefix and decoded controls. The proposed qualified
  v1 schema rejects zero as unsupported native metadata and fails closed. This is not a
  claim that every possible native detector/decoder can never emit zero. Supporting a
  demonstrated native zero-column case requires new evidence and a versioned parser
  decision; do not coerce zero to one or treat it as an unspecified wildcard.
- Reject duplicate **complete identities**, even byte-identical rows; never silently
  deduplicate them. Reject conflicting metadata/approval records for one complete key.
- A repeated native fingerprint alone is valid when the complete identities differ.
  Preserve both rows and match each separately. A disposition matching one cannot accept
  its same-line neighbor. If the available native metadata cannot distinguish two rows,
  fail as ambiguous rather than approving a bucket or credential grammar.

The controls used the real native scanner and reference config; no custom detector was
invented. Disposable fixture repositories, reports and raw logs were removed. Safe
metadata receipts remain in `work/security-native-coordinate-controls.json`,
`work/security-native-coordinate-edge-controls.json`, and
`work/security-native-multiline-order-control.json`.

## 7. Native exit/report state machine

Do not weaken the shared generic command runner to accept arbitrary nonzero exits. Add
an operation-specific native scan result with these exhaustive states:

| Observed process outcome | Required report | Meaning |
| --- | --- | --- |
| Exit 0 | Fresh valid `[]` | Native clean for that mode, subject to source/scope checks |
| Exit 42 | Fresh valid array with at least one row | Native findings; retain every row,
  then evaluate exact external dispositions |
| Exit 0 plus nonempty report | Any | Inconsistent, fail |
| Exit 42 plus empty/missing report | Any | Inconsistent/incomplete, fail |
| Exit 1 or any other exit | Even a valid report with approved rows | Scanner failure, fail |
| Signal, timeout, spawn failure or incomplete process | Any | Scanner failure, fail |
| Missing, stale, malformed, duplicate, ambiguous, oversized or source-mismatched report | Any exit | Evidence failure, fail |

Both modes are required. A findings result in one mode must not prevent collection of
the other mode merely because the generic runner raises on nonzero status. Their native
42 statuses remain visible. The aggregate control may pass under the **explicit Polymarket
policy variation** only if both scans completed consistently, source/scope bindings hold,
and every native row has a matching independently authenticated permitted disposition.
That is not native zero-report acceptance. An unreviewed, changed or real-credential row
fails. No approval exists for any current H/T row today.

## 8. Completeness and freshness require run evidence

A native JSON finding array has no complete scanned-file/commit inventory and no trusted
run nonce or completion timestamp. Its emptiness alone cannot prove full coverage.
Require a trusted `ScanRunReceiptV1` containing:

- independently obtained repository/head/base and protected validator/policy revisions;
- unique workflow run/attempt or equivalent trusted run nonce, start/completion records,
  exact process exit and timeout/cancellation status for each native mode;
- scanner-policy digest, actual executable/configuration/ignore-byte digests and exact
  executed argv/cwd/environment-policy identity;
- immutable source tree OID and independently generated file/path/blob manifest;
- frozen retained ref/tip manifest and complete reachable-commit manifest with hashes;
- each native report's exact byte digest, size, parsed row count and source-resolved
  identity-set digest; source snapshot digests before and after scanning must agree;
- trusted transport binding between worker evidence and the final protected issuer.

Use a new output directory per run, with no old report available as a fallback. Mtime,
candidate timestamps, a familiar filename or a candidate-written `completed=true` flag
cannot establish freshness. The issuer consumes evidence from the current exact run,
head and policy; mismatches fail. A wall-clock TTL alone cannot replace those bindings.

"All available retained history" must name a concrete independently enrolled ref set,
including retained PR129/donor tips, and its reachable commit set. Current checkout is
not shallow; preserve that requirement. A CI fetch of only head/main omits the three
known off-main findings and cannot satisfy the contract. Do not remove refs to shrink
the manifest. Freeze the object/ref snapshot during scanning and fail on scope loss or
unavailable objects. Exact protected fetch/enrollment and worker attestation are owned
by the trusted-boundary design, not asserted from the native report.

Use an isolated immutable export for tree scanning, enrolling all committed regular
files outside the already protected generated roots, including non-Python files. Verify
their bytes/readability independently. Git ignores cannot narrow that manifest. Keep
report outputs outside the scanned export. An authored input hidden in an excluded
generated directory is a source-enrollment defect, not a scanner exemption.

## 9. Historical versus tree dispositions

Historical approval names a particular introduction/diff finding at its exact native
commit/path/blob/range/rule/policy. It neither approves a current-tree counterpart nor a
new occurrence. A separate tree identity and authenticated human disposition are needed.
The current eight tree rows correspond to historical rows, but that relationship is
explanatory metadata only. A one-to-many relation is possible; never infer acceptance
through `corresponding_history_id`.

Approved dispositions may only represent the narrow false-positive role covered by the
amendment. A real credential, including one merely declared revoked, is outside that
exception. Proposed/source-role classifications remain visibly unapproved until exact
external authentication succeeds. Reject multiple contradictory disposition records and
unexpected identities in an approval set; do not choose a last record or approximate match.

## 10. Required negative controls and unresolved issues

Already supported by existing exit receipt: native clean, native current-tree finding,
clean tree after removal, history-only finding, fatal missing configuration. Coordinate
and collision controls above add real native evidence for parser design. Still required
before implementation acceptance:

1. Native report with two same-fingerprint/same-line findings and only one approved:
   retain both; second fails. Duplicate exact rows, conflicting rows, duplicate JSON
   keys, wrong types, zero/negative coordinates and wrong source digests fail.
2. Approve a history identity only; its tree counterpart still fails. Change head, blob,
   path, rule, range, tag, scanner/config/argv policy or repository; approval fails.
3. Missing, malformed, oversized, stale or symlink reports; report/exit disagreement;
   scanner fatal despite approved old rows; timeout; and swallowed status all fail.
4. Same-line adjacent unapproved content, inserted inline allow comments, nonempty ignore
   data, Git ignores, excluded authored paths, shallow/omitted donor history and changing
   refs/files during the run cannot create an accepted result.
5. Candidate-authored approval, revoked/dismissed human review, stale protected policy,
   wrong App/repository/head/base/run, artifact substitution and privileged execution of
   candidate code must fail at the independent trust boundary.

Concrete open matters, without claiming completion:

- The local installed executable/version/digest and coordinate/exit behavior are observed;
  exact release provenance and platform-specific artifact qualification remain open.
- Native JSON alone lacks coverage and skipped-file/error inventories. The immutable
  input manifest, readability checks, exact command and trusted worker must be qualified
  together. Qualify native handling of unreadable inputs, Git ignores, unsupported files,
  decoded inputs and any warning-only skips; do not treat an array plus exit as universal
  proof of complete scanning or invent a fallback detector to fill the gap.
- Zero-column behavior outside the qualified cases and a history finding requiring a
  source revision other than its native commit are unsupported and fail closed pending
  targeted evidence, rather than guessed normalization.
- Cross-head reuse of a tree disposition is deliberately not granted by this proposal.
  Any less strict lifetime must be an explicit human policy choice.
- There is no acceptance-ready registry or trusted approval mechanism from the existing
  H/T adjudication file. All native findings remain failing until those prerequisites and
  legitimate exact approvals exist.
- OSV, Opengrep, other quality gates, hosted required-check verification and merge approval
  remain separate obligations. This research does not make a whole-security completion
  claim.

## Source pointers

- `outputs/polymarket-security-policy-decision.md`, `polymarket-security-direction-approval.json`,
  `polymarket-security-amendment-draft.md`, `polymarket-security-exit-qualification.json`.
- `outputs/polymarket-secrets-adjudication.json`: metadata structure and counts only;
  `work/polymarket-native-history-redacted.json` and `work/polymarket-native-tree-redacted.json`:
  metadata only, never candidate values.
- Reference `quality/security.py:33-57`: native git/dir invocation and empty-report rule;
  `:116-125`: deleting previous reports before verification.
- Reference `quality/security_reports.py:13-22`: JSON loading and empty-array acceptance.
- Reference `quality/commands.py:51-67`: bounded generic process runner rejects any nonzero.
- Reference `quality/report_data.py:16-39`: typed object/array/text boundaries and explicit
  duplicate rejection pattern.
- Reference `quality/source_scope.py:9-23`, `:42-59`, `:82-96`: generated-root/source
  enrollment and symlink policy; `quality/gitleaks.toml`, empty `quality/gitleaks.ignore`;
  `mise.toml:3` and `mise.lock:59-72`: pinned native scanner.
- Local `/opt/homebrew/bin/gitleaks git --help`, `dir --help`, `--help`: supported native
  flags, default decoding/archive behavior, redaction and findings-only exit flag.
