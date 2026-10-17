# Native secret evidence and proposed acceptance contract

Status: preparation authorized by the direction receipt; no finding accepted, no hosting activated,
and no exact governance text ratified. This is the Polymarket variation proposed against the pinned
reference's empty-report rule. Existing ratified governance remains in force. Story5 owns FR-023–030
and SC-011–015. This contract does not vary OSV, Opengrep or any other quality gate.

## Incremental boundaries

1. The first implementation is a pure native-report parser. It accepts bytes, native mode and process
   exit, and returns immutable metadata or a constant safe error. It has no approval input, CLI,
   filesystem freshness claim, aggregate gate registration or ability to accept a finding.
2. Source binding and the native worker follow. Their receipt proves scope/execution separately
   from parser correctness. A syntactically valid report is never authenticated scan evidence.
3. Protected human-policy/issuer integration follows only after the trust adapter and deployment
   conditions below have conclusive controls. Unconfigured trust always rejects acceptance.
4. Real findings require separate authenticated individual decisions. Complete native/hosted/main
   acceptance, supported matrix and legitimate merge remain required before convergence.

The direct direction approval authorizes preparing these increments for review. It does not grant
per-finding acceptance or access changes. No partially implemented control may be published as a
passing required secrets gate. Requested gate registration and its complete remediation stay together.

## Native parser interface (first increment)

`scripts._secret_report.parse_report(payload: bytes, mode: ScanMode, exit_code: int) -> NativeReport`
is a pure boundary. `ScanMode` is an enum containing `dir` and `git`. `NativeReport` contains the raw
byte SHA256 and ordered immutable findings. Native clean is exactly exit0 with an empty array;
native findings is exactly exit42 with a nonempty array. Every other exit/array relationship fails.
There is no disposition/policy result or success property meaning that nonempty findings are accepted.

The parser requires UTF-8 JSON, one root array, no duplicate object keys/nonfinite numbers/trailing
data, at most16MiB and100,000 rows. Bounds are failures, never truncation. The exact pinned8.30.1
required keys are RuleID, Description, StartLine, EndLine, StartColumn, EndColumn, Match, Secret,
File, SymlinkFile, Commit, Entropy, Author, Email, Date, Message, Tags and Fingerprint. Link is the
only optional key. Unknown/missing keys and wrong types fail. Booleans are not numeric fields.
Entropy is finite; Tags is an ordered tuple of strings; Secret must be the qualified literal
redaction marker `REDACTED`. Match and other textual native fields are type-checked then discarded
from the safe model. They must never enter exception messages, reprs, assertion diagnostics or logs.
The untouched native report stays in restricted run evidence, outside normal summaries.

Restricted finding metadata contains mode, exact commit/path/rule/fingerprint, all four coordinates and
tags. SymlinkFile must be empty in the enrolled v1 scope. Rule/path/fingerprint must be nonempty.
History Commit is full lowercase40-hex SHA1; tree Commit is empty. Fingerprint must equal the pinned
format computed from those fields (history `commit:path:rule:start_line`, tree `path:rule:start_line`).
Paths are exact relative POSIX paths: reject absolute paths, empty/dot/parent components, backslashes
and control characters. Reject unexpected forms instead of normalizing them into approval matches.

All coordinates are positive integers; end line cannot precede start line. On one line, end column
cannot precede start column. Multiline end column may precede start column. Native columns are opaque
exact metadata, not Python character offsets or source slices. Zero columns are unqualified and fail
v1; do not claim the scanner can never emit them. Source binding later checks line bounds against
the complete independently resolved blob without invented column-length checks. Two rows with the
same fingerprint but different coordinates are retained. Duplicate complete safe metadata fails as
ambiguous; rows are never silently deduplicated.

Path/rule/fingerprint/tags remain arbitrary untrusted text, not intrinsically safe display data.
Preserve them only for restricted identity processing/evidence. Ordinary output uses fixed categories,
enum mode, counts and digests; never interpolate those native text fields. Finding repr omits every
candidate-controlled text field, and report repr omits findings. A later summary renderer must obey
the same rule; serialization of internal models is not an authorized ordinary-output renderer.

Errors are a single domain exception with a bounded category message and suppressed parser context.
No raw JSON decoder error, candidate value or input excerpt may be rendered. This does not prevent a
privileged debugger from inspecting input memory; production ordinary output must not do so.

## Exact source identity and worker evidence

Full `FindingIdentityV1` adds immutable native repository ID, protected scanner-policy SHA256,
source commit, Git blob OID and independently calculated full-blob SHA256. It keeps every parser
identity field including tags. Historical source commit must equal the native finding commit and
belong to the frozen retained-history manifest. Tree source commit is the exact scanned candidate
head; source bytes must equal that head's regular-file blob. Tree approval is head-specific; no
cross-head carry is granted, even for an unchanged blob. History never approves a tree counterpart.
Symlinks/submodules/unsupported source resolution fail pending an explicit qualified extension.

Canonical identity/set serialization uses sorted-key compact UTF-8 JSON, no nonfinite numbers,
domain-separated SHA256 and sorted full identities; preserve row order separately in native output.
The source hash covers complete file bytes, never Secret/Match or a coordinate-derived substring.
The existing19-row adjudication lacks complete fields and is not an approval-ready registry.

The protected worker verifies a platform-qualified Gitleaks8.30.1 release artifact, exact protected
configuration/default rules, empty ignore bytes, immutable command policy and trusted Git. Tree and
history run in a credential-free immutable snapshot; candidate code/config/hooks/actions are data.
Do not execute candidate setup/install hooks, submodules, actions or shared executable caches.
Configuration and evidence reside outside the source snapshot. The issuer App token never enters
the worker. Environment and Git configuration are trusted allowlists, not candidate inherited state.

Both native `dir .` and `git .` commands require redaction100, ignore-inline-allow, explicit protected
config/empty ignore paths, findings exit42, JSON report, max-target-megabytes0, qualified decode/archive
policy and no history/path/date/count narrowing. Retain upstream native default detectors. The full
history log policy enrolls all frozen retained refs, including PR129's original donor tip; main-only
is diagnostic. No generated-directory exemption may hide authored source. Qualify native warning/
skip behavior; unreadable or unsupported enrolled content is incomplete evidence, never clean.

`ScanRunReceiptV1` binds repository/head/base/tree, protected worker/policy/code, native run/attempt/job,
scanner executable/config/command/environment digests, frozen ref/tip/reachable-commit and file/blob
manifests, before/after snapshot agreement, exact mode exits/completion/timeouts and fresh report byte
digests/sizes/counts. Unique output directories must start empty; missing, stale, symlinked, malformed
or oversized reports fail. Collect both modes even when one returns42. Fatal failure stays failure
regardless of any old disposition. A candidate-supplied manifest or artifact hash grants no provenance.

## Protected approval and lifecycle

Use the reference's protected-base repository-bound GET/pagination/current-head/current-base logic
and dedicated App publisher, extending it with independently authenticated worker evidence and
disposition digests. Upstream currently validates native metadata, not these artifacts; it is not
already an authenticated disposition service. Candidate CI job names or success are insufficient.

An empty protected eligible-human-ID set is the default and rejects all approvals. Provisioned policy
must name immutable human IDs, require native User type, current maintain/admin role and an independent
non-author/non-committer reviewer. Bots, machine automation, candidate JSON/flags, synthetic metadata
and this chat receipt cannot qualify. Human approver credentials must be kept out of agent/worker
automation; attribution to a User account alone is not proof of manual human action.

A native APPROVED review must bind the exact proposal head and canonical disposition-set digest with
substantive false-positive rationale. The issuer fetches the reviewed blob at the review commit and
recomputes the digest. Proposed data may be inspected by protected validator code but cannot replace
that code, its configuration or its authority. No real credential, including a claimed revoked one,
qualifies under this exception. Rationale/source role is evidence for a human, never an automatic rule.

States are proposed → authenticated for one head → active after legitimate protected merge, or
revoked/stale/superseded. Active records require native merged-PR and protected-registry provenance,
the exact reviewed content/digest and still-valid approval/identity/policy. Do not reuse the upstream
open-PR-only validator unchanged. Allowed merge methods need exact content/provenance tests; mere
ancestry is insufficient for squash/rebase. Until that mapping qualifies, those methods cannot activate
records. Never infer authority merely because data appears on main.

Dismissal, later substantive rejection, loss of role, policy/scanner/source/head mismatch or unavailable
metadata invalidates acceptance. Protected event delivery and fresh merge eligibility must replace an
old success with failure; a dispatch-only reevaluation test does not prove revocation delivery.
Activation requires a conclusive native stale-approval-before-merge rejection control and verified
merge-freshness barrier. No report TTL or asynchronous webhook alone is accepted as that barrier.
Until the hosting mechanism proves the barrier, nonempty-report acceptance stays disabled.

## Bootstrap and activation gates

Prepare pure code/negative controls and empty policy first; local results say preparation. Then prepare
the exact amended text and complete worker/issuer implementation for review. There is no bootstrap
pass from an empty registry, operator Boolean, candidate report or unapproved finding. Existing real
native findings keep failing. Establish the independently protected validator revision through the
legitimately reviewed/approved governance and publication path; never privilege candidate code to
bootstrap itself. If that path requires a staged repository-policy change, present its exact diff and
remaining failures for explicit approval before acting; no branch-protection bypass is implied.

Separate authorized activation must establish/read back actual repository/workflow/App identities,
eligible human IDs, protected source/environment/key handling, retained-history lower bound,
least permissions, required check bound to its App source, review/bypass policy and the proven freshness
barrier. Missing/unqualified values cause failure. No App/access/protection mutation is authorized now.

Native reports remain visible; separate deterministic metadata summaries retain every row and decision.
No candidate values or native Match/Secret enter normal output. Source-role analysis establishes neither
issuer provenance nor validity. Exact individual findings, implementation acceptance, required hosting,
merge and resulting-main verification remain distinct. See the tasks and quickstart for all required
native negative controls, donor preservation and full convergence; the parser alone closes none of them.
