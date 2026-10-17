# Secret scanner: policy decision for preserved history

Prepared 2026-10-16. **Decision pending; scanner acceptance has not changed.**

The pinned Relentless reference cannot pass on this repository with its existing historical
test fixtures and its literal empty-report policy. The evidence supports a narrow false-positive
decision; it does not authorize one. The original PR129 remains untouched.

## Verified evidence

- Product main: `523b0a35466e799c36e85e3bc5ea046cf3263c31`.
- Reference: `selamy-labs/relentless-ci-python@b8b191341e3061d7600bb1666f39da5374a492d9`.
- Native Gitleaks `8.30.1`, reference configuration, empty ignore file, redacted output,
  inline allow comments ignored. Fresh scans returned exit **1**, with **11 available-history
  findings**, **8 main-ancestry findings**, and **8 current-tree findings**. Counts overlap.
- The available-history scan includes fetched unmerged PR129 commits. Three history findings
  are outside main ancestry; eight are already in main. The main-only scan was an additional
  diagnostic, not a proposal to narrow the required scan.
- Independent source/use-case review identified nine historical synthetic test inputs,
  one address-shaped test identifier and one production runtime expression with no string literal.
  Six of the synthetic redaction inputs, the identifier and the runtime expression remain in
  the current tree. Merely locating a value in a test was not treated as sufficient evidence.
- Source roles are established by construction, consumers and assertions. Issuer provenance,
  external reuse and credential validity were not checked. No inspected finding establishes
  a confirmed credential incident. No rotation or external report was performed.

[Fresh scan receipt](polymarket-secrets-scan-receipt.json) records observed scopes, exits and report
hashes. Full command arrays and working directories were reconstructed afterward from the saved
qualification driver and are explicitly labeled as reconstructed invocation metadata. [Per-finding adjudication](polymarket-secrets-adjudication.json) records all 19 report rows,
source blob hashes, historical identities and behavioral evidence without candidate values.

## Why forward fixes alone are insufficient

The reference [scanner runner](https://github.com/selamy-labs/relentless-ci-python/blob/b8b191341e3061d7600bb1666f39da5374a492d9/quality/security.py#L33)
scans current files and all available Git history. Its
[report validator](https://github.com/selamy-labs/relentless-ci-python/blob/b8b191341e3061d7600bb1666f39da5374a492d9/quality/security_reports.py#L19)
rejects any nonempty report. A source-level false-positive classification does not satisfy this rule.
Changing today's fixture construction leaves historical introductions detectable. Removing the
unmerged donor refs would still leave eight main-ancestry findings and would reduce the inspected
scope. No refs or history have been removed.

Repository [AGENTS.md](https://github.com/pselamy/polymarket-insider-tracker/blob/523b0a35466e799c36e85e3bc5ea046cf3263c31/AGENTS.md)
requires underlying fixes and prohibits baselines, allowlists and weaker checks. The current durable
goal carries that rule forward. The
[constitution](https://github.com/pselamy/polymarket-insider-tracker/blob/523b0a35466e799c36e85e3bc5ea046cf3263c31/.specify/memory/constitution.md)
reserves assumptions affecting security for Patrick's review. A historical exception therefore
requires an explicit policy and goal amendment before implementation.

## Proposed decision

**Recommend permitting individually reviewed historical false positives through the trusted policy
boundary, while retaining the native scanner and preserving history.** This is an explicit exception
to empty-report acceptance and the no-baseline rule. It must not be described as unchanged native
Relentless acceptance. No code implementing this exception has been prepared or enabled.

If approved, prepare a bounded slice with these requirements for review before merging:

1. Keep native Gitleaks at the pinned version and keep complete current-tree and available-history
   scans. Do not use Gitleaks ignore entries, inline allows, altered detector rules, excluded source
   paths or shortened history to hide a finding.
2. Bind each approved historical disposition to its exact commit, path, rule, report fingerprint
   and independently verified source blob. Keep native findings visible in the report and show
   approved dispositions separately. A whole test file or credential-shaped grammar cannot qualify.
3. New, changed, unreviewed or real credential findings fail. Missing, stale, malformed or incomplete
   reports and scanner errors fail. A runner must never treat an arbitrary nonzero exit as success.
   Approval must come through the trusted human policy boundary, not the candidate branch itself.
4. Retain the existing redaction and scanner-negative scenarios. Qualify any forward fixture changes
   with behavioral tests. The final current-tree policy, including the runtime-expression false
   positive, must be explicit; no readability churn solely to evade a regex is pre-approved here.
5. Prove clean-pass and defect-fail behavior with native current-tree and history-only controls,
   adjacent approved/unapproved content, changed source blobs, forged approval data, missing reports,
   swallowed exits, inline allows, ignored paths, and scan-scope omissions.
6. Independently review the exact implementation and trusted approval mechanism, then verify required
   hosting at the exact PR head. Patrick's merge approval remains separate.

The concrete candidate findings are H0–H10 in the adjudication record. Approval of this direction
would authorize drafting a narrow policy amendment, not blanket acceptance of those findings or
approval to merge an implementation.

Keeping the existing policy unchanged is also valid: record this security control as failing and
continue independent work. The overall finish line then remains incomplete. Rewriting history or
starting a replacement repository is a separate consequential decision, not an automatic remedy.
