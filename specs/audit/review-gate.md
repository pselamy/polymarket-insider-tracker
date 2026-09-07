# Initial Scope and Assumption Review Gate

**Status**: Recommended baseline approved by Patrick on 2026-09-06
**Prepared**: 2026-09-06
**Planning authorization**: Granted for slice 002
**Implementation authorization**: Granted for slice 002 by Patrick on 2026-09-07; merge and later slices remain unauthorized

The gap register and four specification-quality checklists are complete. The specs contain no
unresolved placeholder because each uncertain area has a recommended assumption. Those assumptions
were approved as the baseline package. That approval authorizes clarification, planning, checklist and
task generation, and analysis for slice 002, but not application-code changes.

## Decision A1 — Supported Trade Source

**Recommendation**: Conditionally replace the obsolete anonymous trade WebSocket path with near-real-time
acquisition from Polymarket's currently documented public wallet-bearing trades source. Default to all
public participant observations rather than the provider's taker-only default, retain the normalized trade
consumer contract, and give the legacy WebSocket environment setting an explicit deprecation/rejection path.

**Why**: The current Market WebSocket requires asset subscriptions and does not provide the trader wallet.
Current RTDS documentation covers comments and crypto prices, not trade activity. An all-market insider
tracker needs wallet identity, which the public trades query currently supplies.

Approval is conditional on a bounded feasibility record proving sustainable cadence, ordering/replay handling,
publication lag, cache behavior, and visible page-saturation/loss detection against the provider's documented
10,000-row pagination boundary and 200-requests-per-10-seconds rate limit. The 2026-09-06 probe saturated a
10,000-row response and found rows outside requested time bounds, so these are not theoretical concerns.

**Alternative consequence**: Requiring WebSocket-only ingestion means either using an undocumented legacy
interface or narrowing the product to non-wallet market activity. If the feasibility gate fails, work stops
for a new product decision rather than choosing either alternative silently.

## Decision A2 — Supported Runtime Matrix

**Recommendation**: Support Python 3.11, 3.12, and 3.13 inclusive on Linux and Apple Silicon macOS; run the
blocking compatibility matrix on Linux, require the same repository verification as Apple Silicon release
evidence, and use an advisory/scheduled Apple Silicon job when a suitable runner is available. Reject newer
minors until their dependency and full test matrix is verified.

**Why**: Metadata currently promises every version after 3.11 while automation tests only 3.11. A finite
matrix makes dependency, lockfile, and CI claims testable and includes the Apple Silicon failure reproduced
during the audit.

**Alternative consequence**: Supporting only 3.11 reduces automation cost but is a narrower user contract.
Leaving the upper bound open perpetuates an untestable promise.

## Decision A3 — Delivery Deduplication

**Recommendation**: Record successful delivery per notification channel, only after confirmed success.
Use channel + normalized wallet + market as the dedup identity within the configured window. Dry runs and
confirmed failures write no successful-delivery state; partial retries target only failed channels. An
ambiguous timeout enters an explicit unknown state for 60 seconds, then becomes retryable with a visible
possible-duplicate disposition.

**Why**: The current score-time global key suppresses later real alerts after dry runs and after failures.
Per-channel state avoids spamming a successful channel while allowing a failed channel to recover.

**Alternative consequence**: A simpler global key must choose between resending to successful channels or
suppressing failed channels. Either behavior loses an important safety property.

## Decision A4 — Truthful Core Detection Boundary

**Recommendation**:

- retain the existing 0.80 runtime alert threshold without claiming statistical calibration;
- define the default wallet boundary as fewer than 5 prior transactions;
- call a wallet “fresh” only when its known age is at most 48 hours, and label low-nonce/unknown-age evidence
  as “low-activity wallet, age unknown,” capped at confidence 0.50 with no age/new-wallet bonus;
- use available positive daily volume in core size analysis and mark book depth unavailable rather than zero;
- treat funding chains as best-effort stored enrichment only;
- label sniper clustering as experimental/non-operational;
- explicitly defer book-depth scoring, sniper scoring, funding-based scoring, and backtesting to separately
  approved specifications.
- require a fixed 32-or-more-case replay comparison of the old and proposed score distributions and
  threshold-qualification counts before scoring implementation is approved.

**Why**: This is the smallest contract that makes the already wired core evidence truthful. Expanding dormant
modules into scoring now would require new source, retention, calibration, and false-positive decisions that
are not supported by repository evidence.

**Alternative consequence**: Promoting any deferred capability expands this effort materially and needs its
own bounded specification before planning.

## Spec Kit Execution Discipline

- The built-in `checklists/requirements.md` files are author-owned spec-quality checklists required by
  `$speckit-specify` and maintained by `$speckit-clarify`. Agy initially challenged this path, then corrected
  its finding after reviewing the exact v1.0.4 ownership rule.
- Later `$speckit-checklist` runs create reviewer-owned domain files such as `api.md`, `operations.md`, or
  `data-integrity.md`. Agents MUST leave their new items unchecked; only Patrick or his designated human
  reviewer may mark them complete.
- Numeric feature prefixes record creation order. Execution order is `002` → `001` → `003` → `004`, so the
  dependency/test foundation lands before feature behavior.
- Before every per-slice Spec Kit command, set
  `SPECIFY_FEATURE_DIRECTORY=specs/<NNN>-<slug>`. The ignored `.specify/feature.json` pointer is local state
  and MUST NOT determine a slice implicitly in a multi-feature branch or fresh clone.
- Run the individual skills in the required order: specify → clarify → plan → custom checklist → tasks →
  analyze → human validation → implement → converge. Do not invoke the bundled legacy workflow file because
  it omits required intermediate gates.
- Slice 003 owns the assessment schema and reusable end-to-end harness; slices 001 and 004 contribute fixtures
  and assertions. Every slice's tasks must update its gap-register entries before convergence.

## Already Fixed Boundaries

- Research and monitoring only; no trading execution, recommendations, or accusations of illegal conduct.
- No real Discord or Telegram smoke message without explicit authorization.
- Public Python interfaces, CLI flags, environment variables, and storage remain compatible unless an
  approved spec defines a migration.
- Every approved slice proceeds through clarify, plan, reviewer-owned domain requirements-quality checklists,
  tasks, analyze, human validation, implementation, and convergence.
- A pull request may be prepared after all evidence converges, but it will not be merged without Patrick.

## Decision Record

Patrick responded **“approve recommended baseline”** on 2026-09-06 with no A1–A4 overrides. The resulting
slice-002 plan, reviewer-owned domain checklist, tasks, and analysis return to Patrick for the next human
validation gate before implementation.

Patrick then responded **“approve recommended slice 002 plan”** on 2026-09-07. This approves the four
recommended analysis remediations, accepts the reviewer-owned runtime checklist for implementation entry
without changing its markers, and authorizes slice-002 implementation. It does not authorize a merge or
implementation of slices 001, 003, or 004.
