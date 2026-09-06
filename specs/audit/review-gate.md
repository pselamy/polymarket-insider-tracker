# Initial Scope and Assumption Review Gate

**Status**: Awaiting Patrick's decision
**Prepared**: 2026-09-06
**Implementation authorization**: Not granted

The gap register and four specification-quality checklists are complete. The specs contain no
unresolved placeholder because each uncertain area has a recommended assumption. Those assumptions
still require Patrick's explicit review before clarification, planning, task generation, or code changes.

## Decision A1 — Supported Trade Source

**Recommendation**: Replace the obsolete anonymous trade WebSocket path with near-real-time acquisition
from Polymarket's currently documented public wallet-bearing trades source. Describe the product as
near-real-time, retain the normalized trade consumer contract, and give the legacy WebSocket environment
setting an explicit deprecation/rejection path.

**Why**: The current Market WebSocket requires asset subscriptions and does not provide the trader wallet.
Current RTDS documentation covers comments and crypto prices, not trade activity. An all-market insider
tracker needs wallet identity, which the public trades query currently supplies.

**Alternative consequence**: Requiring WebSocket-only ingestion means either using an undocumented legacy
interface or narrowing the product to non-wallet market activity. Neither preserves the current product promise.

## Decision A2 — Supported Runtime Matrix

**Recommendation**: Support Python 3.11, 3.12, and 3.13 inclusive on Linux and Apple Silicon macOS; reject
newer minors until their dependency and full test matrix is verified.

**Why**: Metadata currently promises every version after 3.11 while automation tests only 3.11. A finite
matrix makes dependency, lockfile, and CI claims testable and includes the Apple Silicon failure reproduced
during the audit.

**Alternative consequence**: Supporting only 3.11 reduces automation cost but is a narrower user contract.
Leaving the upper bound open perpetuates an untestable promise.

## Decision A3 — Delivery Deduplication

**Recommendation**: Record successful delivery per notification channel, only after confirmed success.
Dry runs and failed channels write no successful-delivery dedup state; partial retries target only failed channels.

**Why**: The current score-time global key suppresses later real alerts after dry runs and after failures.
Per-channel state avoids spamming a successful channel while allowing a failed channel to recover.

**Alternative consequence**: A simpler global key must choose between resending to successful channels or
suppressing failed channels. Either behavior loses an important safety property.

## Decision A4 — Truthful Core Detection Boundary

**Recommendation**:

- retain the existing 0.80 runtime alert threshold without claiming statistical calibration;
- define the default wallet boundary as fewer than 5 prior transactions;
- call a wallet “fresh” only when its known age is at most 48 hours, and label low-nonce/unknown-age evidence
  as “low-activity wallet, age unknown”;
- use available positive daily volume in core size analysis and mark book depth unavailable rather than zero;
- treat funding chains as best-effort stored enrichment only;
- label sniper clustering as experimental/non-operational;
- explicitly defer book-depth scoring, sniper scoring, funding-based scoring, and backtesting to separately
  approved specifications.

**Why**: This is the smallest contract that makes the already wired core evidence truthful. Expanding dormant
modules into scoring now would require new source, retention, calibration, and false-positive decisions that
are not supported by repository evidence.

**Alternative consequence**: Promoting any deferred capability expands this effort materially and needs its
own bounded specification before planning.

## Already Fixed Boundaries

- Research and monitoring only; no trading execution, recommendations, or accusations of illegal conduct.
- No real Discord or Telegram smoke message without explicit authorization.
- Public Python interfaces, CLI flags, environment variables, and storage remain compatible unless an
  approved spec defines a migration.
- Every approved slice proceeds through clarify, plan, reviewer-owned requirements checklist, tasks, analyze,
  human validation, implementation, and convergence.
- A pull request may be prepared after all evidence converges, but it will not be merged without Patrick.

## Requested Response

Approve the recommended baseline as a package, or list overrides as `A1`, `A2`, `A3`, and/or `A4` with the
desired behavior. Any override will be incorporated into the affected spec before the next Spec Kit phase.
