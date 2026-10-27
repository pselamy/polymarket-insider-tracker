# T027 Part B — run 1 receipt (R09a v2)

**Verdict: FAIL** on Q1 (recall), computed by the merged judge (`scripts/spec004_evidence/t027_cli.py judge`,
main `3815a51`) under `research.md` R09a v2. The wallet group stays non-scoring: this receipt changes no
configuration, and FAIL keeps the gate closed (`unqualified_pre_T027`).

**Read this first: what the FAIL measured.** All missed trades come from **4 addresses** whose read-1 walks
stopped at the run's own pre-registered read-1 page cap, `clamp(ceil(k/1000) + 1, 2, 10)`. With 1,000 rows
per page, 10 pages is at most 10,000 rows, but these addresses had 9,968–31,701 ground-truth trades in the
24 h window. For the other **170** active addresses, recall is exactly **1** (101,728 of 101,728 ground-truth
trades returned; one of them, with 9,880 window trades, also reached the cap but had already returned
all of them). Every other check passed. R09a v2 itself sets no per-walk page cap, only the
1,000-request budget, and this run used 528 of those requests.
The verdict is reported exactly as the frozen rule computes it. Whether this FAIL stands or whether a
re-walk with a cap that covers the window is authorised is a decision for Patrick (see "Open decision").

Machine-readable receipt: [`t027-partb-run1/receipt.json`](t027-partb-run1/receipt.json). The run's
`sample.json`, `plan.json` and `provenance.json` are next to it.

## Run

| Item | Value |
|---|---|
| Authorisation | Patrick, 2026-10-25 22:54 ET ("go on part b"): bounded, anonymous, read-only |
| Ground-truth capture | Real `TradePoller`, official `/v2/trades`, coverage `all`, 5 s cadence, 600 s horizon, isolated empty Redis DB 13; 2026-10-26 03:00:33Z → 2026-10-27 04:30:34Z (clean `stop`) |
| Capture totals | 4,186,741 delivered observations; **0 loss events**, 0 consecutive failures at the end; 18,342 polls; 6 rows rejected as `invalid:conditionId` (logged, not delivered), 97 `deferred:future-cycle`, 2,088 `padding` |
| Window | t0 = 1792983687 (2026-10-26 03:01:27Z), t1 = t0 + 86,400; sample drawn at t0 + 3,600 |
| Proven coverage | 86,400 s (the full 24 h; ≥ 72,000 s required); no loss intervals |
| Frame / sample | 294 wallets with ≥ $1,000 notional in [t0, t0 + 1 h); seed 20261026 → N = 200 |
| Query | `GET /v2/trades?user=…&taker_only=false&start=1&end=1793070088&limit=1000`, later pages `cursor` only; no `offset`; ≤ 2 req/s |
| Read 1 | 2026-10-27 04:38:55Z → 04:42:46Z; 451 requests, all HTTP 200, no errors |
| Read 2 | 2026-10-27 07:21:55Z → 07:22:36Z (≥ 1 h after read 1); 77 requests, all HTTP 200; re-walks the 65 read-1 complete histories with a cap of read-1 pages + 1 |
| Requests | 528 of the 1,000 budget; no pacing, budget, `end` or sample deviations |
| B | 5 (FR-009 protocol default; not captured from a running product) |

## Results against R09a v2

| Check | Result | Rule | Outcome |
|---|---|---|---|
| Coverage | 200 queried; 174 active (k ≥ 1); 160 heavy (k ≥ B) | ≥ 50 active | met |
| Q1 recall, aggregate | 0.8450 (141,556 / 167,519) | ≥ 0.99 | **FAIL** |
| Q1 recall, per-address minimum | 0.3154 | ≥ 0.95 for every active address | **FAIL** |
| Q2 ordering | 0 violations | 0 | pass |
| Q3 incompleteness | 65 complete; 135 incomplete, every one classified incomplete by its own responses (`terminal-final-page`: the cap ended the walk before `next_cursor = null`/`has_more = false`) | every case detected | pass (true by construction, see limits) |
| Q4 stability | earliest trade identical in 65 / 65 complete histories, reads ≥ 1 h apart | 100 % | pass |
| Q5 (a) replay | 7 / 7 cases hold (exactly-B complete → not low activity; dropped / 500 / timeout / contradicting terminal flags / changed filters / invalid row → `unknown`) | all hold | pass |
| Q5 (b) live boundary | 0 violations in 30 heavy addresses with a complete read-1 history | 0 | pass |

### The Q1 misses

| Address | Ground-truth trades in window | Returned (distinct) | Recall | Read-1 pages |
|---|---:|---:|---:|---:|
| `0x821dab0565ebf5b327f51db06223fdcfe01acf16` | 31,701 | 10,000 | 0.3154 | 10 (cap) |
| `0x2a69660046d7acc4ab204d7cc5ba78b0776cd2f7` | 13,976 | 9,999 | 0.7154 | 10 (cap) |
| `0x21ffd2b7a212a6f277ed3eca1a9f8efcbca90d71` | 10,146 | 9,982 | 0.9838 | 10 (cap) |
| `0x6d3c5bd13984b2de47c3a88ddc455309aab3d294` | 9,968 | 9,847 | 0.9879 | 10 (cap) |

Each of these walks returned 10 full pages of rows from inside the window, newest first, and was cut off by
the cap before it got back to the sampling time. The trades it did not return lie in the part of the
window the walk never reached; the run therefore has no observation of them either way. No address
whose walk reached past the sampling time missed any ground-truth trade.

## Limits (also in the receipt)

- Q3 is true by construction: the classifier is the complete-history rule itself.
- Q5 (a) replays the judge's own classifier; no O6 runtime exists yet.
- B = 5 is the FR-009 protocol default, not a value captured from the product.
- Q5 (b) is judged on 30 heavy addresses with a complete read-1 history.
- 0 active addresses were excluded from Q1 for a read-1 transport failure.
- 1,400 duplicate rows (the same observation identity on more than one page of a walk) were counted once per identity.
- 0 frame-hour trades are missing from complete read-1 histories (diagnostic).
- 135 of 200 read-1 walks are incomplete because `start=1` asks for the whole lifetime history, while the
  page cap was sized from the 24 h window count. These addresses are `unknown` by rule, which is the
  designed outcome; it limits the Q4 population to 65.

## Provenance

- Capture worktree: main `7bf8e24` plus the pre-review `t027_capture.py` (sha256 `f75a87b8…`). The merged
  file (#235) differs only in typing and startup (Redis `dbsize` via a Protocol, `from_url` via the typed
  factory, a `DESCRIPTION` constant instead of `__doc__`); capture behaviour is unchanged.
- Sample, plan, walks and judge: main `3815a51` (#235 merged).
- Out-of-run activity before the run: capture smoke runs into a separate directory and Redis DB 14, and
  two exploratory `/v2/trades` requests (response-shape probe). None of them feed this receipt.
- The operator's link to the jumpbox dropped several times. The capture and walks ran in tmux on the
  jumpbox and were not touched; the capture's status log has no gap over 180 s.
- Inputs not committed (size) are kept on the jumpbox under `~/t027/` and pinned by sha256 in the receipt:
  - `run1/trades.jsonl` `a6965ff8…`
  - `run1/status.jsonl` `2b574191…`
  - `walks1.jsonl` `9b5f589e…`
  - `walks2.jsonl` `85c26c6f…`
  - raw response bodies under `raw/`.

## Open decision (Patrick)

Under R09a, FAIL makes O2, O6 and G3 unavailable. In this run the FAIL comes from the run's own read-1
page cap, not from a returned walk that dropped a trade. Options:

1. **Accept this FAIL as the Part B result.** D1a/D1b go back to Patrick per R09a.
2. **Authorise a re-walk of the four capped addresses only.** Same sample, window, ground truth and `end`,
   with a read-1 cap of `ceil(k/1000) + 1` and no upper clamp. That is about 33 + 15 + 12 + 11 = 71 requests,
   within the 472 left of the 1,000-request budget. Re-walking everything would exceed the budget. Q1 is
   judged on the re-walks, and the other checks keep this run's reads. The change would be recorded as a
   post-run amendment chosen after seeing this result, and this receipt stays in the record.
3. **Authorise a fresh run** (new 24 h capture and new seed) with that cap fixed before any data.

Nothing is re-run without Patrick's word.
