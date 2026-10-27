# T027 Part B — run 1 receipt (R09a v2)

**Verdict: FAIL** on Q1 (recall), computed by the merged judge (`scripts/spec004_evidence/t027_cli.py judge`,
main `3815a51`) under `research.md` R09a v2. The wallet group stays non-scoring: this receipt changes no
configuration, and FAIL keeps the gate closed (`unqualified_pre_T027`).

**Read this first: what the FAIL measured.**

- Every missed trade belongs to one of **4 addresses**, and in each case the read-1 walk ended at its page cap.
  - These addresses had 9,968–31,701 ground-truth trades.
  - Each walk returned 10 full pages (10,000 rows), newest first, and stopped before it got back to the
    sampling time.
- For the other **170** active addresses, recall is exactly **1**: 101,728 of 101,728 ground-truth trades
  returned. Every other check passed.
- Two things set that stopping point, and this run cannot tell them apart:
  - **The run's own page cap.** The plan step capped every read-1 walk at `clamp(ceil(k/1000) + 1, 2, 10)`
    pages (`MAX_PAGE_CAP = 10` in `t027_cli.py`). The cap was fixed in the run tooling before any walk.
    It is **not** an R09a v2 rule: R09a v2 bounds only rate and the 1,000-request total, and this run used
    528 requests. The plan, written before the walks, already showed 5 addresses whose `ceil(k/1000) + 1`
    exceeded 10. The tooling did not flag this as a departure from the protocol, and `deviations` is empty.
  - **A possible API depth limit.** The `/v2/trades` cursor carries a depth counter `d`. It rises by 1,000
    per page and reached 10,000 at every capped walk, which is exactly the legacy `offset` ceiling of
    10,000 that R09a v1 cites. No request in this run went deeper. Whether the API serves rows beyond
    depth 10,000 is **unverified**.
    - If it does not, these four addresses could not be fully recalled under any cap, and the FAIL
      reflects an API limit.
    - If it does, the FAIL comes from the run's cap.

The verdict is reported exactly as the frozen rule computes it. What happens next is Patrick's decision
(see "Open decision").

Machine-readable receipt: [`t027-partb-run1/receipt.json`](t027-partb-run1/receipt.json). The run's
`sample.json`, `plan.json` and `provenance.json` are next to it.

## Run

| Item | Value |
|---|---|
| Authorisation | Patrick, 2026-10-25 22:54 ET ("go on part b"): bounded, anonymous, read-only |
| Ground-truth capture | Real `TradePoller`, official `/v2/trades`, coverage `all`, 5 s cadence, 600 s horizon, isolated empty Redis DB 13; 2026-10-26 03:00:33Z → 2026-10-27 04:30:34Z (clean `stop`) |
| Capture totals | 4,186,741 delivered observations; **0 loss events**, 0 consecutive failures at the end; 18,342 polls; 6 rows rejected as `invalid:conditionId` (logged, not delivered), 97 `deferred:future-cycle`, 2,088 `padding` |
| Window | t0 = 1792983687 (2026-10-26 03:01:27Z), t1 = t0 + 86,400; sample drawn at `sampled_at` = t0 + 3,600 |
| Ground truth | per address, trades with `sampled_at < ts ≤ t1` (the 23 h after sampling), by observation identity |
| Proven coverage | 86,400 s (the full 24 h; ≥ 72,000 s required); no loss intervals |
| Frame / sample | 294 wallets with ≥ $1,000 notional in [t0, t0 + 1 h); seed 20261026 → N = 200 |
| Query | `GET /v2/trades?user=…&taker_only=false&start=1&end=1793070088&limit=1000`, later pages `cursor` only; no `offset`; ≤ 2 req/s |
| Read 1 | 2026-10-27 04:38:55Z → 04:42:46Z; 451 requests, all HTTP 200, no errors |
| Read 2 | 2026-10-27 07:21:55Z → 07:22:36Z (minimum per-address gap 9,583 s ≥ 3,600 s); 77 requests, all HTTP 200; re-walks the 65 read-1 complete histories with a cap of read-1 pages + 1 |
| Requests | 528 of the 1,000 budget; no pacing, budget, `end` or sample deviations |
| B | 5 (FR-009 protocol default; not captured from a running product) |

Capture totals and the read wall-clock times come from files kept on the jumpbox. Those files are pinned
by sha256 in the receipt but not committed (see Provenance).

## Results against R09a v2

| Check | Result | Rule | Outcome |
|---|---|---|---|
| Coverage | 200 queried; 174 active (k ≥ 1); 160 heavy (k ≥ B) | ≥ 50 active | met |
| Q1 recall, aggregate | 0.8450 (141,556 / 167,519) | ≥ 0.99 | **FAIL** |
| Q1 recall, per-address minimum | 0.3154 | ≥ 0.95 for every active address | **FAIL** |
| Q2 ordering | 0 violations | 0 | pass |
| Q3 incompleteness | 65 complete; 135 incomplete, every one classified incomplete by its own responses (`terminal-final-page`: each ended at its planned cap before `next_cursor = null`/`has_more = false`) | every case detected | pass (true by construction, see limits) |
| Q4 stability | earliest trade identical in 65 / 65 complete histories, reads ≥ 1 h apart | 100 % | pass |
| Q5 (a) replay | 7 / 7 cases hold (exactly-B complete → not low activity; dropped / 500 / timeout / contradicting terminal flags / changed filters / invalid row → `unknown`) | all hold | pass |
| Q5 (b) live boundary | 0 violations in 30 heavy addresses with a complete read-1 history | 0 | pass |

### The Q1 misses

| Address | Ground-truth trades (`sampled_at`, t1] | Returned (distinct) | Recall | Read-1 pages | Oldest returned row − `sampled_at` |
|---|---:|---:|---:|---:|---:|
| `0x821dab0565ebf5b327f51db06223fdcfe01acf16` | 31,701 | 10,000 | 0.3154 | 10 | +71,792 s |
| `0x2a69660046d7acc4ab204d7cc5ba78b0776cd2f7` | 13,976 | 9,999 | 0.7154 | 10 | +43,818 s |
| `0x21ffd2b7a212a6f277ed3eca1a9f8efcbca90d71` | 10,146 | 9,982 | 0.9838 | 10 | +1,496 s |
| `0x6d3c5bd13984b2de47c3a88ddc455309aab3d294` | 9,968 | 9,847 | 0.9879 | 10 | +1,835 s |

Checked from the uncommitted `walks1.jsonl` (sha256 pinned in the receipt), which stores every returned row
as (identity, timestamp):

- **Walk shape.** Each of the four walks has 10 pages of exactly 1,000 rows. Every returned row is inside
  the window, newest first, and the oldest returned row is still after `sampled_at`.
- **Where the misses fall.** For three addresses, no missed ground-truth trade is at or after the oldest
  returned timestamp; the misses are all older than anything the walk reached. For `0x821dab05`, 5 missed
  trades share the oldest returned second exactly: the ground truth has 9 trades in that second, and the
  10th page ended after 4 of them.
- **Rows the API returned that the capture lacks.** The walks also returned 4 in-window rows (3 for
  `0x21ffd2b7`, 1 for `0x2a696600`) whose identities are not in the ground truth. This is not explained here;
  it could be a capture gap or an identity mismatch. It does not affect Q1 as defined (ground-truth trades
  returned), but it means the ground truth itself was not shown to be complete.

Of the 170 active addresses with recall 1, three also used 10 read-1 pages:
- `0x576b0696…` (9,880 trades);
- `0x52686279…` (8,575);
- `0x9e3ed7b6…` (8,203).

Only for `0x576b0696…` did the upper clamp cut the cap below `ceil(k/1000) + 1`. All three had returned
every ground-truth trade within their 10 pages.

## Limits (also in the receipt unless marked)

- Q3 is true by construction: the classifier is the complete-history rule itself.
- Q5 (a) replays the judge's own classifier; no O6 runtime exists yet.
- B = 5 is the FR-009 protocol default, not a value captured from the product.
- Q5 (b) is judged on 30 heavy addresses with a complete read-1 history.
- 0 active addresses were excluded from Q1 for a read-1 transport failure.
- 1,400 duplicate rows were counted once per identity. A duplicate is a row whose observation identity
  already appeared in the same walk, whether earlier on the same page or on another page. The judge does
  not separate the two. One address had 216 duplicates across two one-page walks, so in-page repeats do
  occur. Q2 found no ordering violation.
- 0 frame-hour trades are missing from complete read-1 histories (diagnostic).
- 135 of 200 read-1 walks are incomplete. `start=1` asks for the whole lifetime history, but the page cap
  was sized from the window count. These addresses are `unknown` by rule, which is the designed outcome,
  and it limits the Q4 population to 65. *(Summary only.)*
- The 10-page clamp is a tooling choice, not an R09a v2 rule. The judge records it as a parameter but not
  as a deviation. *(Summary only.)*

## Provenance

- **Capture worktree:** main `7bf8e24` plus the pre-review `t027_capture.py` (sha256 `f75a87b8…`).
  - The merged file (#235) differs only in typing and startup: Redis `dbsize` via a Protocol, `from_url`
    via the typed factory, and a `DESCRIPTION` constant instead of `__doc__`.
  - Capture behaviour is unchanged.
- **Sample, plan, walks and judge:** main `3815a51` (#235 merged).
- **Activity outside the run, before it started:** capture smoke runs into a separate directory and
  Redis DB 14, and two exploratory `/v2/trades` requests to probe the response shape. None of them feed
  this receipt.
- **Link drops:** the operator's link to the jumpbox dropped several times. The capture and walks ran in
  tmux on the jumpbox and were not touched; the capture's status log has no gap over 180 s.
- **Inputs kept on the jumpbox under `~/t027/`, not committed (size):** each is pinned by sha256 in the
  receipt.
  - `run1/trades.jsonl` `a6965ff8…`
  - `run1/status.jsonl` `2b574191…`
  - `walks1.jsonl` `9b5f589e…`
  - `walks2.jsonl` `85c26c6f…`
  - raw response bodies under `raw/`

## Open decision (Patrick)

R09a's FAIL row says: "On FAIL, O2, O6 and G3 are unavailable and D1a stays O1". Part A has not run.
Options:

1. **Accept this FAIL as the Part B result.** Under the FAIL row, O2, O6 and G3 become unavailable and D1a
   stays O1. tasks.md routes D1a/D1b back to Patrick only if Part A has also failed, and Part A has not run.
2. **Authorise a re-walk of the four capped addresses only.**
   - Keep the same sample, window, ground truth and `end`.
   - Use a read-1 cap of `ceil(k/1000) + 1` with no upper clamp: about 33 + 15 + 12 + 11 = 71 requests,
     within the 472 left of the 1,000-request budget. Re-walking every address without the clamp would
     need about 536 and exceed the budget.
   - The re-walk pages are judged for Q1, and also for Q2 and Q3, because those cover every returned page.
   - **Caveat:** this works only if `/v2/trades` serves rows past cursor depth 10,000, which is
     unverified (see above). If it stops at 10,000, the re-walk will show that directly, and the FAIL then
     reflects an API limit.
   - It would be recorded as a post-run amendment chosen after seeing this result. This receipt stays in
     the record.
3. **Authorise a fresh run** (new 24 h capture and new seed) with the cap set from the budget alone,
   fixed before any data. The same depth question applies.

Nothing is re-run without Patrick's word.
