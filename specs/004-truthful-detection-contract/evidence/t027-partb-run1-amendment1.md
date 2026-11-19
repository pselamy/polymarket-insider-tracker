# T027 Part B — run 1, post-run amendment 1: re-walk of the four capped addresses

**This is a post-run amendment, chosen after seeing the run 1 FAIL.** It is not part of the run 1
protocol fixed before the walks, and it does not change the run 1 receipt
([`t027-partb-run1.md`](t027-partb-run1.md), [`receipt.json`](t027-partb-run1/receipt.json)), which
stays the recorded verdict: **FAIL** on Q1. Wallet scoring stays disabled and the default threshold
(0.80) is unchanged. Nothing in this amendment changes configuration.

## Authorisation and scope

- Relay `polymarket-four-wallet-approved-20261101`: Patrick chose option 2 of the run 1 receipt
  ("re-walk of the four capped addresses only") on 2026-11-01.
- Only the four addresses whose read-1 walk ended at the 10-page clamp were re-walked.
- The re-walk used the same `sample.json`, capture, window, ground truth and `end=1793070088`.
- Read-1 caps were `ceil(k/1000) + 1` with the upper clamp of 10 removed, and nothing else changed:

| Address | k (ground truth) | Cap |
|---|---:|---:|
| `0x821dab0565ebf5b327f51db06223fdcfe01acf16` | 31,701 | 33 |
| `0x2a69660046d7acc4ab204d7cc5ba78b0776cd2f7` | 13,976 | 15 |
| `0x21ffd2b7a212a6f277ed3eca1a9f8efcbca90d71` | 10,146 | 12 |
| `0x6d3c5bd13984b2de47c3a88ddc455309aab3d294` | 9,968 | 11 |

- Out of scope: no new capture, no resampling, no scoring or trading, no paid resources, and no
  access changes.

## Run

| Item | Value |
|---|---|
| Harness | Unmodified `t027_cli walk --read 1` at `3815a51`. Its `scripts/spec004_evidence` and `src` match main `9d370e6`. |
| Arguments | `--plan plan4.json` (the four caps above); `--prior walks1.jsonl walks2.jsonl`, giving 528 requests used and a budget of 472 left. |
| Retries | None. The walker never retries. |
| Pacing | At most 2 requests/s; the pacer reports no deviations. |
| Requests | **71 of the 71 allowed**, 2026-11-01 15:48:52Z → 15:49:27Z (35.1 s). All 71 returned HTTP 200 with no errors. The `end` parameter matched on every request. |
| Budget | Run 1 used 528 and this amendment used 71, for 599 of 1,000. 401 remain. |
| Failed launch | One earlier launch failed before any request was sent, because `uv` was not on the tmux `PATH`. Its log is kept as `attempt0-*`; no output file was created. |

## Results (four addresses only)

| Address | Pages | Rows (distinct) | Ground truth returned | Recall, run 1 → amendment | Deepest cursor `d` | Oldest row − `sampled_at` |
|---|---:|---:|---:|---|---:|---:|
| `0x821dab05…` | 33 | 33,000 (32,997) | 31,701 / 31,701 | 0.3154 → **1** | 33,000 | −5,199 s |
| `0x2a696600…` | 15 | 15,000 (15,000) | 13,976 / 13,976 | 0.7154 → **1** | 15,000 | −6,781 s |
| `0x21ffd2b7…` | 12 | 12,000 (11,982) | 10,146 / 10,146 | 0.9838 → **1** | 12,000 | −15,733 s |
| `0x6d3c5bd1…` | 11 | 11,000 (10,841) | 9,968 / 9,968 | 0.9879 → **1** | 11,000 | −12,969 s |

- **Cursor depth.** `/v2/trades` kept serving full pages well past cursor depth 10,000, up to 33,000.
  Every walk reached rows older than `sampled_at`. The run 1 stopping point therefore came from the
  run's own 10-page clamp, not from an API depth limit.
- **Q1 (these four).** Every ground-truth trade was returned.
  - If these four walks replace their run 1 read-1 walks, aggregate recall becomes
    **167,519 / 167,519 = 1.0000** and per-address minimum recall becomes **1.0000**. Both clear the
    thresholds (≥ 0.99 aggregate, ≥ 0.95 per address).
  - This substitution is an amended figure only. It is not a new verdict for run 1.
- **Q2 (ordering).** 0 violations across all 71 pages.
- **Q3 (incompleteness).** Each walk ended at its cap with `has_more = true` and a non-null
  `next_cursor`. The judge's own rule classifies each one as incomplete (`terminal-final-page`), so
  each is detected as incomplete from its own responses.
  - Lifetime history starts long before the window, so `start=1` means these walks are incomplete as
    lifetime histories even though they cover the whole window.
  - As in run 1, Q3 is true by construction.
- **Rows the API returned that the capture lacks.** The walks returned 5 in-window rows whose
  identities are not in the ground truth (1, 1, 3 and 0 per address). Run 1 saw 4 of these; the extra
  one for `0x821dab05…` sits beyond run 1's depth.
  - The run 1 caveat stands: the ground truth was not shown to be complete.

## What this does and does not establish

- It removes the ambiguity recorded in run 1. The four misses were caused by the tooling's page clamp.
  With caps sized from the window count, these four addresses are fully recalled.
- It does **not** turn run 1 into a PASS. The amendment was chosen after the result and covers only
  the four addresses.
- The decision about how to treat it rests with Patrick. Options include accepting it as resolving the
  ambiguity, or running a fresh pre-registered run (option 3) with caps set from the budget alone
  before any data is collected.
- R09a's FAIL-row consequences (O2, O6 and G3 unavailable; D1a stays O1) stay in force until that
  decision is made.

## Files

- In the repo, under [`t027-partb-run1-amendment1/`](t027-partb-run1-amendment1/):
  - `plan4.json`: the four caps.
  - `amendment-result.json`: the scoped judge output.
  - `inputs.sha256`: run 1 inputs, re-hashed before the re-walk. They match the run 1 receipt pins.
  - `raw.sha256`: hashes of the 71 raw response bodies.
- Kept on the jumpbox under `~/t027/run1-rewalk4/`, not committed (size):
  - `walks4.jsonl`, sha256 `308186f5…`.
  - The 71 raw bodies under `raw/read1/`.
  - The scoped judge script `judge4.py`, sha256 `4696b217…`. It imports main's `t027_cli` and
    `t027_partb` and reuses `ground_truth`, `address_result`, `recall_figures`,
    `ordering_violations`, `is_complete_history` and `failed_check` unchanged.
