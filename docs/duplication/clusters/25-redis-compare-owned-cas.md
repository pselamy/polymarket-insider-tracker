---
cluster: 25
status: resolved
resolved-by: PR #157
---

# Cluster 25

- **Files:** compare_owned_then_execute (new)
- **Knowledge:** **Resolved**: the watch/compare-owner/MULTI/EXEC CAS skeleton now has one home (`_channel_delivery.compare_owned_then_execute`, terminal operation supplied as an explicit callable); `compare_and_delete` and `compare_and_expire` delegate to it
- **Verdict:** resolved (PR #157)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

### 2026-10-19 resolution update: cluster 25 (Redis watch/compare-then-execute CAS logic)

Cluster 25 is resolved by the dedup card for cluster 25: draft PR
[#157](https://github.com/pselamy/polymarket-insider-tracker/pull/157) from
branch `ralph/dedup-cas`. The Redis delivery-claim safety rule — open a
transactional pipeline, `WATCH` the claim key, `GET` and decode the stored
ownership token, `UNWATCH` and return `False` on a foreign or missing token
without ever issuing a transaction, otherwise `MULTI`, queue the terminal
operation once, `EXEC`, return `True` — moved to exactly one home,
`src/polymarket_insider_tracker/alerter/_channel_delivery.py`
`compare_owned_then_execute(self, key, token, queue_terminal)`. The terminal
operation is an explicit callable against the tiny `_QueuedPipeline` Protocol
(the `delete`/`expire` surface) — not a boolean/mode flag, per the card. The two
former copies remain the named entry points pinned by
`tests/alerter/test_history_extraction.py` and delegate to the home:
`compare_and_delete` queues `pipe.delete(key)`, `compare_and_expire` queues
`pipe.expire(key, ttl)`. The `release_channel_claim`/`extend_channel_claim`
wrappers and their `WatchError` translation to `False` are untouched, and all
tests pass unchanged (only the US-001 characterization tests were added, before
the refactor).

Re-measurement with the pinned scanner (jscpd 5.3.3 receipt verified via
`mise --yes --locked exec -- jscpd --version`, same SCAN_POLICY at the 50-token
tier, no ignore lists or exclusions), enumerating the authored scope exactly
as `verify_sources` does — `quality.source_scope.discover` + `validate_path`,
skipping the 5 rejected `.pyi` stubs, with the 399-line size hook deliberately
not applied — then staging with `quality.duplication_inputs.stage_inputs` and
calling `quality.duplication.scan(root, config, staged, output, minimum=50,
deadline)`:

- Before (baseline commit `2bdf748`, the pre-refactor head of this branch,
  same enumeration): **90 clone groups, 1138 duplicated lines, 1.89%** over
  281 authored files (248 sources tokenized). The cluster-25 pair
  `src/.../alerter/_channel_delivery.py:100-109 <-> 132-141` (73 tokens,
  10 lines) reproduces there.
- After (this change, commit `7053893`): **89 clone groups, 1128 duplicated
  lines, 1.87%** over 281 authored files (248 sources tokenized).

Set-difference of the raw pair sets between the two scans: exactly the one
cluster-25 pair disappeared and **no new pair appeared anywhere**. Compared
with the round-1 scan above (86 groups), the four added pairs are self-pairs
inside `tests/alerter/test_cas_characterization.py` (53, 57, 61, and 113
tokens), the US-001 characterization module added by this card and present in
both measured trees; they are not attributable to the dedup change. Cluster 24
(the competing-owner scenario clone inside
`tests/integration/test_redis_contract.py`) is a different card's scope and is
untouched. `src/...` abbreviates `src/polymarket_insider_tracker`.
