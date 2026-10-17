# Source-binding and parser correction checkpoint

SEC005 is verified at `96b0faf9d4eb7d4fa21e5b4c2a298c673c0291d4` for the exact declared-source binding layer. The final raw-byte guard rejects unsupported inputs before their methods run. Four regressions failed before correction; a deterministic real-child timeout control checks private error/log handling and cleanup. Three prior REVISE commits/reports remain preserved.

Independent review: **SHIP for SEC005 preparation only**. Root verified25 source/document hashes against the filesystem and immutable Git blobs,19 review evidence hashes, all18 owned source/test files, unchanged governance/lock/gates, and the committed full suite.

- Full isolated locked Python3.11 compatibility: **1883 passed,3 skipped,55 warnings**,272.54seconds pytest /276.39seconds profile, exit0. Skips are two real-service checks and one Windows-only check.
- Independent exact-head focused matrix: **288 passed each** on3.11,3.12,3.13.
- Independent component coverage: **604 statements and190 branches,100%**, zero excluded lines; this is not whole-project coverage.
- Seven configured native static gates, both explicit strict checkers on18files, cognitive5 and full-scope C90110/E902 pass. New files remain at most399 physical lines.
- All1719 previous IDs retained; final collection1886 adds167. Relative to rejectedfb5c792, all1881 IDs retained plus5.

See [verification](polymarket-security-source-verification.json), [independent review](polymarket-security-source-review.md) and [collection retention](polymarket-security-source-retention.json). SEC006-015, all11 human-owned security criteria, individual finding decisions, exact amendment ratification, activation, full matrix/services/hosting and merge remain open. No scanner authenticity is inferred from a declared scope or identity.

Native worker Phase0 research is active. It found pinned scanner ignore/config and swallowed-error/skip behavior requiring concrete fail-closed design before worker implementation. APIv2 implementation validation and PR135 merge approval remain the two existing pending human requests. Continue the full recovered project scope.

External immutable code/report evidence: review SHA25651d135c1539e5775609adc8fa2f028456d5550a48bf5687422aee86392ac3df2; compatibility log SHA256751143215d21618bd7237314d5a6cd50c364b89a83e2159be46616eed5a21614. Root source manifest records all18 exact file hashes.
