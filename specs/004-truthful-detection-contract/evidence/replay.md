# Preliminary fixed scoring replay

**Captured**: 2026-10-16. **Status**: independent preliminary SHIP; no product implementation.

[Corpus](replay/corpus.json),[per-case results](replay/old-proposed.csv) and[summary](replay/summary.json)
contain114 synthetic cases, source hashes, distributions, threshold qualification and named changes.
The source product files match current main523b0a3; the recorded execution checkout752a918 adds only
an unrelated CI control. The fixed corpus SHA256 is
`8dfddeb95e1c36729f287872ca2239fb911499b9b5bef6c47678b9967531a86e`.

Independent read-only review reran the prototype byte-identically, reconstructed all228 scores and
checked hashes,114 unique IDs, all four signal combinations and all63 change attributions. At
default runtime configuration99 cases qualified0(old)->15(proposed); over all case-specific
thresholds, qualification is2->22. This is not calibration or expected live alert volume.

The proposed adapter uses positive fixture-designated-current volume, strict nonce semantics and
unknown-age cap0.50. Known-age formula, weights, two-group bonus and quantization remain. Nine
proposed constructor/runtime override cases are explicitly separated from the default subset.
The initial evaluator's old-side override error was corrected before final review; corpus unchanged.

The prototype is not yet a qualified canonical source module. Its exact hash is recorded in summary;
reproducible checked-in tooling and controls remain preparation work before the complete validation
packet. The numerical comparison does not verify operational freshness, source unit qualification,
persistence, rendered output, actual pipeline correctness or implementation approval.

The canonical CSV uses LF line endings. The independently reviewed original uses CRLF;
normalization preserves all114 parsed rows and values. Both byte hashes are retained in
`summary.json` as `results_sha256`(canonical LF) and `audited_results_sha256`(original CRLF).
The earlier local research commit contained CRLF whitespace errors; this corrective commit
repairs that formatting without changing the reviewed numerical comparison.

**Addendum 2026-10-19**: "current main" above means main `523b0a3` at capture time. Main is now
`1bdccbc`; three of the seven recorded source hashes drifted through refactors that change no
scoring behaviour (`evidence/source-refresh-1bdccbc.md`, `scripts/spec004_evidence/check_source_hashes.py`). The
corpus, CSV and summary are unchanged and remain valid as the old-versus-proposed behavioural
comparison. They are **not** live-realistic: every default-runtime qualification (0→15) is a
known-age case that the live runtime cannot currently produce; see research R10 and
`evidence/replay-live.md` for unknown-age live-realistic cases.
