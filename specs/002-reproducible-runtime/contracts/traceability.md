# T5 correction: execution and source binding

Authorized by the bounded R1/R2/R3 corrective lane on 2026-09-12. This changes only
traceability validation, tests and evidence; slice 003 delivery is not reopened.

The checked-in `TRACEABILITY.json` files are immutable historical snapshots from
commit `62f5b1e13aa88327b569acf99ff644c6c8e76c41`. Their original claims and old
manifest are preserved verbatim, including claims the independent probe disproved.
They are not fresh exact-head evidence and the corrected validator rejects them
as such. Do not change their states or summaries to make a test green.

A current receipt document retains the original rows as an exact ordered prefix
and the original manifest under `historical_manifest`. It appends new rows and
uses a new manifest containing `slice`, `history_revision`, `head`, `tree`, and
`inputs_sha256`. The validator obtains history from the immutable Git object and
also requires the full prior HEAD ledger row prefix. Caller hashes cannot replace
that comparison. History contributes no current execution credit.

The operator supplies the full expected revision through `LedgerContext`; it must
equal the independently observed checkout HEAD. Manifest `head` and `tree` must
equal `git rev-parse HEAD HEAD^{tree}`. `inputs_sha256` additionally binds sorted
path/SHA-256 pairs for tracked and nonignored untracked working files, excluding
only ledger outputs. Thus dirty working input is bound explicitly rather than
mistaken for committed content. For a final exact-head claim, execute after the
candidate commit in a clean checkout and retain the resulting receipt outside
Git. There is no manifest claiming an invented future commit or hashing itself.

New rows require exact test node IDs; file and gate references never receive
execution credit. `run` is the canonical child harness command, `source` repeats
the independently checked source identity, `config` hashes the actual pyproject
and lock bytes, and `result` is `passed`. These fields are compared with observed
execution inputs; they are not proof by themselves. Requirement/scenario prose
remains a reviewer's semantic assessment, not a coverage claim inferred by code.

`validate_ledger_document(..., execution_output=<new directory>)` launches the
real pytest harness, records actual collected IDs and every setup/call/teardown
outcome, and requires all three phases to pass for every claimed node. Collection
errors, skips, xfails, failures, missing outcomes, duplicate outcomes, nonzero exit,
and input changes during execution fail closed. Standard output and standard
error are separate; command, exit and raw receipt are retained. An existing output
directory is rejected. Without this execution option validation is read-only and
grants no credit. Legacy caller sets are accepted only for call compatibility.
No supplied receipt file, digest, producer string, or executed-node set is trusted.

The trust boundary is an intact reviewed validator, Git object database, pytest
and repository tests on the local host. This is not an attestation against an
attacker executing arbitrary Python or replacing the trusted runtime. The child
runs with only PATH/HOME/TMPDIR, in a fresh directory; no application credentials,
service enablement or inherited pytest options are forwarded. Real service/gate
receipts remain separate native verification evidence and are not converted into
pytest row credit. Final candidate acceptance still requires the full native
static, Python 3.11–3.13 compatibility and loopback services commands, cognitive
complexity <=5 and independently measured cyclomatic complexity <=10.

### Final R2 tightening — committed inputs only

The earlier working-byte description is an input fingerprint, not permission to
credit dirty code as exact-head-passed. Admission now also requires clean committed
inputs before and after the child execution (only the two ledger output paths are
excluded). A self-consistent caller digest for a modified harness/source is rejected.
Tests that deliberately mutate source execute in their own committed local clones;
they do not contaminate the shared checkout. This correction preserves the earlier
candidate commit and adds a regression for dirty source with a recomputed manifest.
