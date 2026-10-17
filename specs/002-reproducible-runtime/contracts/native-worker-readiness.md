# Native worker preparation and readiness

Status: Phase0 research and measurements, not an implementation-ready complete worker contract.
SEC006 remains unchecked. Existing preparation direction permits reviewable work; findings,
governance activation, protected deployment, required hosting and merge retain their separate gates.

## Established source and byte measurements

Pinned upstream source is Gitleaks8.30.1 commit83d9cd684c87d95d656c1458ef04895a7f1cbd8e.
Release299662760 assets378332059 (Darwin arm64) and378332058 (Linux x64) were independently
downloaded and measured. Both archives match official API/checksum identities and contain three
regular members only: LICENSE, README.md and gitleaks. Measured executable hashes are
ba52fb1bfabbcde42f032afad3d6e0b19dff8ed105229a16e7caa338bbc0e84f (Darwin) and
88f91962aa2f93ac6ab281d553b9e125f5197bbbce38f9f2437f7299c32e5509 (Linux).
Linux bytes were not executed. API/checksum agreement establishes bytes/publication evidence,
not verified signer or build provenance. Protected independently reviewed artifact import must
remain explicit; neither a candidate digest nor a version string authenticates a tool.

The proposed full detector configuration removes every global/per-rule allowlist while preserving
all other fields of all222 upstream rules. Root independently compared parsed TOML structures.
Prepared bytes SHA2564c8c6a90ea46cfbb1732c3883968f389c3857ea569ec230f1adfe94a3c404865,
72607bytes, remain research input rather than an active gate policy. No native detector is replaced
with an aggregate/custom fallback. The original configuration and all historical evidence remain.

See `../evidence/security-policy/research/worker/research.md` for primary links, bounded synthetic
controls, exact archive/config identities, rejected alternatives and qualification limits.

## Required worker behavior

1. Acquire independently authenticated repository/head/base/ref inputs outside candidate execution.
   Freeze every retained ref/tip and reachable commit, including the original PR129 donor. A bundle
   or manifest by itself is data, not proof of complete native enrollment.
2. Use a fresh owned bare store, empty template and trusted Git/config with no candidate hooks,
   remotes, alternates, grafts, replacements, shallow state or shared executable caches. Verify the
   full raw object graph. Materialize immutable head regular files from verified raw blob bytes.
3. Retain all path/mode/blob records and full historical versions. Case/Unicode filesystem collisions,
   unsupported modes or content, missing/extra/unreadable/changed files and manifest drift fail the
   whole run. Preserve every enrolled byte; unsupported never means omitted or clean.
4. Run fixed native `dir .` and `git .`, collecting both even after findings exit42. Use protected
   configuration and empty ignore outside source; redact100, ignore inline allows, JSON reports,
   max-target-megabytes0, explicit decode5/archive0, trace diagnostics, fixed native timeout.
5. History options are fixed: `--full-history --all --root --no-renames --no-ext-diff --no-textconv
   --text --diff-merges=first-parent`. The last flag changes merge diff format only; traversal
   `--first-parent` is forbidden. No date/count/path/ref narrowing, deduplication or fallback.
6. Capture bounded stdout and restricted stderr/report bytes in fresh owned output directories.
   Verify regular non-symlink report identity, freshness, complete reads, exact exit/report
   relationships, immutable before/after inputs and source-bound findings. Missing/stale/partial/
   malformed/oversized reports, native errors, aborts, unsupported skips and unknown diagnostics fail.
7. The later protected execution boundary must establish OS process/network/filesystem/resource
   isolation and authentic worker/run/attempt identity. Python values, subprocess wrappers, frozen
   dataclasses and byte hashes cannot establish those properties or human decision authority.

Trace output is restricted raw evidence and can carry arbitrary candidate text despite redaction.
Ordinary errors/output use constant categories, counts and digests; never render native diagnostics.
Only qualified pinned benign events may pass, with any empty-file claim bound to the actual inventory.
Native exit0 plus an empty array is necessary for a clean native result, never sufficient by itself.

## Native behavior requiring a complete design resolution

- The pinned scanner independently loads source-root `.gitleaksignore` even when an explicit empty
  protected ignore is supplied. A conservative run can refuse a nonempty root ignore, preserving it;
  supporting that input requires a separately reviewed scanner correction, not deletion/renaming.
- Exact upstream defaults exclude authored paths. The stronger complete detector configuration is a
  reviewable removal-only correction; it still needs protected policy pinning before real use.
- Default Git merge formatting misses merge-only content; separate-parent formatting produces
  duplicate complete rows. The fixed first-parent diff format preserves all-parent traversal.
- File/fragment/Git-process failures can be logged and swallowed into exit0. Trace-level error veto
  catches observed counterexamples, but does not itself prove all source reads completed.
- In `sources/files.go`, a non-permission `os.Open` error returns nil without a diagnostic. Successful
  preflight/after-read checks cannot logically prove this intermediate read succeeded. `Sema.Go`
  and deferred Git Wait error handling also require source-level completeness analysis.

The final point is a design readiness blocker for a complete unmodified-release worker. Independent
analysis must either establish a conclusive compatible native completion mechanism or identify the
smallest separately qualified upstream-source correction. Do not hide it behind a diagnostic matcher,
unconfigured deployment string, ordinary successful exit or additional byte-only helper. No full
worker implementation readiness, acceptance or SEC006 completion is claimed by this document.

Any proposed scanner correction must keep every native detector/field/schema, use a pinned source
and exact reviewable patch/build identity, propagate incomplete native work as failure, and preserve
all source/history/report obligations. Its revised qualification/contract and implementation-validation
boundary must be concrete and independently reviewed before invoking a new human decision.

## Verification order and retained work

First independently review this readiness boundary and resolve actual native completeness. Then
refine the owned plan/model/quickstart/tasks and run managed analyze before implementation. Meaningful
failing native controls precede fixes, followed by strict types, cognitive5/C90110/399-line Python
limits, exact supported-minor and Linux/Apple qualification, retained IDs, committed compatibility
and independent artifact review. SEC007 donor scenarios can proceed independently in distinct files.
SEC009/010 protected authentication/transport/hosting and SEC011/012 exact human decisions remain
separate; no partial passing gate is registered. Full original product and Relentless scope remains.
