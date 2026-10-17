# SEC006 native-worker Phase0 research

Prepared 2026-10-17. Scope is preparation only: no real repository scan/finding approval, protected-origin claim, hosting, access, credential, policy activation or merge. The product checkout was read-only; CodeGraph was used before source exploration. All synthetic mutations, downloaded artifacts/configs and restricted native reports/logs are under `work/security-worker-research/`. Earlier source-binding research is preserved.

## Concrete recommendation

Prepare the smallest worker that (1) verifies declared tool/config/input identities, (2) inventories and materializes owned immutable source inputs, (3) runs both fixed native modes, (4) captures bounded fresh evidence, and (5) returns restricted observations or **incomplete/failure**, never finding approval or a required passing check. Constructors and digests measure bytes; protected origin is a later authenticated execution boundary.

Use the unmodified official Gitleaks8.30.1 release executable, a stronger **protected full configuration retaining all222 upstream detectors with every global/rule allowlist removed**, an explicit protected empty ignore file, and a fail-closed veto for any active target-root `.gitleaksignore`, unsupported content or scan diagnostic. Preserve all candidate bytes and enrolled scope; do not delete ignore files, rename authored paths, use an aggregate fallback, or call an excluded/unreadable case clean. A future scanner correction can support active root ignores without suppression, but it must be a separately identified, reviewed build.

## Pins and artifact qualification

Public tag `v8.30.1` resolved to commit `83d9cd684c87d95d656c1458ef04895a7f1cbd8e`. All24 downloaded source files matched their GitHub Git-tree blob OIDs. The [official release](https://github.com/gitleaks/gitleaks/releases/tag/v8.30.1) was published2026-03-21; API release ID299662760 reported `draft=false`, `prerelease=false`, **`immutable=false`**. A version string or movable tag is not an artifact pin.

Relevant exact archive identities observed in the official release API and checksum list:

| Runtime | Official filename | Archive SHA256 |
| --- | --- | --- |
| AppleSilicon macOS | `gitleaks_8.30.1_darwin_arm64.tar.gz` | `b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5` |
| Ubuntu amd64 | `gitleaks_8.30.1_linux_x64.tar.gz` | `551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb` |
| Ubuntu arm64, if enrolled | `gitleaks_8.30.1_linux_arm64.tar.gz` | `e4a487ee7ccd7d3a7f7ec08657610aa3606637dab924210b3aee62570fb4b080` |

The [pinned GoReleaser config](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/.goreleaser.yml) maps Go `amd64` to archive `x64`, includes darwin/linux and arm64, and builds with `gore2regex`. The downloaded Darwin archive was7,897,593 bytes, matched both release API digest and checksum line, and contained only regular LICENSE, README and gitleaks members. Extracted executable SHA256 was `ba52fb1bfabbcde42f032afad3d6e0b19dff8ed105229a16e7caa338bbc0e84f`; it reported8.30.1. The locally installed Homebrew executable also reported8.30.1 but had different bytes, so it was not substituted for this probe artifact.

The12 release assets contained checksums and platform archives, with no signature/certificate/provenance bundle. Unauthenticated repository-attestation queries for the three archive digests above each returned404; no signed provenance was verified. This is bounded negative evidence, not proof that no attestation exists anywhere. The pinned GoReleaser config has no signing/attestation stanza, and the [release workflow](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/.github/workflows/release.yml) describes Docker publication, not authenticated provenance for these tarballs. GitHub's [attestation API](https://docs.github.com/en/rest/repos/attestations#list-attestations) requires cryptographic signature/timestamp and signer-identity verification for a provenance claim.

Minimal qualification is a **protected, independently reviewed artifact-import manifest** pinning official repository/release/asset IDs, source commit, archive/extracted-binary hashes, platform, observed regex engine and test results. The worker rechecks bytes against that already trusted manifest; it must not enroll a digest supplied by the candidate. API/checksum agreement is byte/publication evidence, not signed build provenance. If policy requires authenticated upstream build provenance, this release has not satisfied it: a verified upstream statement or a separately qualified protected reproducible-build/import boundary is a design/activation prerequisite. Missing protected deployment IDs/keys are later unconfigured values; pretending unsigned checksums establish a signer is an unresolved design error.

## Protected detector configuration

Exact upstream `config/gitleaks.toml` is97,731 bytes, SHA256 `e163e53b9e7e8a8511e77271e2b323ed057759542a6d988258afe3a1fa329caf`. It contains222 rules,25 global path exclusions,13 global regex allow patterns, and13 rule allowlist tables. Paths include authored lock/config/SVG files and generated-looking directories; intact defaults therefore cannot prove all authored paths were scanned. See the [pinned config](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/config/gitleaks.toml#L18-L65).

Prepared **outside the repository**: `security-worker-research/protected-detectors-no-allowlists.toml`,72,607 bytes, SHA256 `4c8c6a90ea46cfbb1732c3883968f389c3857ea569ec230f1adfe94a3c404865`. Every global and per-rule allowlist table was removed; parsed TOML confirmed that every remaining field of all222 upstream rules is identical, in order, with zero allowlists. This only strengthens detection. Remove whole tables rather than clearing one path criterion from an AND allowlist, which could accidentally broaden suppression. Do not use `extend.useDefault` to try to erase defaults: [configuration extension](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/config/config.go#L420-L487) appends allowlists.

Synthetic native proof: a detector match in `poetry.lock` produced0/[] with the exact upstream config and42/1 with this stronger config. This protected derivative needs review/pinning before use; it was not activated. No detector IDs, regexes, entropy thresholds, keywords, required rules or other detection fields were weakened.

## Fixed native commands and history policy

Run with an absolute verified executable, owned credential-free cwd and an immutable source argument `.`. Keep report/config/ignore paths outside the scanned tree, and use separate unique output directories for the two modes. Fixed common argv:

```text
--redact=100 --ignore-gitleaks-allow
--config <protected-full-config> --gitleaks-ignore-path <protected-empty-file>
--exit-code=42 --report-format=json --report-path <fresh-outside-report>
--max-target-megabytes=0 --max-decode-depth=5 --max-archive-depth=0
--no-banner --no-color --log-level=trace --timeout=<protected-seconds>
```

Tree mode is `dir .` in the raw-byte head materialization. History mode is `git .` in the owned bare store with this **one fixed log-opts argument**, never candidate options:

```text
--log-opts=--full-history --all --root --no-renames --no-ext-diff --no-textconv --text --diff-merges=first-parent
```

`--diff-merges=first-parent` controls merge **diff format**, not history traversal: do not use the separate traversal flag `--first-parent`. All retained roots and all parents remain enrolled. Without an explicit merge-diff format, Git log-p emits no merge diff; `separate` emits a separate patch per parent. See [Git log documentation](https://git-scm.com/docs/git-log/2.50.0). In a synthetic merge-only finding, default formatting returned0/[], first-parent diff formatting42/1, and separate formatting42/2 with only one unique safe native identity. Separate formatting would violate the existing duplicate-complete-row rejection; never silently deduplicate its native rows. `--text` prevents Git's binary classification from silently hiding historical raw bytes: a NUL/ELF synthetic history control was0/[] without it and42/1 with it. Archives/unsupported content still require the explicit veto below.

Pinned [Git command construction](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/sources/git.go#L71-L124) splits log-opts on spaces and replaces the tool's default options when supplied. No dates/counts/path ranges, rev exclusions, custom diff filters, baselines, rule selection, staged/pre-commit, verbose findings, templates, diagnostics HTTP server or candidate option concatenation is permitted. Independently verify the exact frozen refs/reachable-commit graph, including the original PR129 donor; `--all` cannot invent an omitted donor ref. Synthetic main-only history was0/[], whereas the disjoint retained donor gave42/1 both in the owned working repo and a fresh bare store with its donor explicitly enrolled.

## Ignore, decode, archive and incomplete-scan dispositions

**Root ignore is a real native suppression mechanism.** [cmd/root.go300–321](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/cmd/root.go#L300-L321) loads the explicit ignore path and then independently loads `join(source,".gitleaksignore")`. An outside cwd and absolute source do not prevent this. Synthetic control42/1 became0/[] with an enrolled target-root ignore file, with all candidate bytes unchanged, despite the explicit protected empty file. Git mode exhibited the same suppression in a checked-out root. A fresh bare history store avoids candidate root files; the plain tree does not.

Smallest fail-closed preparation: preflight the enrolled root ignore and require absence or **exact empty bytes**; otherwise preserve it and return incomplete/failure before any success claim. This is explicit refusal, not file omission or acceptance. A narrow upstream/vendor correction would load automatic target ignores only when the user did not explicitly set `--gitleaks-ignore-path`; that changes the executable and requires a separately qualified source/build/artifact identity. Per-file `dir <file>` avoided root loading in the control but changes whole-tree invocation/aggregation semantics; `stdin` has no native File/Commit identity ([stdin source](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/cmd/stdin.go#L22-L43)). Neither is an accepted replacement/fallback for required `dir .` and `git .`.

**Decode policy:** pin5 explicitly. Actual8.30.1 CLI/source defaults to5, contradicting the README's0; source and release executable agree. A base64 synthetic fixture was0/[] at0 and42/1 at5. Preserve all native tags/positions and bind the entire original blob; do not reinterpret decoded columns as Python character slices. A depth policy is bounded detector semantics, not a promise to find arbitrarily nested encodings.

**Archive policy:** max-depth0 plus explicit incomplete/failure for any enrolled archive, compressed/opaque application binary or unsupported source. Native archive0 silently skipped a ZIP at trace level and returned0/[]; depth1 found an inner match42/1, whose `outer!inner` identity is not enrolled in SEC005's regular-file binding. Enabling archive traversal without container/inner-byte binding and traversal/resource controls would invent authority. Retain full inventory including historical/deleted versions so unsupported bytes cannot disappear from scope. A later qualified archive extension may replace refusal; present preparation may not call those repositories clean.

**Unreadable/failed content:** native statuses alone are unsound. [Directory traversal/open](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/sources/files.go#L60-L82) skips failures; [fragment errors](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/detect/detect.go#L208-L232) are logged then swallowed; [Git child Wait](https://github.com/gitleaks/gitleaks/blob/83d9cd684c87d95d656c1458ef04895a7f1cbd8e/sources/git.go#L299-L304) failures are debug-only. Synthetic unreadable file, missing directory and a silent nonzero Git-log child each returned0/[] with diagnostic evidence. Treat error/fatal/abort/partial/unsupported-skip diagnostics as failure irrespective of native exit/report, with only exact pinned benign events (e.g. no remote links; empty files proven by inventory; expected findings-count warning) allowed. Unknown diagnostics fail. Trace logs are restricted raw evidence and can contain candidate values even with redaction; never render their contents or interpolate native fields into ordinary errors.

`GIT_GRAFT_FILE=/dev/null` is unsuitable as a scan-history shortcut: this local Git emitted legacy-graft advice to stderr, Gitleaks logged errors, then returned0/[] and a clean-looking summary. Use an actually fresh graft-free store, rather than ignoring its diagnostics. Prior SEC005 raw-object behavior remains separately documented.

## Owned input store, process and evidence boundaries

Prepare a fresh owned bare store with an explicit empty template, trusted config, no candidate config/remotes/hooks/alternates/grafts/shallow state/replacements or shared executable caches. Protected acquisition must independently supply the exact native repository/ref/tip/object set; a candidate bundle/manifest is only data until verified. Validate complete objects/parents/refs and materialize head regular files via verified raw blobs, not checkout/smudge/filter/hooks/submodules. Reject unsupported modes, filesystem case/Unicode collisions, missing/extra/unreadable files, changed hashes or before/after manifest drift. All enrolled authored paths remain in the inventory; unsupported content fails the complete run.

Pin native Git through a private trusted PATH and require the fail-visible `--no-lazy-fetch` capability (upstream2.46 baseline) before execution. Use a fresh env allowlist with system/global config disabled, no inherited GIT/GITLEAKS/proxy/loader/askpass/SSH/trace controls, `GIT_NO_REPLACE_OBJECTS=1`, `GIT_NO_LAZY_FETCH=1`, `GIT_ALLOW_PROTOCOL=''`, no terminal prompts, and trusted private temp/report directories. Do not pass issuer credentials into the worker. Default Ubuntu24.04 Git availability is not qualification; protected provisioning remains required for the full approved Linux/Apple scope.

Bound wall time, native child process group, stdout, restricted stderr, report bytes/rows, input/manifest/object counts, path depth, temp disk and total work. Kill/reap incomplete processes; run both scopes even after42, and preserve fatal failure when either fails. A report must start absent in a new owned directory, finish as a non-symlink regular file, be read completely through a bound, and match parsed exit/report relationships and byte digest. Missing/stale/oversized/partial/malformed reports fail. POSIX process wrappers and byte constructors do not prove network denial, filesystem immutability, memory/CPU containment, service identity or protected origin; those require actual qualified deployment controls and authenticated transport/receipt verification later.

Receipt preparation should bind protected worker/code/policy IDs, repository/head/base/tree, frozen refs and reachable-commit/file/blob manifest digests, executable/config/empty-ignore/argv/env identities, mode-specific start/completion/exit/timeout/diagnostic status and exact raw report digest/size/count. Candidate-supplied values cannot authenticate themselves. No partial passing gate is registered.

## Evidence and remaining qualification

`release.json`, `tag-ref.json`, `git-tree.json`, `source-object-checks.json`, official checksum/archive files and the extracted preparation executable preserve bounded acquisition evidence. `probe-worker.py`/`probe-results.json` and `qualify-config-history.py`/`qualification-results.json` reproduce the synthetic outcomes above; each owned run holds restricted reports/logs. The config derivation verifies semantic rule equality and saves its exact bytes outside the repo. All executed controls used AppleSilicon arm64, Python3.11.15, `/usr/bin/git`2.50.1 Apple Git-155 and the downloaded official Darwin Gitleaks8.30.1 bytes. Linux and Python3.12/3.13 were not executed here; the full approved matrix remains required.

No additional human decision is needed to prepare conservative failures or the stronger detector config for review. Actual activation still needs independently approved protected artifact/policy trust, provisioned identities and qualified OS isolation/transport. Requiring signed upstream build provenance while supplying only unsigned checksums, accepting active root ignores with an unmodified scanner, or accepting unsupported/skipped authored content are **design blockers**, not merely missing deployment strings. This research supplies explicit fail-closed behavior and a reviewable stronger config; it grants none of those exceptions.
