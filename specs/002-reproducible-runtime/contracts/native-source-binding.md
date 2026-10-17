# Exact committed-source binding (SEC005 preparation)

This refines FR-023/024/026/027 and SC-012/013 within the approved preparation direction. No finding,
amendment activation, native scan, worker authenticity or hosting acceptance is granted. Source objects,
scope declarations and identity constructors are measurements/data, never approval authorities.

## Inputs and result

`SourceScope` is frozen metadata: positive immutable native repository ID (not bool), lowercase64-hex
scanner-policy SHA256, full lowercase40-hex scanned head and a nonempty frozenset of full40-hex retained
commit IDs containing that head. Format/membership checks do not prove origin or complete enrollment.
The later protected worker must derive/authenticate these values and freeze the complete ref/history set.

`GitLocation` names an explicit absolute native Git executable and absolute object-store location.
`GitObjects` reads and verifies raw objects at that location. The
worker must supply a fresh owned bare store/config with no candidate remotes, hooks, grafts, alternates,
replacement refs or executable caches as authority. Preparation fixtures may use owned local repositories.
The reader does not claim arbitrary candidate-owned Git configuration or executable paths are trusted.

`bind_finding(finding, scope, objects, *, tree_bytes=None)` returns a frozen `FindingIdentityV1`:
schema1, repository ID, scanner-policy SHA256, all restricted native finding fields including ordered
tags/empty symlink path, exact source commit, Git blob OID and full-byte blob SHA256. Native path/rule/
fingerprint/tag data and source bytes are omitted from repr/ordinary output. There is no accepted flag.

History uses exactly the native commit and requires membership in the declared retained set; supplied
tree bytes are an error. No fallback to HEAD, parent, similar text or a different path is permitted.
Tree uses exactly the scanned head and requires empty native Commit plus non-null `tree_bytes` exactly
equal to that head's full regular-file blob. These bytes must later be measured from the actual immutable
scanner export by the protected worker. A caller-supplied byte string alone proves no scan execution.
Tree identities bind the head, even when an unchanged blob recurs later. History never matches tree.

## Verified object chain

Require storage object format SHA1. For every object, require the direct native type, read the entire
bounded raw body and independently recompute its native OID using
`git hash-object --no-filters -t <commit|tree|blob> --stdin` without `-w`, `--path` or `--literally`.
Compare the exact full result to the requested OID. Native hashing preserves Git's object-format and
collision-detection implementation; the independently retained full-blob digest uses stdlib SHA256.

1. The starting object must be a commit, including for a historical row. Tags cannot peel to commits.
   Parse exactly the first `tree <40-hex>\n` header from the verified raw commit bytes.
2. Read/hash every tree traversed. Parse the already verified raw bytes directly, avoiding an unverified
   recursive ls-tree mapping and a verify-then-reread race. Compare each requested UTF-8 path component
   with the raw name bytes; do not decode or normalize unrelated sibling names.
3. A raw entry is canonical octal mode, space, nonempty slash-free name, NUL,20-byte SHA1 OID. Require
   full consumption, canonical modes100644/100755/120000/40000/160000, no dot/dotdot/duplicate raw names,
   and strict Git order: raw name plus slash for directories or NUL for other entries. Zero-padded,
   unsupported, ambiguous, malformed or truncated entries fail rather than normalize.
4. Intermediate components must be canonical directories40000. The final entry must be100644/100755
   and directly reference a blob. Symlink, submodule, directory, missing path and type mismatch fail.
5. Verify the complete blob OID and SHA256. Enforce the existing qualified LF-position upper bound
   `blob.count(b"\n") + 1`, including a possible terminal empty position; do not reinterpret native
   columns as character slices or impose an invented column-length bound. This preserves the prior
   native contract, rather than adopting the Git-only note's alternative physical-line count. It does
   not assert that an empty file generates findings. Full scanner-coordinate qualification remains
   in SEC008; the pinned location implementation and existing19-row bounds support this boundary.

Object-chain checks are necessary: research reproduced recursive ls-tree returning a different valid
blob for a corrupted nested-tree object stored under its old OID. Checking only the final blob hash
would miss that mapping change. Every commit/tree/blob body in the selected chain is verified here.

## Native command boundary and budgets

Use argv only, absolute Git, explicit repository, no replacement objects and fail-visible global
`--no-lazy-fetch`. Upstream2.46 is the demonstrated capability baseline; older executables must fail
instead of silently ignoring an environment variable. Set `GIT_ALLOW_PROTOCOL` empty as a second
no-transport barrier. Build a fresh fixed environment; inherit no credentials, dynamic-loader, trace,
Git directory/config/alternate/helper/pager/askpass variables. Disable system/global config and prompts.
No filters, textconv, checkout, diff, hooks, fetch or candidate executable code is requested.

Read stdout while running with POSIX select/read, a monotonic deadline and byte cap. Do not buffer
unbounded output then truncate. Discard stderr; require zero process exit and complete output. Feed
bounded native-hash input via a private automatically closed temporary file to avoid pipe deadlock.
Kill/reap incomplete children on overflow, timeout or failure. Errors use constant categories with
suppressed context, including when called inside an unrelated private exception handler.

Initial qualified capacities:16MiB per object/body/hash input,64 bytes for simple native metadata,
100,000 entries per tree,256 path components,10 seconds per command. A resolution budget additionally
caps total raw object bytes at64MiB, native commands at1024 and total elapsed time at30 seconds.
Exhaustion fails the whole resolution without a partial identity. There is no CLI override or prefix
hashing. These bound source processing; later worker OS memory/CPU/disk/network/process isolation
and full-run limits remain required. They are not claims of complete native process sandboxing.

## Canonical identity

Canonical bytes are compact sorted-key UTF-8 JSON (`ensure_ascii=False`, no nonfinite numbers),
covering every identity field, with schema_version1 and mode `dir`/`git`. Preserve all native values
without normalization; tags remain ordered. Identity SHA256 is over
`b"polymarket-native-finding-v1\0" + canonical_bytes`. This serialization is restricted identity
evidence, not an ordinary-output renderer or authenticated decision. Set canonicalization and
trusted disposition/report provenance are later protected-policy work.

## Verification and retained obligations

Use real owned repositories for history/tree separation, exact changed tree bytes, full blob/commit/
tree corruption, replacements, literal/Unicode paths, direct types, symlink/submodule/directory,
missing objects/paths and no-network behavior. Unit controls cover raw tree framing/order/duplicates,
scope formats/membership, identity deltas, LF bounds and constant repr/log/traceback failures.
Process controls cover exact cap/overflow, endless output, timeout, EOF-then-hang, stderr/nonzero
exit, incomplete process and hostile inherited environment. No generic mock framework is introduced.

Full existing tests/IDs, strict independent types, cognitive5/cyclomatic10/399 lines and supported
Python3.11–3.13/Linux/Apple scope remain. The observed Apple Git2.50.1 is preparation evidence, not
qualified release provenance. Provisioning/qualifying a protected capable Git on Ubuntu remains a
visible SEC006 dependency; Ubuntu support is not removed or claimed verified by this component.
SEC005 can close only this source-binding layer. Full source enrollment/export equality, retained
history completeness, immutable worker evidence, scanner provenance, human decisions and actual
required hosting remain separate blocking completion requirements in SEC006–015.
