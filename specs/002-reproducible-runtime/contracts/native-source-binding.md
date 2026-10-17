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

## Direct nominal-value hardening after independent refutation

The initial implementation1490299 was independently REVISE despite passing local gates and the
full compatibility suite. Uninitialized slotted models leaked active caller exception context;
primitive subclasses redirected a literal path or expanded declared retained membership.
`source-initial-review.md` preserves that first-pass result unchanged.

The correction requires exact built-in strings, tuple tags and frozenset commit sets at the direct
metadata boundary. Scope, finding, location, object-reader and canonical-identity entrypoints validate
initialized fields and suppress malformed-state exception context with constant schema categories.
Canonical serialization revalidates schema1 and every identity field; it does not trust a frozen
constructor having run. A fixed raw-finding repr also hides deliberately invalid mode/coordinate text.
Git paths require native Path values; process argv/input/capacity/deadline primitives are validated.
The original byte-report parser behavior remains; its existing schema byte prefix is preserved.

These checks reject unsupported Python values; they do not authenticate a caller, scope declaration,
source store, scanner execution or approval. Arbitrary Python with memory mutation privileges is not
made a protected authority. Original native source, complete scan/worker and human/hosting obligations
remain unchanged.36 new adversarial controls were observed RED before correction; six further controls
cover closely related malformed kinds, versions and native paths. Logging and traceback checks both
run inside an unrelated active private exception handler.

## Exact base entrypoints and mode/location correction

Independent re-review of351405c remained REVISE: virtual canonical validation/hash dispatch,
uninitialized enum instances, partially initialized native paths and a direct reader kind misuse
violated the safe boundary. The preserved report is `source-first-correction-review.md`.

Base identity construction, canonical serialization and hashing now use an exact-class non-virtual
validator; base hashing anchors the base canonical implementation. An entirely replaced arbitrary
Python method is not a protected authority. Both report and direct metadata validators accept only
the actual two ScanMode singletons. Location validation checks renderable native paths before use;
the transport also translates malformed-state failures safely. The public reader validates its
supported literal native object kind before native operations. Its resolution accountant remains
internally constructed by resolve, not authenticated candidate metadata.

17 additional controls cover the family;16 were observed RED on the unchanged first correction,
while the existing argument guard already safely rejected one text-subclass kind. All59 boundary
controls now pass in the initial corrective run. Full gates, matrix, committed compatibility and
independent exact-artifact re-review remain required; no completion or acceptance is inferred here.

## Exact immutable report bytes correction

The second correction fb5c792 remained independently REVISE: a bytes subclass could replace the
decoded JSON while the report digest still described its invalid underlying bytes, or raise an
exception exposing active private context. The preserved result is `source-second-correction-review.md`.

The JSON boundary now requires exact immutable bytes before invoking length or decoding. Other input
kinds fail with a constant, context-suppressed domain error. Four regressions were observed RED before
the guard: invalid JSON and invalid UTF-8 subclasses, mutable bytearray and a raising decoder tripwire.
The tests check formatted traceback and standard exception logging inside an unrelated private handler.
This changes no native byte-report schema, source authenticity or approval authority. Exact corrected
source verification and independent review remain necessary before SEC005 can close.

The guard is factored into one byte-input validator to preserve the existing cognitive5 budget.
An intermediate287-case run passed all assertions but missed the wait-timeout handler under
concurrent startup load (99.87% coverage). A narrow deterministic wait failure now additionally
checks constant error/log output and actual child reap; the original real EOF/silent hang tests remain.
Historical failed gate and coverage evidence are retained, not relabeled as success.
