# SEC005 initial source-binding preparation

Status: implementation prepared for independent review; SEC005 is still unchecked. The approved
policy direction authorizes preparation only. No actual finding, ratified amendment, scan execution,
human disposition, trusted worker, required hosting or merge is accepted by this component.

The source reader verifies direct native types and the complete commit/tree/blob OID chain using
native hash-object without writes or filters. It parses verified raw trees, resolves literal UTF-8
paths, rejects unsupported selected modes and requires exact full tree-export bytes. Every identity
retains all native fields and full blob SHA256, with separate tree/history semantics. Declared scope
and constructors are not authenticated provenance or completeness evidence.

The process boundary fixes its environment and no-transport/no-replacement/no-lazy-fetch controls,
streams bounded stdout, discards stderr, enforces command and aggregate resolution limits, and
kills/reaps incomplete children. These controls do not replace the later protected worker sandbox.
Git capability/provenance on Ubuntu remains an explicit SEC006 dependency; Python/Linux/Apple
support is preserved. The observed local native Git is preparation evidence only.

New files are the five planned source modules and six real-Git/tree/identity/process fixture/test
files. The existing `_secret_schema.py` byte prefix is unchanged; public direct-metadata validators
are appended so both boundaries share native field/path/position rules without private imports or
copied validation logic. No CLI, required gate, approval flag or candidate exception is registered.

Failing evidence preceded implementation: the first source/tree collection failed because their
new modules did not exist. An additional negative proved that an equal mutable bytearray was wrongly
accepted as a tree measurement; requiring exact immutable bytes corrected it. Initial fixture faults
(read-only native object files and coinciding environment values) were fixed in the owned tests.
All native objects and intentionally unguarded fetch controls use disposable local bare repositories;
no network, credentials, user repository or external delivery is involved.

The first static runs correctly blocked on formatting, explicit types/private exports and cognitive
complexity6 in command orchestration. Public validators, descriptor-based streaming types and a
separate command-deadline calculation address those causes. No threshold, native scope, suppression,
ignore, confidence, test ID or historical result was weakened or removed.

At the author checkpoint,224 focused parser/source controls pass; this includes103 new SEC005 cases
and all121 prior parser cases. The nine parser/source modules measured510/510 statements and160/160
branches on Python3.11. This is component coverage, not whole-repository coverage or complete security
acceptance. Final committed checks, supported minor reruns, complete existing ID retention,
compatibility and independent exact-artifact review must be recorded before SEC005 can close.

The independent design report is preserved as `source-plan-analysis.md`; its one LOW naming mismatch
was corrected by distinguishing GitLocation (paths) from GitObjects (verified reader). The raw research
and original differing line-count suggestion remain historical evidence. The selected LF-count-plus-one
bound and opaque columns preserve the prior native contract. Full scanner qualification remains SEC008.

SEC006–015, native finding decisions, human-owned checklist markers, exact amendment ratification,
actual protected hosting and all whole-project completion criteria remain open.

## Preserved initial rejection and correction

Initial commit14902995b7656e31f788ba821218f56f5fabeb0e passed1819 tests with3 skips and55 warnings
(pytest295.44s; compatibility299.39s). The3 skips were two real-service tests and one Windows-only
test, not service acceptance. All1719 prior checkpoint IDs were retained with103 additions.
Independent review nevertheless REVISE for private-context leakage from missing nominal slots and
primitive subclass path/membership spoofing. The original review is `source-initial-review.md`.

The corrective implementation validates exact primitive semantics and initialized nominal fields,
revalidates schema/fields before canonicalizing, and makes raw finding repr constant even for malformed
manually created values. It also validates the fixed native location/argv/input/capacity/deadline
boundary.36 regression cases were all observed failing on the unmodified initial code, then passing
after correction; six adjacent controls extend malformed kind/version/native-path coverage. Final
verification/re-review remain required before a completion marker. No original result is relabeled
accepted, and no full security or native-finding acceptance is implied.
