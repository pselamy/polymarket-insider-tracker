---
cluster: 16
status: resolved
---

# Cluster 16

- **Files:** tests/tooling/module_loading.py
- **Knowledge:** **Resolved**: the isolated-interpreter `load(name, path)` program and the `from scripts import X` prologue have one home (`LOAD_PROGRAM`, `scripts_program`)
- **Verdict:** resolved (this card)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 resolution update: clusters 16, 17 and 19 (verifier/spec module loaders, test_main wiring)

Cluster 19: `test_complexipy.py`, `test_gate_negatives.py` and `test_policy_snapshot.py` each
carried the same `spec_from_file_location` recipe; it is now `tests.tooling.module_loading.load_module`
and each call site still names its own module explicitly. Cluster 16: `_LOAD` and `_LOAD_VERIFIER`
were the same isolated-interpreter loader program except that `_LOAD` registered the module in
`sys.modules` and `_LOAD_VERIFIER` did not. They are now one `LOAD_PROGRAM` that registers (harmless in
a fresh interpreter; no verifier assertion depends on absence), and the five pickle/import programs
share the `scripts_program` prologue. Cluster 17: three `TestMain`/`TestCliOverrideConsistency` tests
repeated the `cli.run_pipeline` recording stub; `_record_pipeline_runs` owns it. The fourth
`run_pipeline` stub in `test_main.py` (blocks until cancelled) is different knowledge and was left alone.
Incidental similarity left alone: the `_run`/`_run_program` and `_copy_scripts`/`_copy_verifier` helpers
differ in `-S`, timeout and source directory and are below the clone threshold.

Re-measured with the pinned jscpd 5.3.3 (`quality.duplication.scan`, minimum 50, over
`quality.source_scope.discover`): the parent tree reports 36 clone groups (430 duplicated lines,
0.70%); this branch reports 29 (365 lines). Matched by token count, lines and file pair, exactly
the seven cluster 16/17/19 groups are gone and none was added.
