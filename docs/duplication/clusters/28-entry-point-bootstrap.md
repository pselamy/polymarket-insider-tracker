---
cluster: 28
status: resolved
resolved-by: PR #160
---

# Cluster 28

- **Files:** _entry_bootstrap.py (new)
- **Knowledge:** **Resolved**: the script entry-point sibling-import bootstrap now has one home (`scripts/_entry_bootstrap.py`); both entry points load it
- **Verdict:** resolved (PR #160)
- **Counts when first grouped (historical):** 0 clones, 0 duplicated lines. Current counts are generated, see [the README](../README.md).

## 2026-10-19 resolution update: cluster 28 (script entry-point bootstrap)

Cluster 28's single clone pair had `scripts/runtime_services.py` and
`scripts/verify.py` each rebuilding the same entry-point bootstrap: create a
unique synthetic package (`module_from_spec(ModuleSpec(name, loader=None,
is_package=True))`), append the script's own directory to its `__path__`,
register it in `sys.modules`, save `__package__`/`__spec__`, repoint them at the
new package (spec name `f"{helper}.entry"`) so the private sibling modules import
under it, and restore both in a `finally`. The copies differed only in the helper
prefix (`_polymarket_services_` vs `_polymarket_verifier_`) and the random source
(`uuid.uuid4()` plus the module's reusable `uuid` import vs a local `uuid4`
alias), so no behaviour needed adjudicating.

That knowledge now has one home, `scripts/_entry_bootstrap.py`
(`sibling_imports`, a `@contextmanager`). Both entry points load it by file path
with `runpy.run_path(Path(__file__).with_name("_entry_bootstrap.py"))` — the
module cannot be imported, because the package machinery it provides does not
exist until it has run, and `runpy` carries no `sys.path` mutation, so the
isolated (`-I -S`) standalone-verifier contract still holds. Each call site keeps
its own prefix in `with _sibling_imports(globals(), "<prefix>"):` and loses only
the now-unused `importlib`/`uuid4` imports. All three invocation modes stay
correct: direct `python scripts/x.py`, `python -m scripts.x`, and
`spec_from_file_location` loading under an arbitrary name (including a copied
standalone `scripts/` tree). Every existing test passes unchanged; the added
characterization tests pin all of it for both entry points.

Re-measured with the identical pinned method (jscpd 5.3.3, `SCAN_POLICY` at the
50-token tier, empty config, no ignore lists or exclusions), enumerating the
authored scope as `quality.source_scope.discover` + `validate_path` (`.py` only,
size hook not applied) and staging with `quality.duplication_inputs.stage_inputs`:

- Before (branch base `6be456a`, the cluster-22 resolution; 282 authored Python
  files): **85 clone groups, 1080 duplicated lines, 1.80%**.
- After (this change, 284 authored files; the two additions are
  `scripts/_entry_bootstrap.py` and
  `tests/tooling/test_entry_bootstrap_characterization.py`): **84 clone groups,
  1071 duplicated lines, 1.78%**.

Rebased onto main after #157 (cluster 25, 283 files: 88 groups, 1116 lines,
1.85%), the identical method measures **87 clone groups, 1107 duplicated lines,
1.83%** over 285 authored files (252 sources tokenized, 0 clone groups touching
`scripts/_entry_bootstrap.py`, `scripts/runtime_services.py` or
`scripts/verify.py`): −1 group and −9 lines, exactly the cluster-28 clone.

Set-difference of the raw clone lists: exactly the cluster-28 clone
(`scripts/runtime_services.py:46-54 <-> scripts/verify.py:27-35`, 69 tokens / 9
lines) disappeared and **no new clone group appeared anywhere**. The
characterization test was written with a single consolidated loader program so
that it neither self-duplicated nor copied the `_LOAD`/`_LOAD_VERIFIER` loader
text that cluster 16 still owns. Resolved by draft PR
[#160](https://github.com/pselamy/polymarket-insider-tracker/pull/160) from branch
`ralph/dedup-bootstrap`.
