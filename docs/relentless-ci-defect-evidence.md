# Relentless CI enrolled-control defect evidence

For every control enrolled in `quality/checks.json` and the `quality.verify` hook
sequence, this records a **clean-passes** run on the current tree and a
**defect-fails** run against a deliberately introduced defect. The defect probe is
removed after each run, so no probe artifact is committed.

Commands were run from the repository root. `uv run --locked --group dev` supplies
the enrolled tool set; the clean column is exactly the command from
`quality/checks.json`.

| Control | Clean command | Clean exit | Deliberate defect | Defect exit | First defect diagnostic |
| --- | --- | --- | --- | --- | --- |
| `REF-CMD-01` `uv lock --check` | `uv lock --check` | 0 | add `requests>=2.0.0` to `[project].dependencies` without re-locking | 1 | `The lockfile at uv.lock needs to be updated, but --check was provided` |
| `REF-CMD-17` `black` | `uv run --locked --group dev black --check src tests quality scripts alembic conftest.py` | 0 | `quality/_defect_probe.py` containing `x=1` | 1 | `would reformat quality/_defect_probe.py` |
| `REF-CMD-18` `ruff` | `uv run --locked --group dev ruff check src tests quality scripts alembic conftest.py` | 0 | `quality/_defect_probe.py` with `import os` and no use | 1 | `F401 'os' imported but unused` |
| `REF-CMD-19` `mypy` | `uv run --isolated --locked --all-extras --python 3.11 mypy src scripts quality alembic conftest.py` | 0 | `quality/_defect_probe.py` containing `VALUE: int = "not-an-int"` | 1 | `quality/_defect_probe.py:3: error: Incompatible types in assignment (expression has type "str", variable has type "int") [assignment]` |
| `REF-CMD-20` `pyright` | `uv run --locked --group dev pyright` | 0 | `src/polymarket_insider_tracker/_defect_probe.py` with `VALUE: int = "not-an-int"` | 1 | `Type "Literal['not-an-int']" is not assignable to declared type "int"` (`reportAssignmentType`) |
| `REF-CMD-22` `complexipy` | `uv run --locked --group dev complexipy src tests quality scripts alembic conftest.py --max-complexity-allowed 5` | 0 | `quality/_defect_probe.py` with a cognitive-complexity-6 function | 1 | `probe FAILED` |
| `REF-HOOK-02` `verify_tracked` | `uv run --locked --group dev python -m quality.verify` | 0 | stage `quality/_CaseProbe.py` and `quality/_caseprobe.py` (case-colliding tracked paths) | 1 | `ValueError: repository paths collide after Unicode/case normalization` |

## Notes

- The clean run of the whole enrolled sequence is
  `uv run --locked --group dev python -m quality.verify`, which exits `0`.
- The `uv lock --check` probe runs in a disposable copy of `pyproject.toml` and
  `uv.lock` so the repository lock is never mutated.
- The `REF-CMD-19` mypy control resolves its dependencies at the minimum supported
  minor through one isolated locked resolution (`uv run --isolated --locked
  --all-extras --python 3.11 mypy`), the same command shape the repository's own
  `strict-types` gate (`scripts/_verify_policy.py`) enforces, so the result is
  deterministic regardless of the host interpreter (mypy's
  `python_version = "3.11"` must not parse 3.12+-only stub syntax such as the
  `type` statement in numpy's shipped stubs).
- The `pyright` probe must run through `uv run` so Pyright resolves the project
  virtual environment; running the Pyright binary outside the managed environment
  cannot resolve runtime imports and is not the enrolled command.
- The `verify_tracked` probe stages the colliding paths in the Git index, then
  unstages and deletes them; the index is left clean.
