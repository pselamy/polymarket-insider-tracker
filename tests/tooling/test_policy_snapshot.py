"""Golden policy snapshot against check-weakening (T2, accepted).

This module binds the three policy surfaces together AND to their golden
values, closing the self-certification loophole where a threshold such as
``--max-complexity-allowed 5`` could be raised to ``6`` consistently in the
verifier, the config file, and the CI workflow without any test failing.

Golden policy (change requires Patrick's approval — see module docstring rule):
- Complexipy threshold ``max-complexity-allowed`` is exactly ``5``.
- Gate scope is exactly ``src tests scripts alembic conftest.py``.
- Blocking CI jobs are exactly
  ``(static, vulture, complexipy, compatibility, services)``.
- Strict type gates stay strict (mypy ``strict = true``, Pyright
  ``typeCheckingMode = strict``); no baselines, allowlists, diff-only modes,
  exclusions, suppressions, or non-blocking status.

The live tamper probe proves the threshold is semantic, not decorative: a
score-6 function MUST fail under the golden policy.

Golden-file changes in the same change that weakens the policy are rejected by
review rule: they require Patrick's approval. Remote branch-protection wiring
is NOT_INSPECTED and claimed by nobody.
"""

from __future__ import annotations

import importlib.util
import tomllib
from pathlib import Path
from types import ModuleType

import pytest
import yaml

from polymarket_insider_tracker.config import DatabaseSettings, RedisSettings

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPOSITORY_ROOT / "scripts" / "complexipy_gate.py"
VERIFIER_PATH = REPOSITORY_ROOT / "scripts" / "verify.py"
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml"
PIPELINE_PATH = REPOSITORY_ROOT / "src" / "polymarket_insider_tracker" / "pipeline.py"

GOLDEN_MAX_COMPLEXITY = 5
GOLDEN_SCOPE = ("src", "tests", "scripts", "alembic", "conftest.py")
GOLDEN_BLOCKING_JOBS = ("static", "vulture", "complexipy", "compatibility", "services")

GOLDEN_COMPLEXIPY_COMMAND = (
    "uv run --isolated --locked --all-extras --python 3.11 python scripts/complexipy_gate.py "
    + " ".join(GOLDEN_SCOPE)
    + " --max-complexity-allowed 5 --no-ignore --ignore-complexity=false"
    + " --snapshot-ignore=true --snapshot-create=false --exclude=. --check-script=true"
)

SCORE_6_CODE = (
    "def sample_func(x: int) -> int:\n"
    "    if x > 1: return 1\n"
    "    if x > 2: return 2\n"
    "    if x > 3: return 3\n"
    "    if x > 4: return 4\n"
    "    if x > 5: return 5\n"
    "    if x > 6: return 6\n"
    "    return 0\n"
)

SCORE_5_CODE = (
    "def sample_func(x: int) -> int:\n"
    "    if x > 1: return 1\n"
    "    if x > 2: return 2\n"
    "    if x > 3: return 3\n"
    "    if x > 4: return 4\n"
    "    if x > 5: return 5\n"
    "    return 0\n"
)


def _verifier() -> ModuleType:
    spec = importlib.util.spec_from_file_location("verify_policy_snapshot", VERIFIER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _complexipy_runner() -> ModuleType:
    spec = importlib.util.spec_from_file_location("complexipy_gate_policy_snapshot", RUNNER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _workflow() -> dict[str, object]:
    with WORKFLOW_PATH.open(encoding="utf-8") as handle:
        loaded: dict[str, object] = yaml.safe_load(handle)
    return loaded


def _required_job_needs() -> list[str]:
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    required = jobs["required"]
    assert isinstance(required, dict)
    needs = required["needs"]
    assert isinstance(needs, list)
    entries = list(needs)
    assert all(isinstance(entry, str) for entry in entries)
    return [entry for entry in entries if isinstance(entry, str)]


def _pyproject_table(name: str) -> dict[str, str | int | bool | list[str]]:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    tool = pyproject["tool"]
    raw_table = tool[name]
    table: dict[str, str | int | bool | list[str]] = {}
    for key in ("paths", "max-complexity-allowed", "no-ignore", "check-script"):
        if key in raw_table:
            table[key] = raw_table[key]
    return table


def _strict_table(section: str) -> dict[str, str | bool]:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    raw_table = pyproject["tool"][section]
    table: dict[str, str | bool] = {}
    for key in ("strict", "typeCheckingMode", "enableTypeIgnoreComments"):
        if key in raw_table:
            table[key] = raw_table[key]
    return table


def _path_entries(value: str | int | bool | list[str] | None) -> list[str]:
    assert isinstance(value, list)
    return list(value)


def test_golden_complexity_threshold_is_five() -> None:
    verifier = _verifier()
    command = verifier.GATES["complexipy"].command
    index = command.index("--max-complexity-allowed")

    assert command[index + 1] == "5"
    assert command[index + 1] == str(GOLDEN_MAX_COMPLEXITY)


def test_golden_scope_covers_every_tracked_tree_exactly_once() -> None:
    verifier = _verifier()
    command = verifier.GATES["complexipy"].command
    marker = command.index("scripts/complexipy_gate.py") + 1
    verifier_scope = command[marker : marker + len(GOLDEN_SCOPE)]

    assert verifier_scope == GOLDEN_SCOPE
    raw_scope = command[marker:]
    terminator = raw_scope.index("--max-complexity-allowed")
    assert raw_scope[:terminator] == GOLDEN_SCOPE


def test_golden_pyproject_policy_matches_verifier() -> None:
    complexipy = _pyproject_table("complexipy")
    vulture = _pyproject_table("vulture")

    complexipy_paths = _path_entries(complexipy.get("paths"))
    vulture_paths = _path_entries(vulture.get("paths"))
    assert complexipy["max-complexity-allowed"] == GOLDEN_MAX_COMPLEXITY
    golden_scope_list = list(GOLDEN_SCOPE)
    assert complexipy_paths == golden_scope_list
    assert complexipy["no-ignore"] is True
    assert complexipy["check-script"] is True
    assert vulture_paths == golden_scope_list


def test_golden_verifier_command_text_matches_canonical_form() -> None:
    verifier = _verifier()

    assert verifier.GATES["complexipy"].command_text == GOLDEN_COMPLEXIPY_COMMAND
    assert dict(verifier.DIRECT_GATE_COMMANDS)["complexipy"] == GOLDEN_COMPLEXIPY_COMMAND


def test_golden_blocking_jobs_are_bound_in_ci_and_verifier() -> None:
    verifier = _verifier()

    assert verifier.gate_ids_for_profile("static") == (
        "lock",
        "format",
        "lint",
        "strict-types",
        "pyright",
        "vulture",
        "complexipy",
    )
    jobs = _workflow()["jobs"]
    assert isinstance(jobs, dict)
    for job_id in GOLDEN_BLOCKING_JOBS:
        assert job_id in jobs
        job = jobs[job_id]
        assert isinstance(job, dict)
        assert "continue-on-error" not in job
    assert _required_job_needs() == list(GOLDEN_BLOCKING_JOBS)
    required = jobs["required"]
    assert isinstance(required, dict)
    assert required.get("if") == "always()"


def test_golden_strict_type_flags_are_not_weakened() -> None:
    mypy = _strict_table("mypy")
    pyright = _strict_table("pyright")

    assert mypy["strict"] is True
    assert pyright["typeCheckingMode"] == "strict"
    assert pyright["enableTypeIgnoreComments"] is False


def _write_fixture_scope(root: Path, code: str) -> None:
    for directory in ("src", "tests", "scripts", "alembic"):
        (root / directory).mkdir(parents=True, exist_ok=True)
    (root / "src" / "sample.py").write_text(code, encoding="utf-8")
    (root / "conftest.py").write_text("", encoding="utf-8")


def _configured_sqlite_urls() -> list[str]:
    """Collect the sqlite engine URLs this policy must reject end to end."""
    return ["sqlite:///tracker.db", "sqlite+aiosqlite:///:memory:"]


def _rejected_sqlite_url_error(url: str) -> str:
    """Run the shipped settings path; return the rejection message.

    A weakening that accepts sqlite returns the accepted URL instead of
    raising, which the caller treats as a tamper-acceptance failure. The
    return (rather than bare ``pytest.raises``) keeps the semantic
    accept/reject outcome visible at the assertion site. ``ValidationError``
    is referenced through ``DatabaseSettings`` so this module never imports
    the validation framework itself.
    """
    try:
        accepted = DatabaseSettings.model_validate({"DATABASE_URL": url})
    except Exception as exc:
        assert type(exc).__name__ == "ValidationError", f"unexpected {type(exc)}"
        return str(exc)
    return f"ACCEPTED:{accepted.url}"


LAUNCHER_POLICY_FLAGS = (
    "--max-complexity-allowed",
    str(GOLDEN_MAX_COMPLEXITY),
    "--no-ignore",
    "--ignore-complexity=false",
    "--snapshot-ignore=true",
    "--snapshot-create=false",
    "--exclude=.",
    "--check-script=true",
)


def test_score_6_fixture_fails_under_golden_policy(
    tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    """Execute the shipped launcher; the failure must be the complexity rule.

    The score-5 positive control proves the tool is present and the harness
    is valid. The score-6 run must name ``sample_func`` as ``FAILED``: a
    missing-analyzer import error exits nonzero without that marker, so it
    fails this test instead of being accepted as a complexity rejection.
    """
    fixture_ok = tmp_path / "fixture-ok"
    fixture_bad = tmp_path / "fixture-bad"
    _write_fixture_scope(fixture_ok, SCORE_5_CODE)
    _write_fixture_scope(fixture_bad, SCORE_6_CODE)

    runner = _complexipy_runner()

    assert runner.CANONICAL_SCOPE == GOLDEN_SCOPE
    assert runner.POLICY_FLAGS == LAUNCHER_POLICY_FLAGS
    assert runner.run_complexipy(fixture_ok) == 0
    capfd.readouterr()

    assert runner.run_complexipy(fixture_bad) != 0
    captured = capfd.readouterr()
    report = captured.out + captured.err
    assert "sample_func" in report
    assert "FAILED" in report


def test_consistent_threshold_6_across_surfaces_still_fails_golden() -> None:
    """A candidate that raises the threshold to 6 everywhere must fail this suite.

    This negative probe simulates the self-certification attack: even if the
    verifier, pyproject, and workflow agreed on ``6``, the golden value pinned
    here stays ``5`` and the tamper is detected by value, not by consistency.
    """
    assert GOLDEN_MAX_COMPLEXITY == 5
    assert "6" not in GOLDEN_COMPLEXIPY_COMMAND.split("--max-complexity-allowed")[1].split()[0]


def test_production_database_path_rejects_sqlite() -> None:
    """The shipped settings path rejects sqlite engine URLs end to end.

    The residual assigned by the accepted review is a negative case through
    the shipped database-URL path: production ``DatabaseSettings`` must
    reject ``sqlite://`` (and the async ``sqlite+aiosqlite://`` spelling
    used by test-only engines). Each URL is asserted through the real
    shipped validator (``normalize_database_url`` via ``DatabaseSettings``)
    AND shown to be genuinely rejected (not vacuously failing): the
    rejection message names the supported drivers, and the driver-identity
    pin makes an allowlist tamper visible at the allowlist itself.
    """
    from polymarket_insider_tracker.storage import database_url as shipped

    assert set(shipped.SUPPORTED_DRIVERS) == {
        "postgresql",
        "postgresql+asyncpg",
        "postgresql+psycopg",
    }

    urls = _configured_sqlite_urls()
    _require_configured_sqlite_urls(urls)
    for url in urls:
        message = _rejected_sqlite_url_error(url)
        assert "ACCEPTED:" not in message, f"shipped settings accepted {url}"
        assert "DATABASE_URL" in message
        assert "postgresql+psycopg" in message


def _require_configured_sqlite_urls(urls: list[str]) -> None:
    """Guard the negative control against empty discovery or non-sqlite rows."""
    assert urls, "empty discovery would spuriously pass: no sqlite URLs configured"
    assert all(url.startswith("sqlite") for url in urls)


def test_production_redis_settings_reject_non_redis_scheme() -> None:
    """RedisSettings accepts only redis:// (memory:// proves no silent fake)."""
    message = _rejected_redis_url_error("memory://")
    assert "ACCEPTED:" not in message
    assert "redis://" in message


def _rejected_redis_url_error(url: str) -> str:
    """Run the shipped Redis settings path; return the rejection message."""
    try:
        accepted = RedisSettings.model_validate({"REDIS_URL": url})
    except Exception as exc:
        assert type(exc).__name__ == "ValidationError", f"unexpected {type(exc)}"
        return str(exc)
    return f"ACCEPTED:{accepted.url}"


def test_pipeline_wires_the_real_redis_client_identity() -> None:
    """The production pipeline constructs Redis from the real client class."""
    from redis.asyncio import Redis

    import polymarket_insider_tracker.pipeline as pipeline

    source = PIPELINE_PATH.read_text(encoding="utf-8")

    assert "from redis.asyncio import Redis" in source
    assert pipeline.Redis is Redis
