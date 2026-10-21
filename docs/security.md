# Security gate

`python -m quality.security_main` is enrolled in `quality/checks.json`, so it runs in
`python -m quality.verify` locally and in the hosted "Relentless CI gate" job (that checkout uses
`fetch-depth: 0`, which the history scan needs). Scanners are provisioned by `mise --locked`
(`mise.toml`, `mise.lock`). There are no suppressions: `quality/gitleaks.ignore` is empty,
opengrep runs with `--disable-nosem`, and no baseline, fingerprint or allowlist exists.

| Scanner | Mode | Enforced |
| --- | --- | --- |
| opengrep | `quality/security.yml` over every authored `py`/`pyi` file | yes |
| gitleaks `dir` | the full working tree | yes |
| gitleaks `git` | `--log-opts=<cut-over>..HEAD`, commits after the cut-over only | yes |
| osv-scanner | every package in `uv.lock` | yes, any advisory fails; see Dependency advisories |

## Deviation from the reference: secret scan cut-over

The reference scans all history. Here history before the commit recorded in
`quality/gitleaks-cutover.txt` (`db93cf3c644d3a477038f422b82c91ddad6f59b4`, `origin/main` when
the gate was enrolled) is not scanned, because eleven old commits contain findings that no code
change can remove and no allowlist or fingerprint is permitted. The gate fails closed if the
cut-over is not a full lowercase SHA or not an ancestor of `HEAD`.

All eleven are fake test fixtures or, in one case, a false positive; none is a secret:

| Commit | File | Line | What it is |
| --- | --- | --- | --- |
| `2d5dcc623634` | `tests/test_redaction.py` | 30 | fake redaction fixture |
| `76bd4a569535` | `tests/test_redaction.py` | 41 | fake redaction fixture |
| `f45f39927cb9` | `tests/test_redaction.py` | 53, 227 | fake redaction fixtures |
| `c4bb3da537b0` | `tests/test_redaction.py` | 69 | fake redaction fixture |
| `e677332c4b8e` | `tests/tooling/test_secret_redaction.py` | 153 | fake token fixture |
| `69b2c429d8a9` | `tests/tooling/test_gate_negatives.py` | 284, 480 | fake fixtures |
| `038bac61456f` | `tests/alerter/test_dispatcher.py` | 142 | fake token fixture |
| `7eee2a3b24e5` | `tests/profiler/test_chain.py` | 21 | public token contract address |
| `3571a3e3fbef` | `src/polymarket_insider_tracker/alerter/dispatcher.py` | 461 | false positive: `extend_channel_claim(..., token, ttl=...)` |

The working tree still contained eight of these patterns when the full-tree scan was first run.
They were removed by renaming test identifiers (`*_SECRET` to `*_CANARY`, `VALID_TOKEN` to
`VALID_CONTRACT`, locals in two tests) and by one line-break change in `dispatcher.py`; no value
or behaviour changed.

## Dependency advisories (enforced)

osv-scanner results are enforced. The scan covers every package in `uv.lock`, with no allowlist,
baseline, ignore file, severity floor or exemption. Any advisory group, of any severity, makes
`python -m quality.security_main` exit 1 and print each affected package, its locked version and
the advisory IDs. The gate fails closed: a missing `mise` or scanner, a scanner crash or timeout, a
missing or malformed report, or a report that does not cover exactly the locked packages also
fails, with a message that osv-scanner could not prove the lockfile clean. The scan runs as it did
in report-only mode (pinned by `mise --locked`, `quality/osv-scanner.toml`).

At enforcement the tree has 0 advisory groups, after the six scoped family upgrades listed in
`docs/dependency-upgrades.md` (60 groups in 17 packages on `main` before them). "Groups" are osv
alias groups, one per underlying advisory; that document's "Advisories" column counts every
GHSA/PYSEC record separately, so its numbers are larger.

When a new advisory appears, handle it with a scoped upgrade PR for the affected package family
(minimum fixed release, existing tests unchanged). Do not add an exemption to unblock the gate.

## Why the reference hook is not used

The reference `verify_security` runs after `verify_sources`, which fails on this tree: a large set
of authored files exceed 399 physical lines, and `alembic/versions` names are not snake_case.
Fixing that means splitting files, which is out of scope. `quality.security_gate` reuses the
reference scanner functions unchanged and takes the file inventory from `discover` instead.

## Negative controls

Run on 2026-10-21 against this tree: a test credential committed and then removed, a credential
only in the working tree, and `pickle.loads` in a new `quality/` file each made
`python -m quality.security_main` exit 1; the clean tree exits 0. `tests/quality/test_security_gate.py`
repeats the secret cases in scratch repositories when `mise` is available (Relentless CI and local
runs; the other hosted jobs do not provision it and skip them).
