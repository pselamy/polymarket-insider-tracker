# Root Agent Guidance Contract

This documentation-only follow-up implements Patrick's request for a separate root `AGENTS.md`
codifying the practices established during slice 002. It changes no public runtime contract.

## Requirements and Sources

| Guidance | Authoritative basis | Acceptance evidence |
|---|---|---|
| Read-only monitoring, safe alerts, durable records, compatibility | [Constitution](../../../.specify/memory/constitution.md), principles I–V | Root guidance links and accurately summarizes the boundaries |
| Explicit slice activation, artifact/checklist ownership, truthful evidence, approval before merge | Constitution, Development Workflow and Quality Gates | Root guidance gives a valid explicit activation command and does not authorize agent self-signoff |
| Locked setup, Black/Ruff, strict mypy/Pyright, Vulture, Complexipy <=5, real services and supported minors | [Runtime contract](runtime-verification.md), `pyproject.toml`, `scripts/verify.py`, CI | Commands agree with tracked executable configuration; no new checker or exception |
| Low cognitive load, underlying remediation, separate coherent PRs, no `.claude/skills` | Patrick's explicit quality and repository-hygiene instructions | Root guidance rejects metric gaming and copied tool-specific skill trees without deleting the managed Spec Kit integration |
| Working fakes, observable outcomes, independent verification of delegated work | Patrick's test-double and Agy/Claude/Codex review instructions; constitution principle III | Policy is stated without claiming all legacy mocks are already removed or all fakes are already contract-tested |
| CodeGraph before broad code exploration | Patrick's supplied AGENTS instructions | Local generated index handling and unavailable-tool fallback are explicit |

## Non-goals

- No constitution amendment, model mandate for every future task, new approval gate, or new feature.
- No test-double implementation, source-code refactor, dependency change, or CI modification.
- No enforcement script that parses prose as a second configuration authority.
- No duplication of global skills or replacement of the installed Spec Kit integration.

## Verification

Check relative links, resolve the explicit feature example using the real prerequisite script,
compare commands with the real verifier, and run existing static/compatibility profiles. Record
evidence in `evidence/agent-guidance.md`. Preserve human ownership of domain checklist decisions.
